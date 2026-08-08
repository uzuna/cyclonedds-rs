//! 非同期向けの実装を提供するモジュール

use std::{
    future::{self, Future},
    sync::{Arc, Mutex, Weak},
    task::Poll,
    time::Instant,
};

use cyclonedds_sys::DDSError;
use futures_util::{future::Either, task::AtomicWaker};

use crate::{
    DdsListener, DdsListenerBuilder,
    error::ReaderError,
    match_watch::{MatchWatch, Watched, watchdog},
};

/// cycloneddsのコールバックを使って起こすWakerの型
pub(crate) type AsyncWaker = Arc<ReaderShared>;

/// 非同期リーダーとリスナーコールバックが共有する状態
pub(crate) struct ReaderShared {
    waker: AtomicWaker,
    /// 一度だけ通知するエラー(deadline違反)
    pending_error: Mutex<Option<ReaderError>>,
    /// ライター不在の監視。[`crate::ReaderBuilder::stop_when_no_writer`]指定時のみ`Some`
    watch: Option<MatchWatch>,
}

impl ReaderShared {
    fn new(watch: Option<MatchWatch>) -> Arc<Self> {
        Arc::new(Self {
            waker: AtomicWaker::new(),
            pending_error: Mutex::new(None),
            watch,
        })
    }

    /// 保持しているエラーを取り出す(一度返したら消費される)
    fn take_pending_error(&self) -> Option<ReaderError> {
        self.pending_error.lock().unwrap().take()
    }

    /// 手元にサンプルが無いときに返す不在エラー。状態から毎回導出するので、
    /// 一度返した後も対向が現れるまで呼ぶたび同じエラーを返す(sticky)
    fn absent_writer_error(&self) -> Option<ReaderError> {
        let watch = self.watch.as_ref()?;
        watch.is_absent().then(|| ReaderError::NoMatchedWriter {
            timeout: watch.timeout(),
        })
    }

    /// マッチしているwriterが不在確定しているか(オプション未指定なら常に`false`)
    fn is_writer_absent(&self) -> bool {
        self.watch.as_ref().is_some_and(|w| w.is_absent())
    }
}

impl Watched for ReaderShared {
    fn absence_deadline(&self) -> Option<Instant> {
        self.watch.as_ref().and_then(|w| w.absence_deadline())
    }

    fn notify_absence(&self) {
        if let Some(watch) = &self.watch {
            watch.mark_notified();
        }
        self.waker.wake();
    }
}

/// リーダーとそれに紐づくWakerの有無を保持する型
pub(crate) enum ReaderType {
    /// リーダーに紐付いたWakerがある
    Async(AsyncWaker),
    Sync,
}

impl ReaderType {
    /// マッチしているwriterが不在確定しているか(オプション未指定・同期リーダーは常に`false`)
    pub(crate) fn is_writer_absent(&self) -> bool {
        match self {
            ReaderType::Async(shared) => shared.is_writer_absent(),
            ReaderType::Sync => false,
        }
    }
}

pub(crate) fn read<'a, F>(
    reader_type: &'a ReaderType,
    mut readn_from_entity_now: F,
) -> impl Future<Output = Result<usize, ReaderError>> + use<'a, F>
where
    F: FnMut() -> Result<usize, DDSError> + 'a,
{
    if let ReaderType::Async(waker) = reader_type {
        Either::Left(future::poll_fn(move |ctx| {
            // Why: 状態を見る前に必ずWakerを登録する(register-then-check)。
            // 逆順(読んでから登録)にすると、「データなし」と判定してから登録するまでの間に
            // cyclonedds側のリスナーが`wake()`を呼んだ場合、`AtomicWaker`にはまだ何も
            // 登録されていないため通知が捨てられ、以降データが来なければ永久にPendingになる。
            // `AtomicWaker::wake()`は登録済みWakerを取り出す(スロットが空になる)実装なので、
            // この窓は初回pollだけでなくwakeされる度に開く
            waker.waker.register(ctx.waker());

            // wakerがエラーを持っていたらそれを返す
            if let Some(err) = waker.take_pending_error() {
                return Poll::Ready(Err(err));
            }

            match readn_from_entity_now() {
                Ok(len) => Poll::Ready(Ok(len)),
                // データがない場合は次のデータが来るまで待つ。
                // 本物のエラー(`OutOfResources`等)は待っても解消しないので即返す。
                //
                // `stop_when_no_writer`未指定時はここが唯一の分岐で、writerが全ていなくなっても
                // このfutureは完了せずPendingのままになる。writerの消滅を検知したい場合は
                // `ReaderBuilder::stop_when_no_writer`を指定するか、呼び出し側で
                // `tokio::time::timeout`等の上限を掛けること
                Err(DDSError::NoData) => match waker.absent_writer_error() {
                    // 手元にサンプルが無いときだけ不在を確定させる。
                    // Why: 届いているデータを不在エラーで捨てない。writerが消えても
                    //      受信済みのサンプルは読み切れる
                    Some(err) => Poll::Ready(Err(err)),
                    None => Poll::Pending,
                },
                Err(e) => Poll::Ready(Err(ReaderError::DdsError(e))),
            }
        }))
    } else {
        Either::Right(future::ready(Err(ReaderError::ReaderNotAsync)))
    }
}

/// DataReader用のListenerとWakerの組み合わせを作成する
///
/// データ到達時はOK、Deadline超え(QoS違反)はエラーをセットする。`watch`が`Some`の場合は
/// 対向writerの不在監視も有効にし、`on_subscription_matched`を連鎖させてウォッチドッグに
/// 登録する。`listener_builder`には利用者が
/// [`crate::ReaderBuilder::with_listener_builder`]経由で設定済みのコールバックが
/// 入りうるため、内部のコールバックは`chain_*`で連鎖させ、利用者側を潰さない。
///
/// Why not on_liveliness_changed: writerの出現/消滅は正常なイベントであって読み出しの
/// 失敗ではない。ここでリスナーを登録するとwriterが1つ現れただけで読み出しがエラーを
/// 返してしまい、呼び出し側が読み飛ばしを強いられる。またリスナーを登録すると
/// DDSの仕様上コールバック時点でstatusの変化カウンタがリセットされるため、
/// [`crate::DdsReader::liveliness_changed_status`]で正確な変化量を読めなくなる。
///
/// `watch`未指定時はこの帰結として、writerが全ていなくなっても非同期読み出しは完了せず
/// Pendingのままになる。writerの消滅を検知したい場合は
/// [`crate::ReaderBuilder::stop_when_no_writer`]を指定すること。
pub(crate) fn data_reader_listener(
    listener_builder: DdsListenerBuilder,
    watch: Option<MatchWatch>,
) -> (DdsListener, ReaderType) {
    let shared = ReaderShared::new(watch);
    let has_watch = shared.watch.is_some();

    let listener_builder = listener_builder
        .chain_data_available({
            let shared = shared.clone();
            move |_entity| {
                // 新規データが有効になったら起こす
                shared.waker.wake();
            }
        })
        .chain_requested_deadline_missed({
            let shared = shared.clone();
            move |_entity, _status| {
                // 期限内にデータが来なかったのは契約違反なので、待ち続けずに知らせる
                *shared.pending_error.lock().unwrap() = Some(ReaderError::RequestedDeadLineMissed);
                shared.waker.wake();
            }
        });

    let listener_builder = if has_watch {
        listener_builder.chain_subscription_matched({
            let shared = shared.clone();
            move |_entity, status| {
                if let Some(watch) = &shared.watch {
                    watch.update(status.current_count);
                }
                // 待機中のウォッチドッグに新しい期限を拾わせる
                watchdog().reschedule();
            }
        })
    } else {
        listener_builder
    };

    let listener = listener_builder.build();

    if has_watch {
        // 猶予の起点は生成時刻(`MatchWatch::new`)なので、最初のマッチングイベントを
        // 待たずにここで登録する
        watchdog().register(Arc::downgrade(&shared) as Weak<dyn Watched>);
    }

    (listener, ReaderType::Async(shared))
}

/// BuiltinDataReader向けのリスナー
///
/// こちらはメタデータの読み出しなのでデータの到達イベントだけ。builtinトピックに
/// 対向不在の概念は持ち込まないため`watch`は常に無効
pub(crate) fn participant_reader_listener() -> (DdsListener, ReaderType) {
    let waker = ReaderShared::new(None);

    let listener = DdsListenerBuilder::new()
        .on_data_available({
            let waker = waker.clone();
            move |_entity| {
                // 有効なデータが届いたときに反応する
                waker.waker.wake();
            }
        })
        .build();

    (listener, ReaderType::Async(waker))
}
