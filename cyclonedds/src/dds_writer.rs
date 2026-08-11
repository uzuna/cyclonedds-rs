/*
    Copyright 2020 Sojan James

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

use cyclonedds_sys::*;
use std::convert::From;
use std::ffi::c_void;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing::{error, info};

pub use cyclonedds_sys::DdsEntity;

use crate::match_watch::MatchWatch;
use crate::serdes::{FixedTopicType, Sample, TopicType};
use crate::{
    DdsWritable, Entity, Keepalive, dds_listener::DdsListener, dds_listener::DdsListenerBuilder,
    dds_qos::DdsQos, dds_topic::DdsTopic,
};

/// 不在/復帰ログに載せるトピック名を取得する。取得できなくてもエンティティ生成自体は
/// 続けたいので、失敗時は空文字にフォールバックする
fn topic_name_for_log(topic_entity: &DdsEntity) -> String {
    let mut buf = [0 as std::os::raw::c_char; 256];
    let ret = unsafe { dds_get_name(topic_entity.entity(), buf.as_mut_ptr(), buf.len()) };
    if ret >= 0 {
        // SAFETY: dds_get_nameは成功時にbufへNUL終端した文字列を書き込む
        unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) }
            .to_string_lossy()
            .into_owned()
    } else {
        String::new()
    }
}

pub struct WriterBuilder<T: TopicType> {
    maybe_qos: Option<DdsQos>,
    maybe_listener: Option<DdsListener>,
    maybe_listener_builder: Option<DdsListenerBuilder>,
    watch_timeout: Option<Duration>,
    phantom: PhantomData<T>,
}

impl<T> Default for WriterBuilder<T>
where
    T: TopicType,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> WriterBuilder<T>
where
    T: TopicType,
{
    pub fn new() -> Self {
        Self {
            maybe_qos: None,
            maybe_listener: None,
            maybe_listener_builder: None,
            watch_timeout: None,
            phantom: PhantomData,
        }
    }

    pub fn with_qos(mut self, qos: DdsQos) -> Self {
        self.maybe_qos = Some(qos);
        self
    }

    /// マッチするreaderが`timeout`の間1つも居ない状態を監視し、
    /// [`DdsWriter::is_reader_absent`]で参照できるようにする
    ///
    /// [`Self::with_listener`]で渡したhook済みリスナーより優先される
    /// (hook済みのリスナーは後からコールバックを連鎖できないため)。
    /// 併用したい場合は[`Self::with_listener_builder`]を使うこと。
    ///
    /// 書き込み(`write`/`forward`/`loan`/`return_loan`)の挙動自体は変わらない。
    /// 不在時に送信を止めるかどうかは呼び出し側が判定材料を見て決める。
    ///
    /// 判定に使う対向数はQoS互換でマッチした数のため、QoSが噛み合わない相手も不在として
    /// 扱われる(相手が起動していても検知されない)。理由を知りたい場合は
    /// [`Self::with_listener_builder`]で`on_offered_incompatible_qos`を観測すること。
    pub fn watch_reader_absence(mut self, timeout: Duration) -> Self {
        self.watch_timeout = Some(timeout);
        self
    }

    /// hook済みの`DdsListener`は後からコールバックを連鎖できないため、
    /// [`Self::watch_reader_absence`]指定時はこちらが優先され、このリスナーは無視される。
    /// 併用したい場合は[`Self::with_listener_builder`]を使うこと。
    pub fn with_listener(mut self, listener: DdsListener) -> Self {
        self.maybe_listener = Some(listener);
        self
    }

    /// hook前のリスナービルダーを渡す。クレートが内部コールバックを必要とする機能を
    /// 有効にした場合、ここで設定済みのものを潰さずに連鎖して追加される。
    ///
    /// [`Self::watch_reader_absence`]と併用でき、指定した場合はそちらが優先されて
    /// このビルダーへ内部コールバックを連鎖する([`Self::with_listener`]で渡した
    /// hook済みリスナーは無視される)。一方、`watch_reader_absence`を使わない場合に
    /// `with_listener`も指定していると、hook済みの方が優先されこちらの内容は無効になる
    /// (hook済みのリスナーは後からコールバックを連鎖できないため)。
    pub fn with_listener_builder(mut self, builder: DdsListenerBuilder) -> Self {
        self.maybe_listener_builder = Some(builder);
        self
    }

    pub fn create(
        self,
        entity: &dyn DdsWritable,
        topic: DdsTopic<T>,
    ) -> Result<DdsWriter<T>, DDSError> {
        if self.watch_timeout.is_some() {
            DdsWriter::create_with_watch(
                entity,
                topic,
                self.maybe_qos,
                self.maybe_listener_builder.unwrap_or_default(),
                self.watch_timeout,
            )
        } else if let Some(listener) = self.maybe_listener {
            DdsWriter::create(entity, topic, self.maybe_qos, Some(listener))
        } else if let Some(listener_builder) = self.maybe_listener_builder {
            DdsWriter::create(
                entity,
                topic,
                self.maybe_qos,
                Some(listener_builder.build()),
            )
        } else {
            // QoS・リスナー・不在監視のいずれも未指定ならリスナーを一切作らずNULLを渡す
            // (readerと同じ形。毎回`dds_create_listener`するのは既存挙動を変える)
            DdsWriter::create(entity, topic, self.maybe_qos, None)
        }
    }
}

pub enum LoanedInner<T: Sized + TopicType> {
    Uninitialized(NonNull<T>, DdsEntity),
    Initialized(NonNull<T>, DdsEntity),
    Empty,
}

pub struct Loaned<T: Sized + TopicType> {
    inner: LoanedInner<T>,
}

impl<T> Loaned<T>
where
    T: Sized + TopicType,
{
    pub fn as_mut_ptr(&mut self) -> Option<*mut T> {
        match self.inner {
            LoanedInner::Uninitialized(p, _) => Some(p.as_ptr()),
            LoanedInner::Initialized(p, _) => Some(p.as_ptr()),
            LoanedInner::Empty => None,
        }
    }

    pub fn assume_init(mut self) -> Self {
        match &mut self.inner {
            LoanedInner::Uninitialized(p, e) => Self {
                inner: LoanedInner::Initialized(*p, e.clone()),
            },
            LoanedInner::Initialized(p, e) => Self {
                inner: LoanedInner::Initialized(*p, e.clone()),
            },
            LoanedInner::Empty => Self {
                inner: LoanedInner::Empty,
            },
        }
    }
}

impl<T> Drop for Loaned<T>
where
    T: Sized + TopicType,
{
    fn drop(&mut self) {
        let (mut p_sample, entity) = match &mut self.inner {
            LoanedInner::Uninitialized(p, entity) => (p.as_ptr(), Some(entity)),
            LoanedInner::Initialized(p, entity) => (p.as_ptr(), Some(entity)),
            LoanedInner::Empty => (std::ptr::null_mut(), None),
        };

        if let Some(entity) = entity {
            let voidpp: *mut *mut T = &mut p_sample;
            let voidpp = voidpp as *mut *mut c_void;
            unsafe { dds_return_loan(entity.entity(), voidpp, 1) };
        }
    }
}

/// マッチしているreaderの数(discoveryの結果)
///
/// [`WriterBuilder::watch_reader_absence`]を有効にすると、内部で`on_publication_matched`
/// リスナーを登録するため、DDSの仕様上コールバック時点で`*_change`がリセットされ、
/// 以降このAPIを読んでも変化量は常に0付近になる。`total_count`/`current_count`
/// (累計/現在数)自体は影響を受けない。
///
/// **このAPIを「readerが戻ったか」の判定に使わないこと。** FFIを経由するポーリング専用の
/// 窓口であり、復帰の検出には[`DdsWriter::is_reader_absent`]を使う。
/// 用途は調査・監視・ログ出力に限る。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PublicationMatchedStatus {
    /// マッチしたreaderの累計数
    pub total_count: u32,
    /// 前回の読み出し以降の`total_count`の変化量
    pub total_count_change: i32,
    /// 現在マッチしているreaderの数
    pub current_count: u32,
    /// 前回の読み出し以降の`current_count`の変化量
    pub current_count_change: i32,
}

impl From<dds_publication_matched_status_t> for PublicationMatchedStatus {
    fn from(status: dds_publication_matched_status_t) -> Self {
        Self {
            total_count: status.total_count,
            total_count_change: status.total_count_change,
            current_count: status.current_count,
            current_count_change: status.current_count_change,
        }
    }
}

/// [`WriterBuilder::watch_reader_absence`]指定時のみ持つ、リーダー不在監視の状態
struct AbsenceWatch {
    watch: Arc<MatchWatch>,
    /// ログに載せるトピック名。生成時に一度だけ取得する
    topic_name: String,
    /// 直前に`is_reader_absent`で観測した不在状態。遷移した時だけログを出すために使う
    last_absent: AtomicBool,
}

/// `Clone`は導出しない。[`Drop`]でエンティティを`dds_delete`するため、
/// cloneを許すと同じハンドルが二重に削除されてしまう。複数箇所で共有したい場合は
/// 呼び出し側で`Arc<Mutex<DdsWriter<T>>>`などに包むこと
pub struct DdsWriter<T> {
    p: DdsEntity,
    /// 生成に使ったトピック。手放すとUAFになりうる理由は[`DdsTopic`]を参照
    _topic: DdsTopic<T>,
    /// 生成元のpublisher。先に消えるとC側で子のwriterごと削除されるため保持する
    _parent: Keepalive,
    _maybe_listener: Option<DdsListener>,
    absence_watch: Option<AbsenceWatch>,
}

impl<T> DdsWriter<T> {
    pub fn create(
        entity: &dyn DdsWritable,
        topic: DdsTopic<T>,
        maybe_qos: Option<DdsQos>,
        maybe_listener: Option<DdsListener>,
    ) -> Result<Self, DDSError> {
        Self::create_entity(entity, topic, maybe_qos, maybe_listener, None)
    }

    /// [`WriterBuilder::create`]の内部実装。`listener_builder`には利用者が
    /// [`WriterBuilder::with_listener_builder`]経由で設定済みのコールバックが入りうるため、
    /// 内部コールバックはchainして追加する。`absence_timeout`は
    /// [`WriterBuilder::watch_reader_absence`]経由で渡される
    pub(crate) fn create_with_watch(
        entity: &dyn DdsWritable,
        topic: DdsTopic<T>,
        maybe_qos: Option<DdsQos>,
        listener_builder: DdsListenerBuilder,
        absence_timeout: Option<Duration>,
    ) -> Result<Self, DDSError> {
        let watch = absence_timeout.map(|timeout| Arc::new(MatchWatch::new(timeout)));

        // `listener_builder`は値渡しのビルダーなので、chain先はここで一度だけ再束縛する
        let listener_builder = match &watch {
            Some(watch) => {
                let watch = watch.clone();
                listener_builder.chain_publication_matched(move |_entity, status| {
                    // 復帰(0→対向あり)は猶予無しで反映する(4-d)。ここでは状態更新のみ行い、
                    // ログは`is_reader_absent`を呼び出し側が実際に観測した時点で出す
                    watch.update(status.current_count);
                })
            }
            None => listener_builder,
        };

        let absence_watch = watch.map(|watch| AbsenceWatch {
            watch,
            topic_name: topic_name_for_log(topic.entity()),
            last_absent: AtomicBool::new(false),
        });

        let listener = listener_builder.build();
        Self::create_entity(entity, topic, maybe_qos, Some(listener), absence_watch)
    }

    fn create_entity(
        entity: &dyn DdsWritable,
        topic: DdsTopic<T>,
        maybe_qos: Option<DdsQos>,
        maybe_listener: Option<DdsListener>,
        absence_watch: Option<AbsenceWatch>,
    ) -> Result<Self, DDSError> {
        unsafe {
            let w = dds_create_writer(
                entity.entity().entity(),
                topic.entity().entity(),
                maybe_qos.map_or(std::ptr::null(), |q| q.into()),
                maybe_listener
                    .as_ref()
                    .map_or(std::ptr::null(), |l| l.into()),
            );

            if w >= 0 {
                Ok(DdsWriter {
                    p: DdsEntity::new(w),
                    _topic: topic,
                    _parent: entity.keepalive(),
                    _maybe_listener: maybe_listener,
                    absence_watch,
                })
            } else {
                Err(DDSError::from(w))
            }
        }
    }

    /// マッチするreaderが不在確定しているか
    /// ([`WriterBuilder::watch_reader_absence`]未指定なら常に`false`)
    ///
    /// メッセージ生成や[`Self::loan`]のコスト自体を避けたい場合に、`write`の手前で見る。
    /// 参照するのはリスナーが更新したメモリ上の状態だけで、FFI呼び出しもシステムコールも
    /// 無いため送信周期ごとに呼んでよい。
    ///
    /// 呼び出した時点の状態を最終観測値として保持し、前回から遷移した瞬間だけログを出す
    /// 副作用がある。そのため呼び出し間隔より短く不在→復帰→不在とフラップした場合、
    /// 両端の観測値が一致してログに残らないことがある。
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use std::time::Duration;
    /// # use cdds_derive::Topic;
    /// # use serde::{Deserialize, Serialize};
    /// # use cyclonedds_rs::*;
    /// # #[derive(Serialize, Deserialize, Topic, Default)]
    /// # struct HeavyMessage { value: u32 }
    /// # fn build_expensive_message() -> Arc<HeavyMessage> { Arc::new(HeavyMessage::default()) }
    /// # async fn run() -> Result<(), DDSError> {
    /// # let participant = DdsParticipant::get_or_create(Some(0))?;
    /// # let publisher = DdsPublisher::create(participant, None, None)?;
    /// # let topic = HeavyMessage::create_topic(participant, None, None, None)?;
    /// let mut writer = WriterBuilder::new()
    ///     .watch_reader_absence(Duration::from_secs(1))
    ///     .create(&publisher, topic)?;
    /// let mut interval = tokio::time::interval(Duration::from_millis(100));
    /// loop {
    ///     interval.tick().await;
    ///     // 不在の間は重いメッセージ生成ごと飛ばす。復帰は次のtickで拾える
    ///     if writer.is_reader_absent() {
    ///         continue;
    ///     }
    ///     writer.write(build_expensive_message())?;
    /// }
    /// # }
    /// ```
    ///
    /// **スキップ回数を数える必要はない。** `timeout`自体がヒステリシスとして働くため、
    /// `true`が返った時点がそのまま送信を止めるタイミングになる。
    ///
    /// 猶予時間は不在方向にのみ掛かる: readerが0件から1件以上に戻った瞬間、
    /// `timeout`を待たず即座に`false`に戻る。復帰を遅らせて送信を余分に止めるより、
    /// 復帰直後に無駄な送信が数回出る方が実害が小さいという判断による。
    ///
    /// 判定と`write`の間でreaderが消えるレースは残るが、余分な送信が数回出るだけで
    /// timeoutの猶予に比べて無視できるため許容する。
    pub fn is_reader_absent(&self) -> bool {
        let Some(absence_watch) = &self.absence_watch else {
            return false;
        };
        let absent = absence_watch.watch.is_absent();
        let was_absent = absence_watch.last_absent.swap(absent, Ordering::Relaxed);
        if absent != was_absent {
            if absent {
                info!(
                    topic = %absence_watch.topic_name,
                    timeout = ?absence_watch.watch.timeout(),
                    "matched reader is now considered absent"
                );
            } else {
                info!(topic = %absence_watch.topic_name, "matched reader has recovered");
            }
        }
        absent
    }

    /// マッチしているreaderの数を取得する
    ///
    /// 注意点は[`PublicationMatchedStatus`]を参照。
    pub fn publication_matched_status(&self) -> Result<PublicationMatchedStatus, DDSError> {
        let mut status = dds_publication_matched_status_t::default();
        let ret =
            unsafe { dds_get_publication_matched_status(self.entity().entity(), &mut status) };
        if ret == 0 {
            Ok(PublicationMatchedStatus::from(status))
        } else {
            Err(DDSError::from(ret))
        }
    }

    pub fn set_listener(&mut self, listener: DdsListener) -> Result<(), DDSError> {
        unsafe {
            let refl = &listener;
            let rc = dds_set_listener(self.p.entity(), refl.into());
            if rc == 0 {
                self._maybe_listener = Some(listener);
                Ok(())
            } else {
                Err(DDSError::from(rc))
            }
        }
    }

    /// [crate::DdsReader]で読んだサンプルを転送する
    ///
    // 受信データの場合はすでにシリアライズされているため、型を知らなくても転送できる
    pub fn forward(&mut self, sample: &Sample<T>) -> Result<(), DDSError> {
        let serdata = sample.serdata.expect("Sample has no SerData");
        unsafe {
            let ret = dds_forwardcdr(self.entity().entity(), serdata);
            if ret >= 0 {
                Ok(())
            } else {
                Err(DDSError::from(ret))
            }
        }
    }
}

impl<T> DdsWriter<T>
where
    T: Sized + TopicType,
{
    pub fn write_to_entity(entity: &DdsEntity, msg: std::sync::Arc<T>) -> Result<(), DDSError> {
        unsafe {
            let sample = Sample::<T>::from(msg);
            let sample = &sample as *const Sample<T>;
            let sample = sample as *const ::std::os::raw::c_void;
            let ret = dds_write(entity.entity(), sample);
            if ret >= 0 {
                Ok(())
            } else {
                Err(DDSError::from(ret))
            }
        }
    }

    /// fixed_sizeでない型のサンプルを書き込むためのメソッド
    pub fn write(&mut self, msg: std::sync::Arc<T>) -> Result<(), DDSError> {
        Self::write_to_entity(&self.p, msg)
    }
}

impl<T> DdsWriter<T>
where
    T: Sized + FixedTopicType,
{
    /// Cyclone DDSの貸出しバッファを要求する。
    ///
    /// cyclonedds-rsの型サポートはPSMXのシリアライズ済み経路を使うため、現在は
    /// `DDS_RETCODE_UNSUPPORTED` を返す。直接のRAW貸出しはRustの`Sample<T>`表現と
    /// 互換なアプリケーション型ABIを提供してから有効化する。
    pub fn loan(&mut self) -> Result<Loaned<T>, DDSError> {
        let mut p_sample: *mut T = std::ptr::null_mut();
        let voidpp: *mut *mut T = &mut p_sample;
        let voidpp = voidpp as *mut *mut c_void;
        let res = unsafe { dds_loan_sample(self.p.entity(), voidpp) };
        if res == 0 {
            Ok(Loaned {
                inner: LoanedInner::Uninitialized(
                    NonNull::new(p_sample).unwrap(),
                    self.entity().clone(),
                ),
            })
        } else {
            Err(DDSError::from(res))
        }
    }

    // Return the loaned buffer.  If the buffer was initialized, then write the data to be published
    pub fn return_loan(&mut self, mut buffer: Loaned<T>) -> Result<(), DDSError> {
        let res = match &mut buffer.inner {
            LoanedInner::Uninitialized(p, entity) => {
                let mut p_sample = p.as_ptr();
                let voidpp: *mut *mut T = &mut p_sample;
                let voidpp = voidpp as *mut *mut c_void;
                unsafe { dds_return_loan(entity.entity(), voidpp, 1) }
            }
            LoanedInner::Initialized(p, entity) => {
                let p_sample = p.as_ptr();
                unsafe { dds_write(entity.entity(), p_sample as *const c_void) }
            }
            LoanedInner::Empty => 0,
        };

        if res == 0 {
            Ok(())
        } else {
            Err(DDSError::from(res))
        }
    }
}

impl<T> Entity for DdsWriter<T> {
    fn entity(&self) -> &DdsEntity {
        &self.p
    }
}

impl<T> Drop for DdsWriter<T> {
    fn drop(&mut self) {
        unsafe {
            let ret: DDSError = cyclonedds_sys::dds_delete(self.p.entity()).into();
            if DDSError::DdsOk != ret {
                error!("cannot delete Writer: {}", ret);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::common::TestDomain;
    use crate::*;
    use cdds_derive::Topic;
    use serde::{Deserialize, Serialize};

    #[repr(C)]
    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
    enum Position {
        #[default]
        Front,
        Back,
    }

    #[derive(Serialize, Deserialize, Topic, Debug, PartialEq)]
    #[cdds(fixed_size)]
    struct TestTopic {
        a: u32,
        b: u16,
        c: [u8; 10],
        d: [u8; 15],
        #[topic_key]
        e: u32,
        #[topic_key_enum]
        pos: Position,
    }

    impl Default for TestTopic {
        fn default() -> Self {
            Self {
                a: 10,
                b: 20,
                c: [0; 10],
                d: [1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5],
                e: 0,
                pos: Position::default(),
            }
        }
    }
    /// Why: `watch_reader_absence`は`with_listener`/`with_listener_builder`より優先される
    ///      べきで、逆転しているとhook済み`with_listener`を渡しただけで不在監視が
    ///      黙って無効化される(コンパイルは通り、以後`is_reader_absent()`が常に`false`を
    ///      返す実行時の破壊になる)
    /// Method: 排他オプションの組み合わせでwriterを生成し、margin後の`is_reader_absent()`を
    ///         期待値と比較する。`watch_reader_absence`指定時は常に`true`になるはず
    #[test]
    fn test_builder_option_priority_enables_absence_watch() {
        struct Case {
            with_listener: bool,
            with_listener_builder: bool,
            watch_reader_absence: bool,
            expected_absent: bool,
        }

        let timeout = Duration::from_millis(100);
        let margin = timeout * 3;
        let cases = vec![
            // オプション未指定は常にfalse
            Case {
                with_listener: false,
                with_listener_builder: false,
                watch_reader_absence: false,
                expected_absent: false,
            },
            Case {
                with_listener: true,
                with_listener_builder: false,
                watch_reader_absence: false,
                expected_absent: false,
            },
            Case {
                with_listener: false,
                with_listener_builder: true,
                watch_reader_absence: false,
                expected_absent: false,
            },
            Case {
                with_listener: true,
                with_listener_builder: true,
                watch_reader_absence: false,
                expected_absent: false,
            },
            Case {
                with_listener: false,
                with_listener_builder: false,
                watch_reader_absence: true,
                expected_absent: true,
            },
            // watch_reader_absenceが優先され、with_listenerを渡していても監視が働く
            Case {
                with_listener: true,
                with_listener_builder: false,
                watch_reader_absence: true,
                expected_absent: true,
            },
            Case {
                with_listener: false,
                with_listener_builder: true,
                watch_reader_absence: true,
                expected_absent: true,
            },
        ];

        let participant =
            DdsParticipant::get_or_create(Some(TestDomain::WriterBuilderPriority.id())).unwrap();
        let publisher = DdsPublisher::create(participant, None, None).unwrap();

        for (i, case) in cases.into_iter().enumerate() {
            let topic = TestTopic::create_topic(
                participant,
                Some(&format!("writer_builder_priority_{i}")),
                None,
                None,
            )
            .unwrap();
            let mut builder = WriterBuilder::new();
            if case.with_listener {
                builder = builder.with_listener(DdsListenerBuilder::new().build());
            }
            if case.with_listener_builder {
                builder = builder.with_listener_builder(DdsListenerBuilder::new());
            }
            if case.watch_reader_absence {
                builder = builder.watch_reader_absence(timeout);
            }
            let writer = builder.create(&publisher, topic).unwrap();
            std::thread::sleep(margin);
            assert_eq!(writer.is_reader_absent(), case.expected_absent, "case {i}");
        }
    }

    /// Why: `watch_reader_absence`はreader不在を`is_reader_absent`で参照可能にする新機能で、
    ///      不在確定・復帰の非対称性(4-d)と、書き込みAPIの挙動を変えないことを保証する
    /// Method: 同一ドメイン上でシナリオを順に実行し、`is_reader_absent()`(必要なら`write`の
    ///         結果も併せて)を期待値と構造体比較する
    #[test]
    fn test_watch_reader_absence() {
        #[derive(Debug, PartialEq)]
        struct Outcome {
            is_reader_absent: bool,
            write_result: Result<(), DDSError>,
        }

        let timeout = Duration::from_millis(300);
        let margin = timeout * 3;
        let participant =
            DdsParticipant::get_or_create(Some(TestDomain::WriterNoReader.id())).unwrap();
        let publisher = DdsPublisher::create(participant, None, None).unwrap();
        let subscriber = DdsSubscriber::create(participant, None, None).unwrap();

        // ケース1: readerを一度も作らない -> timeout経過前はfalse、経過後はtrue
        {
            let topic =
                TestTopic::create_topic(participant, Some("watch_reader_absence_none"), None, None)
                    .unwrap();
            let writer = WriterBuilder::new()
                .watch_reader_absence(timeout)
                .create(&publisher, topic)
                .unwrap();
            assert!(
                !writer.is_reader_absent(),
                "生成直後から不在確定してはいけない"
            );
            std::thread::sleep(margin);
            assert!(writer.is_reader_absent());
        }

        // ケース2: readerが居る -> 常にfalse
        {
            let topic = TestTopic::create_topic(
                participant,
                Some("watch_reader_absence_present"),
                None,
                None,
            )
            .unwrap();
            let writer = WriterBuilder::new()
                .watch_reader_absence(timeout)
                .create(&publisher, topic.clone())
                .unwrap();
            let _reader = DdsReader::create(&subscriber, topic, None, None).unwrap();
            std::thread::sleep(margin);
            assert!(!writer.is_reader_absent());
        }

        // ケース3: readerをdropして消す -> dropの概ねtimeout後にtrue
        {
            let topic =
                TestTopic::create_topic(participant, Some("watch_reader_absence_gone"), None, None)
                    .unwrap();
            let writer = WriterBuilder::new()
                .watch_reader_absence(timeout)
                .create(&publisher, topic.clone())
                .unwrap();
            let reader = DdsReader::create(&subscriber, topic, None, None).unwrap();
            // マッチが成立するのを待ってからreaderを消す
            std::thread::sleep(Duration::from_millis(100));
            drop(reader);
            std::thread::sleep(margin);
            assert!(writer.is_reader_absent());
        }

        // ケース4: dropしたreaderを作り直す -> timeoutを待たず即座にfalse(4-dの非対称性)
        {
            let topic = TestTopic::create_topic(
                participant,
                Some("watch_reader_absence_recovers"),
                None,
                None,
            )
            .unwrap();
            let writer = WriterBuilder::new()
                .watch_reader_absence(timeout)
                .create(&publisher, topic.clone())
                .unwrap();
            let reader = DdsReader::create(&subscriber, topic.clone(), None, None).unwrap();
            std::thread::sleep(Duration::from_millis(100));
            drop(reader);
            std::thread::sleep(margin);
            assert!(writer.is_reader_absent());

            let _reader = DdsReader::create(&subscriber, topic, None, None).unwrap();
            // 復帰には猶予が無いため、マッチイベントが届く程度の短い待ちで十分
            std::thread::sleep(Duration::from_millis(100));
            assert!(!writer.is_reader_absent());
        }

        // ケース5: オプション未指定 -> 常にfalse
        {
            let topic = TestTopic::create_topic(
                participant,
                Some("watch_reader_absence_disabled"),
                None,
                None,
            )
            .unwrap();
            let writer = WriterBuilder::new().create(&publisher, topic).unwrap();
            std::thread::sleep(margin);
            assert!(!writer.is_reader_absent());
        }

        // ケース6: is_reader_absent()がtrueの間もwriteは従来どおり成功する
        {
            let topic = TestTopic::create_topic(
                participant,
                Some("watch_reader_absence_write_still_succeeds"),
                None,
                None,
            )
            .unwrap();
            let mut writer = WriterBuilder::new()
                .watch_reader_absence(timeout)
                .create(&publisher, topic)
                .unwrap();
            std::thread::sleep(margin);

            let actual = Outcome {
                is_reader_absent: writer.is_reader_absent(),
                write_result: writer.write(Arc::new(TestTopic::default())),
            };
            assert_eq!(
                actual,
                Outcome {
                    is_reader_absent: true,
                    write_result: Ok(()),
                }
            );
        }
    }

    /// Why: matched statusはFFI越しに対向数を読める調査/監視用の窓口で、
    ///      writer/reader双方が期待どおりのtotal_count/current_countを返すことを保証する
    /// Method: writerとreaderをマッチさせ、双方のstatusを期待値の構造体と比較する
    #[test]
    fn test_matched_status() {
        #[derive(Debug, PartialEq)]
        struct Outcome {
            publication: PublicationMatchedStatus,
            subscription: SubscriptionMatchedStatus,
        }

        let participant =
            DdsParticipant::get_or_create(Some(TestDomain::MatchedStatus.id())).unwrap();
        let publisher = DdsPublisher::create(participant, None, None).unwrap();
        let subscriber = DdsSubscriber::create(participant, None, None).unwrap();
        let topic =
            TestTopic::create_topic(participant, Some("matched_status_topic"), None, None).unwrap();

        let writer = DdsWriter::create(&publisher, topic.clone(), None, None).unwrap();
        let reader = DdsReader::create(&subscriber, topic, None, None).unwrap();
        // マッチが成立するのを待つ
        std::thread::sleep(Duration::from_millis(300));

        let actual = Outcome {
            publication: writer.publication_matched_status().unwrap(),
            subscription: reader.subscription_matched_status().unwrap(),
        };
        assert_eq!(
            actual,
            Outcome {
                publication: PublicationMatchedStatus {
                    total_count: 1,
                    total_count_change: 1,
                    current_count: 1,
                    current_count_change: 1,
                },
                subscription: SubscriptionMatchedStatus {
                    total_count: 1,
                    total_count_change: 1,
                    current_count: 1,
                    current_count_change: 1,
                },
            }
        );
    }
}
