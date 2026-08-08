//! 対向エンティティ(readerにとってのwriter、writerにとってのreader)の不在を
//! 時間の経過で確定させる共通部品。
//!
//! 状態の畳み込み([`MatchWatch`])と、期限が来たら起こす仕組み([`Watchdog`])に分かれる。

use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};
use std::thread;
use std::time::{Duration, Instant};

/// 対向エンティティの不在を一定時間の経過で確定させる
///
/// Why: 対向の有無そのものは瞬間的に揺れる(再起動、discoveryの往復)。
///      「今0か」ではなく「0の状態が続いたか」で判定して、瞬断で誤爆させない
pub(crate) struct MatchWatch {
    /// 不在がこの時間続いたら不在確定とする
    timeout: Duration,
    state: Mutex<MatchState>,
}

/// 対向の有無と、不在期間についての通知済みフラグを1つの状態機械として表す。
///
/// Why enumにするか: `notified`は不在計測中(`Absent`)にしか意味を持たない値だった。
/// 構造体でフラグとして持たせると「`Present`に戻すときにリセットし忘れる」という
/// 不具合クラスが生まれる。`Present`に`notified`フィールド自体を存在させないことで、
/// リセット漏れを型で表現不能にする
#[derive(Clone, Copy)]
enum MatchState {
    /// 対向が居る(計測していない)
    Present,
    /// 対向が0になってからの計測中。`notified`はこの不在期間についてウォッチドッグが
    /// 起床済みか(期限到達後に何度起こしても結果は変わらないので1不在期間に1回へ抑える)
    Absent { since: Instant, notified: bool },
}

impl MatchWatch {
    /// 生成時点を不在の起点にする(生成直後は必ず対向0のため)
    pub(crate) fn new(timeout: Duration) -> Self {
        Self::new_at(timeout, Instant::now())
    }

    fn new_at(timeout: Duration, now: Instant) -> Self {
        Self {
            timeout,
            state: Mutex::new(MatchState::Absent {
                since: now,
                notified: false,
            }),
        }
    }

    /// リスナーコールバックから呼ぶ。0への遷移で計測開始、非0への遷移で計測解除する
    pub(crate) fn update(&self, current_count: u32) {
        self.update_at(current_count, Instant::now());
    }

    fn update_at(&self, current_count: u32, now: Instant) {
        let mut state = self.state.lock().unwrap();
        if current_count == 0 {
            // 既に計測中ならその起点を保つ。ここで毎回now
            // に置き換えると瞬断のたびに猶予がリセットされてしまう
            if matches!(*state, MatchState::Present) {
                *state = MatchState::Absent {
                    since: now,
                    notified: false,
                };
            }
        } else {
            *state = MatchState::Present;
        }
    }

    /// 不在が確定しているか
    pub(crate) fn is_absent(&self) -> bool {
        self.is_absent_at(Instant::now())
    }

    fn is_absent_at(&self, now: Instant) -> bool {
        match *self.state.lock().unwrap() {
            MatchState::Absent { since, .. } => now.duration_since(since) >= self.timeout,
            MatchState::Present => false,
        }
    }

    /// 未通知かつ計測中なら不在確定時刻を返す。ウォッチドッグが起こす対象を絞るのに使う
    pub(crate) fn absence_deadline(&self) -> Option<Instant> {
        match *self.state.lock().unwrap() {
            MatchState::Absent {
                since,
                notified: false,
            } => {
                // `Duration::MAX`(実質の無効化)のような組み合わせでは`Instant + Duration`が
                // オーバーフローしてpanicする。呼び出し元はwatchdogスレッドなので、panicすると
                // 呼び出し元に何も伝わらないまま常駐スレッドだけが静かに死ぬ。
                // オーバーフローする場合は「監視しない」に丸めて安全側に倒す
                since.checked_add(self.timeout)
            }
            _ => None,
        }
    }

    /// エラーに載せる設定値としてのtimeout(実際の経過時間ではない)
    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }

    /// この不在期間を通知済みにする(ウォッチドッグから呼ぶ)
    pub(crate) fn mark_notified(&self) {
        if let MatchState::Absent { notified, .. } = &mut *self.state.lock().unwrap() {
            *notified = true;
        }
    }
}

/// 不在確定時刻で起こされる対象
///
/// Why: ウォッチドッグを`futures::ReaderShared`の具体型から切り離す。
///      DDSを一切起動しないダミー実装で時刻制御込みの単体テストができる。
pub(crate) trait Watched: Send + Sync {
    /// 未通知の不在確定時刻。`None`なら今回は監視対象外
    fn absence_deadline(&self) -> Option<Instant>;
    /// 不在確定を通知する(呼ばれた側でwakeする)
    fn notify_absence(&self);
}

/// 登録された`Watched`の不在確定時刻に達したら起こす、プロセス共有のスレッド
///
/// Why: 不在の開始はリスナーコールバックで分かるが、「そこから猶予時間が経過した」は
///      誰も通知してくれないため、futureを起こす時間源が別に要る。
/// Why not tokioタスク: リーダーの生存期間はランタイムの生存期間と一致しない。
///      ランタイム外での生成は`spawn`がpanicし、ランタイム停止後は監視だけが
///      静かに止まって「不在なのに永久Pending」へ戻る。常駐スレッドなら
///      executorの状態と無関係に期限を守れる
struct WatchdogState {
    entries: Vec<Weak<dyn Watched>>,
    /// `entries`を変更する(register/reschedule)たびに加算する世代カウンタ。
    ///
    /// Why: 走査でロックを解放してからwaitに入るまでの間に来たregister/rescheduleの
    ///      `notify_one`は、待機者が居ないため取りこぼされうる。走査直前に読んだ世代と、
    ///      wait突入直前(同じロックガードのまま)の世代を比較すれば、その間の変更を
    ///      検知してwaitをスキップし、取りこぼしを再走査で回収できる
    generation: u64,
}

pub(crate) struct Watchdog {
    state: Mutex<WatchdogState>,
    signal: Condvar,
}

/// poisoned(走査中のpanicでロック保持中に汚染された)場合でも、中身(Vec/カウンタ)の
/// 整合性自体はRustのunwind安全性により壊れていないため、監視を止めないよう無視して使う
fn lock_state(mutex: &Mutex<WatchdogState>) -> MutexGuard<'_, WatchdogState> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

static WATCHDOG: OnceLock<&'static Watchdog> = OnceLock::new();

/// プロセス共有のウォッチドッグを取得する。初回呼び出しで常駐スレッドを起動する
pub(crate) fn watchdog() -> &'static Watchdog {
    WATCHDOG.get_or_init(|| {
        let watchdog: &'static Watchdog = Box::leak(Box::new(Watchdog {
            state: Mutex::new(WatchdogState {
                entries: Vec::new(),
                generation: 0,
            }),
            signal: Condvar::new(),
        }));
        // Why not stack_sizeを縮める: 既定の2MiBは仮想アドレス空間の予約でしかなく、
        // 実RSSは実際に触れたページだけなので削っても実メモリは減らない。一方で
        // `Waker::wake`の中で何が起きるかはexecutorの実装次第(その場でfutureをpollする
        // 実装もWakerの契約上は合法)で、スタック消費の上限をこちら側で見積もれない。
        // 削る利点が無く、溢れる危険だけが増える。
        thread::Builder::new()
            .name("cyclonedds-watch".to_owned())
            .spawn(move || watchdog.run())
            .expect("failed to spawn cyclonedds-watch thread");
        watchdog
    })
}

impl Watchdog {
    /// 登録して起こす。保持は`Weak`のみで、エンティティのdropを妨げない。
    /// `upgrade`できなくなったエントリは走査中に取り除かれるが、次の走査まで待つと
    /// マッチが続く(=走査が長時間起きない)間に溜め続けてしまうため、登録の都度も掃除する
    pub(crate) fn register(&self, entry: Weak<dyn Watched>) {
        let mut state = lock_state(&self.state);
        state.entries.retain(|w| w.strong_count() > 0);
        state.entries.push(entry);
        state.generation += 1;
        drop(state);
        self.signal.notify_one();
    }

    /// 新しい期限を拾わせる。長時間マッチしていて待機中に対向が0へ遷移した場合、
    /// 次の期限が来るまで待ち続けてしまわないようにリスナーから呼ぶ
    pub(crate) fn reschedule(&self) {
        lock_state(&self.state).generation += 1;
        self.signal.notify_one();
    }

    /// 走査してexpired/nearestを求め、その時点(ロック解放直前)の世代を返す
    fn scan(&self) -> (Vec<Arc<dyn Watched>>, u64, Option<Instant>) {
        let mut expired = Vec::new();
        let mut state = lock_state(&self.state);
        let now = Instant::now();
        let mut nearest: Option<Instant> = None;
        state.entries.retain(|weak| match weak.upgrade() {
            None => false,
            Some(watched) => {
                match watched.absence_deadline() {
                    Some(deadline) if deadline <= now => expired.push(watched),
                    Some(deadline) => nearest = Some(nearest.map_or(deadline, |n| n.min(deadline))),
                    None => {}
                }
                true
            }
        });
        let seen_generation = state.generation;
        drop(state);
        (expired, seen_generation, nearest)
    }

    /// 次の期限まで(無ければ無期限に)待つ。ただし走査後にここへ来るまでの間に
    /// register/rescheduleが起きていれば(世代が変わっていれば)、その変更を
    /// 取りこぼさないよう待たずに戻る(呼び出し元が次の走査をすぐやり直す)
    fn wait_for_next(&self, seen_generation: u64, nearest: Option<Instant>) {
        let mut state = lock_state(&self.state);
        if state.generation != seen_generation {
            return;
        }
        match nearest {
            Some(deadline) => {
                let now = Instant::now();
                if deadline > now {
                    state = self.signal.wait_timeout(state, deadline - now).unwrap().0;
                }
            }
            None => {
                state = self.signal.wait(state).unwrap();
            }
        }
        drop(state);
    }

    /// 走査・通知・待機の1周分
    fn run_once(&self) {
        let (expired, seen_generation, nearest) = self.scan();

        // ロックを解放してからwakeする。
        // Why: `Waker::wake`はexecutor次第でその場でfutureをpollしうる。pollは
        //      `MatchWatch`を読むので、ロックを持ったままwakeすると自己デッドロックになる。
        for watched in expired {
            watched.notify_absence();
        }

        self.wait_for_next(seen_generation, nearest);
    }

    fn run(&self) {
        loop {
            // `notify_absence`はexecutor次第でその場で利用者のfutureをinline pollしうる。
            // そこでpanicしても常駐スレッドまで巻き込むと、以後プロセス全体の不在検知が
            // 無言で止まってしまうため、1周ごとに区切って捕捉し走査を継続する。
            //
            // AssertUnwindSafe: このクロージャがキャプチャするのは`&self`のみで、
            // panic後に外へ持ち出す(不変条件が壊れた)可変状態を保持していないため安全。
            // `self.state`自体もpoison後は`lock_state`が無視して使う
            if let Err(payload) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.run_once()))
            {
                tracing::error!(
                    "cyclonedds-watch thread caught a panic and continues: {payload:?}"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Why: 不在判定は「0が続いた時間」で決まり、瞬断や再マッチで誤爆してはならない
    /// Method: 単一のMatchWatchにイベント(update)と判定時刻を順に適用し、各時点での
    ///         is_absent/absence_deadlineの有無を期待値と一括比較する
    #[test]
    fn match_watch_absence_transitions() {
        #[derive(Debug, PartialEq)]
        struct Snapshot {
            is_absent: bool,
            has_deadline: bool,
        }

        let timeout = Duration::from_millis(100);
        let t0 = Instant::now();
        let watch = MatchWatch::new_at(timeout, t0);

        // (適用するイベント, 判定時刻, 期待値)。イベントがNoneなら状態は変えず判定のみ行う
        let steps: Vec<(Option<u32>, Instant, Snapshot)> = vec![
            // 生成直後、timeout未経過は計測中だが未確定
            (
                None,
                t0,
                Snapshot {
                    is_absent: false,
                    has_deadline: true,
                },
            ),
            // timeout経過で不在確定
            (
                None,
                t0 + timeout,
                Snapshot {
                    is_absent: true,
                    has_deadline: true,
                },
            ),
            // 途中でマッチすると計測は解除される
            (
                Some(1),
                t0 + Duration::from_millis(50),
                Snapshot {
                    is_absent: false,
                    has_deadline: false,
                },
            ),
            // 解除後は同じ時刻を指しても不在確定しない
            (
                None,
                t0 + timeout,
                Snapshot {
                    is_absent: false,
                    has_deadline: false,
                },
            ),
            // 再度0になったら、その時刻から計測がやり直される(timeout未経過)
            (
                Some(0),
                t0 + Duration::from_millis(60),
                Snapshot {
                    is_absent: false,
                    has_deadline: true,
                },
            ),
            (
                None,
                t0 + Duration::from_millis(60) + timeout - Duration::from_millis(1),
                Snapshot {
                    is_absent: false,
                    has_deadline: true,
                },
            ),
            (
                None,
                t0 + Duration::from_millis(60) + timeout,
                Snapshot {
                    is_absent: true,
                    has_deadline: true,
                },
            ),
        ];

        for (event, at, expected) in steps {
            if let Some(count) = event {
                watch.update_at(count, at);
            }
            let actual = Snapshot {
                is_absent: watch.is_absent_at(at),
                has_deadline: watch.absence_deadline().is_some(),
            };
            assert_eq!(actual, expected);
        }

        // 通知済みにすると、不在の確定自体は保ったままdeadlineだけ隠れる
        // (1不在期間に1回だけウォッチドッグを起こせば足りるため)
        let still_absent_at = t0 + Duration::from_millis(60) + timeout;
        watch.mark_notified();
        assert_eq!(watch.absence_deadline(), None);
        assert!(watch.is_absent_at(still_absent_at));
    }

    struct DummyWatched {
        deadline: Mutex<Option<Instant>>,
        notified: AtomicUsize,
    }

    impl DummyWatched {
        fn new(delay: Duration) -> Self {
            Self {
                deadline: Mutex::new(Some(Instant::now() + delay)),
                notified: AtomicUsize::new(0),
            }
        }
    }

    impl Watched for DummyWatched {
        fn absence_deadline(&self) -> Option<Instant> {
            *self.deadline.lock().unwrap()
        }

        fn notify_absence(&self) {
            self.notified.fetch_add(1, Ordering::SeqCst);
            *self.deadline.lock().unwrap() = None;
        }
    }

    /// 十分に緩い上限まで条件成立をポーリングする(このモジュール唯一の実時間依存の待ち方)
    fn wait_until(limit: Duration, mut condition: impl FnMut() -> bool) {
        let started = Instant::now();
        while !condition() {
            assert!(
                started.elapsed() < limit,
                "condition was not satisfied within {limit:?}"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    /// Why: 通知タイミングと、Weakが切れたエントリの自動除去はバックグラウンドスレッド越しに
    ///      実時間で確認しないと壊れたことに気づけない
    /// Method: 短い期限のダミーを登録して通知到達を待ち、drop後にreschedule()で走査を
    ///         促して該当Weakが登録から消えることを確認する
    #[test]
    fn watchdog_notifies_and_forgets_dropped_entries() {
        let entry = Arc::new(DummyWatched::new(Duration::from_millis(50)));
        let weak: Weak<dyn Watched> = Arc::downgrade(&entry) as Weak<dyn Watched>;
        watchdog().register(weak.clone());

        wait_until(Duration::from_secs(5), || {
            entry.notified.load(Ordering::SeqCst) > 0
        });

        drop(entry);
        watchdog().reschedule();
        wait_until(Duration::from_secs(5), || {
            !lock_state(&watchdog().state)
                .entries
                .iter()
                .any(|w| Weak::ptr_eq(w, &weak))
        });
    }

    /// Why: `absence_deadline`はwatchdogスレッドから呼ばれるため、`since + timeout`が
    ///      オーバーフローしてpanicすると呼び出し元に何も伝わらず常駐スレッドだけが死ぬ
    ///      (`stop_when_no_writer(Duration::MAX)`は不在検知を実質無効化する常套句なので、
    ///      指定されうる値である)
    /// Method: `Duration::MAX`を timeout に渡し、オーバーフローする時刻でも
    ///         panicせず「監視対象外」(deadline無し・不在にもならない)になることを確認する
    #[test]
    fn match_watch_absence_deadline_does_not_overflow() {
        let t0 = Instant::now();
        let watch = MatchWatch::new_at(Duration::MAX, t0);
        assert_eq!(watch.absence_deadline(), None);
        assert!(!watch.is_absent_at(t0 + Duration::from_secs(3600)));
    }

    /// Why: `register`は走査(scan)を待たずに呼ばれるため、この場でdrop済みエントリを
    ///      掃除しないと、マッチが続く(=走査が長時間起きない)間にreaderの生成/破棄を
    ///      繰り返すたびWeakが溜まり続ける
    /// Method: drop済みのWeakが混ざった状態でregisterし、直後の内部状態から
    ///         生きているエントリだけが残ることを確認する
    #[test]
    fn watchdog_register_sweeps_dropped_entries() {
        let watchdog = Watchdog {
            state: Mutex::new(WatchdogState {
                entries: Vec::new(),
                generation: 0,
            }),
            signal: Condvar::new(),
        };

        let dropped = Arc::new(DummyWatched::new(Duration::from_secs(60)));
        let dropped_weak: Weak<dyn Watched> = Arc::downgrade(&dropped) as Weak<dyn Watched>;
        watchdog.register(dropped_weak.clone());
        drop(dropped);

        let alive = Arc::new(DummyWatched::new(Duration::from_secs(60)));
        let alive_weak: Weak<dyn Watched> = Arc::downgrade(&alive) as Weak<dyn Watched>;
        watchdog.register(alive_weak.clone());

        let entries = &lock_state(&watchdog.state).entries;
        assert!(!entries.iter().any(|w| Weak::ptr_eq(w, &dropped_weak)));
        assert!(entries.iter().any(|w| Weak::ptr_eq(w, &alive_weak)));
    }

    /// Why: 走査がロックを解放してからwaitに入るまでの間に来たregister/rescheduleの
    ///      `notify_one`は待機者が居ないため取りこぼされ、そのままだと対向が一度も
    ///      現れないシナリオで監視が永久に止まる(この機能が防ごうとしている失敗モードそのもの)
    /// Method: 走査直後の世代をそのまま使い、その後にregisterで世代を進めてから
    ///         `wait_for_next`を呼ぶ。世代の変化を検知していれば待たずに戻るはずなので、
    ///         別スレッド越しのタイムアウトで「戻ってこない(=待ちに入った)」ことを弾く
    #[test]
    fn watchdog_skips_wait_when_registered_between_scan_and_wait() {
        let watchdog = Arc::new(Watchdog {
            state: Mutex::new(WatchdogState {
                entries: Vec::new(),
                generation: 0,
            }),
            signal: Condvar::new(),
        });

        let (_, seen_generation, nearest) = watchdog.scan();
        assert_eq!(nearest, None, "エントリが無いので走査直後はnearestも無い");

        // 走査とwaitの間にregisterが割り込んだ状況を再現する
        let entry = Arc::new(DummyWatched::new(Duration::from_secs(60)));
        watchdog.register(Arc::downgrade(&entry) as Weak<dyn Watched>);

        let (tx, rx) = std::sync::mpsc::channel();
        let watchdog_for_wait = watchdog.clone();
        thread::spawn(move || {
            watchdog_for_wait.wait_for_next(seen_generation, nearest);
            let _ = tx.send(());
        });
        rx.recv_timeout(Duration::from_millis(300)).expect(
            "世代の変化を無視してwaitに入り、registerのnotify_oneを取りこぼして戻ってこなかった",
        );
    }

    struct PanicOnNotify;

    impl Watched for PanicOnNotify {
        fn absence_deadline(&self) -> Option<Instant> {
            // 走査した瞬間に必ずexpired扱いになるよう、既に過ぎた時刻を返す
            Some(Instant::now() - Duration::from_millis(1))
        }

        fn notify_absence(&self) {
            panic!("simulated panic from an inline-polled future");
        }
    }

    /// Why: `Waker::wake`はexecutor次第でその場で利用者のfutureをinline pollしうる。
    ///      そこでpanicしても常駐スレッドまで道連れにすると、以後プロセス全体の
    ///      不在検知が無言で止まってしまう(`OnceLock`のため再起動もされない)
    /// Method: `run()`と同じ形でrun_once相当の処理をcatch_unwindで包んで直接呼び出し、
    ///         panicが外へ伝播しないこと、その後も内部状態(走査)が使い続けられることを確認する
    #[test]
    fn watchdog_survives_panicking_notify_absence() {
        let watchdog = Watchdog {
            state: Mutex::new(WatchdogState {
                entries: Vec::new(),
                generation: 0,
            }),
            signal: Condvar::new(),
        };
        let entry: Arc<dyn Watched> = Arc::new(PanicOnNotify);
        watchdog.register(Arc::downgrade(&entry));

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            watchdog.run_once();
        }));
        assert!(
            result.is_err(),
            "PanicOnNotify::notify_absenceのpanicが発生していない"
        );

        // panic後もMutexの中身が壊れておらず、走査を継続できることを確認する
        let (_, _, nearest) = watchdog.scan();
        assert_eq!(nearest, None);
    }
}
