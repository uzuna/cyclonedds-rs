//! 受信経路の統計
//!
//! CycloneDDSはserdata構築に失敗したサンプルを警告ログ1行とともに捨て、
//! リーダー側のステータスには一切反映しない(RTPS的には配送済み扱いになるため
//! RELIABLEでも再送されない)。読み出しAPIからは「最初から届かなかった」ようにしか
//! 見えないため、破棄を観測する手段としてこのカウンタを持つ

use std::sync::atomic::{AtomicU64, Ordering};

/// サンプルを破棄した理由
pub(crate) enum DiscardReason {
    /// フラグチェーンが検証を満たさなかった
    InvalidFragchain,
    /// 受信バッファからCDRを組み立てられなかった
    CdrAssemblyFailed,
}

static INVALID_FRAGCHAIN: AtomicU64 = AtomicU64::new(0);
static CDR_ASSEMBLY_FAILED: AtomicU64 = AtomicU64::new(0);

/// 破棄したサンプルの累計件数
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DiscardedSamples {
    /// フラグチェーンが検証を満たさず破棄した件数
    pub invalid_fragchain: u64,
    /// CDRを組み立てられずに破棄した件数
    pub cdr_assembly_failed: u64,
}

impl DiscardedSamples {
    /// 理由を問わない破棄の合計
    pub fn total(&self) -> u64 {
        self.invalid_fragchain + self.cdr_assembly_failed
    }
}

pub(crate) fn count_discarded_sample(reason: DiscardReason) {
    // 監視用の統計で他のメモリ操作と順序関係を持たないためRelaxedで十分
    let counter = match reason {
        DiscardReason::InvalidFragchain => &INVALID_FRAGCHAIN,
        DiscardReason::CdrAssemblyFailed => &CDR_ASSEMBLY_FAILED,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

/// 破棄したサンプルの累計を取得する
///
/// 破棄はDDSのステータスに現れないため、監視する場合はこの累計の差分を見る。
/// 破棄はserdata構築のコールバックで起きて参加者もリーダーも特定できないため、
/// 粒度はプロセス全体でドメインやトピックごとには分離されない
/// (どのトピックかは同時に出力される`ERROR`ログの`type_name`で判別する)
pub fn discarded_samples() -> DiscardedSamples {
    DiscardedSamples {
        invalid_fragchain: INVALID_FRAGCHAIN.load(Ordering::Relaxed),
        cdr_assembly_failed: CDR_ASSEMBLY_FAILED.load(Ordering::Relaxed),
    }
}
