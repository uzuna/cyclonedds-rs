//! 実利用ケースのテスト([practical.rs](../practical.rs))で共有する待ちの上限とドメインID。

use std::time::Duration;

/// 実利用ケーステスト用のドメインID。
///
/// Why ここに置く: `src/common.rs`の`TestDomain`は`#[cfg(test)]`で単体テスト専用のため、
/// 結合テストからは参照できない。結合テスト側のIDはこのモジュールに集めて衝突を防ぐ。
/// Why 120番台: `TestDomain`(20番台の自動採番)や`tests/raii.rs`(12〜16)と重ならない範囲。
pub const PRACTICAL_DOMAIN_ID: u32 = 120;

/// 同一型で多数トピックを張るケース専用のドメインID。
///
/// Why 分ける: 30トピックを張る回帰テストなので、他ケースが残したdiscovery情報が混ざると
/// 「トピック数が原因か」の切り分けができなくなる。
pub const PRACTICAL_MANY_TOPICS_DOMAIN_ID: u32 = 121;

/// 待ち時間の倍率。遅いランナー向けに`PRACTICAL_TIMEOUT_SCALE`で上書きできる。
fn timeout_scale() -> u32 {
    std::env::var("PRACTICAL_TIMEOUT_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        // 0だと全待ちが即タイムアウトして「1件も届かない」扱いになり全ケースが落ちるため、
        // 0は既定の1にフォールバックする
        .filter(|v| *v > 0)
        .unwrap_or(1)
}

/// 待ちの上限。期待待ち時間ではなく「来なければ失敗させる」ための上限なので、
/// 遅いランナーでフレーキーにならないよう正常時(数ms〜数十ms)の桁を大きく上回る値を置く。
pub fn arrival_timeout() -> Duration {
    Duration::from_secs(5) * timeout_scale()
}

/// 「これ以上は来ない」ことを確かめるための静穏窓。
///
/// Why 必要: 期待件数が揃ったあとに余分が届いていないかは、待つ以外に確かめる方法がない。
/// 期待件数まではイベント待ちで進むので、固定の待ちが要るのはこの確認だけに閉じている。
pub fn silence_window() -> Duration {
    Duration::from_millis(300) * timeout_scale()
}
