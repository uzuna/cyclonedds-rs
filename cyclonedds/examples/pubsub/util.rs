use std::time::Duration;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// ロガーセットアップ
pub fn init() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

/// shared memory transportを使う設定を環境変数にセットする
///
/// DDSエンティティを生成する前、メインスレッドが他スレッドを起動するより前に呼ぶこと
pub fn use_shm_config() {
    let config_path = format!("{}/testdata/cyclonedds_shm.xml", env!("CARGO_MANIFEST_DIR"));
    // SAFETY: DDS初期化前にメインスレッドから呼ばれるため、他スレッドが環境変数を読む前に設定できる
    unsafe { std::env::set_var("CYCLONEDDS_URI", config_path) };
}

/// monotonic timestampを取得する
pub fn monotonic_ts() -> Duration {
    nix::time::ClockId::CLOCK_MONOTONIC.now().unwrap().into()
}
