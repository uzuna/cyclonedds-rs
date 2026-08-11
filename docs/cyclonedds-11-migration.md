# Cyclone DDS 11.0.0 移行計画

## 決定事項

- Cyclone DDS 11.0.0 を唯一の対応世代とする。
- `shm` は Linux と Iceoryx 2.0 を対象にする。Iceoryx と `psmx_iox` はシステム提供とし、利用者が動的ライブラリ検索パスを設定する。
- 型サポートは XCDR1 に固定する。XCDR2 と CDR クレートの拡張は別スパイクで扱う。
- PR ごとに RouDi、`psmx_iox`、1 MiB 共有メモリ送受信を実行する。

## 実施順序

1. vendored Cyclone DDS と `cyclonedds-sys` の取得コミットを 11.0.0 に統一し、生成バインディングを更新する。
2. `ddsi_sertype_ops` と `ddsi_serdata_ops` を 11.0.0 ABI に移行する。XCDR1 以外のエンコーディングは拒否する。
3. 旧Iceoryxチャンクの直接操作を PSMX のシリアライズ済み経路へ置換し、`from_loaned_sample` と `from_psmx` を実装する。Rustの `Sample<T>` はCycloneの生`T` ABIと互換ではないため、raw loan は明示的に未対応とする。
4. PSMX設定、vendoredパッケージ、CIを更新する。Linux CIでは動的プラグインを発見できる環境でSHM統合試験を実行する。
5. 通常通信、XCDR1のCDR、1 MiB SHMを検証し、失敗時は内部ABIへの依存箇所を追加せずに原因を記録する。

## 完了条件

- `cargo test --all --all-features -- --test-threads=1` が通る。
- `make test-local-shm` が11.0.0と`psmx_iox`で1 MiBを送受信する。
- CIで通常通信とPSMX経路をPRごとに実行する。

## スパイク判定

- XCDR1のシリアライズ済み PSMX 経路で1 MiBのプロセス間送受信を確認した。
- `Sample<T>` を共有メモリ上の生 `T` として借り出す設計は ABI 非互換のため採用しない。ゼロコピーの raw loan を提供するには、別途Rust側のアプリケーション型ABIを設計・検証する。
