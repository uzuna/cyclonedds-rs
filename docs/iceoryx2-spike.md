# iceoryx2 PSMX スパイク

Cyclone DDS 11.0.0 が同梱する iceoryx2 版 PSMX プラグイン `psmx_iox2` を、
cyclonedds-rs から使えるかを確認した記録。

## 結論

**現時点では使えない。** 送信側は動くが受信側が失敗する。
`psmx_iox` (iceoryx1) 経路は引き続き正常なので、そちらを維持する。

期待していた「調停プロセス不要」という性質自体は成立していた。
`iox-roudi` を一切起動せずに participant 生成、discovery、writer/reader の
マッチ、2010件の送信までが完了している。落ちるのは受信経路だけ。

## 検証環境

- Cyclone DDS 11.0.0 (`1f7f75c0`)、`ENABLE_ICEORYX=YES` と `ENABLE_ICEORYX2=ON` の同時有効ビルド
- `libddsc.so.11.0.0` / `libpsmx_iox.so` / `libpsmx_iox2.so` が同居し、XML の
  `PubSubMessageExchange` だけで経路を選べる状態
- 再現手順は `make -C vendor build-iox2` と `make test-local-iox2`

## iceoryx2 の版ごとの結果

| 版 | リリース日 | ビルド | 送信 | 受信 |
| --- | --- | --- | --- | --- |
| v0.7.0 | 2025-09-13 | 成功 | 成功 (2010件) | **失敗** |
| v0.8.1 | 2026-01-15 | 成功 | 成功 (2010件) | **失敗** |
| main (`37835e032`) | 2026-08-12 | **失敗** | — | — |

main がビルドできないのは、`psmx_iox2_impl.c:896` が使う
`iox2_unable_to_deliver_strategy_e_DISCARD_SAMPLE` と `..._BLOCK` が
iceoryx2 側に存在しなくなっているため。上流 Cyclone DDS の CI は iceoryx2 を
main から版数固定なしで取得しており、11.0.0 のプラグインが追随できていない。

vendored submodule は v0.8.1 に固定した。3版とも受信は通らないが、v0.8.1 は
ビルドが通り、かつ Cyclone DDS 11.0.0 のタグ (2026-02-19) に最も近い世代のため、
再現用の起点として最も妥当と判断した。

## 失敗の内容

`take` が `DdsError` を返す。原因は
[serdata.rs](../cyclonedds/src/serdata.rs) の `serdata_from_psmx` が受け取る
`dds_loaned_sample` のメタデータが壊れていること。一時プローブで実測した値:

```
sample_state=1195787588 cdr_identifier=29040 cdr_options=29554 sample_size=1397903696
sample_state=4          cdr_identifier=0     cdr_options=0     sample_size=280
sample_state=3014832560 cdr_identifier=56796 cdr_options=57310 sample_size=3216948668
```

`sample_state=4` (`SERIALIZED_DATA`)、`sample_size=280` の行が正しい値で、
256B ペイロードの CDR 表現と一致する。それ以外は壊れており、32bit 値を
ASCII として読むと `"DTEG"` `"P1US"` のような文字列断片になる。
user header (`dds_psmx_metadata_t`) の読み出し位置が、iceoryx2 が
サービス名などの文字列を置いている領域を指していることを示す。

`serdata_from_psmx` は不正なメタデータに対して null を返す実装なので、
Cyclone 側はサンプル生成失敗として扱い、`take` がエラーになる。

## なぜこの経路に入るか

cyclonedds-rs の `SerType` は `set_is_memcpy_safe(0)` を設定する。
Rust の `Sample<T>` が Cyclone の生 `T` ABI と互換でないためで、この判断自体は
[cyclonedds-11-migration.md](cyclonedds-11-migration.md) で確定済み。

その結果 `psmx_iox2` 側では
[psmx_iox2_impl.c:828](../vendor/cyclonedds/src/psmx_iox/src/psmx_iox2_impl.c)
で型が `iox2_type_variant_e_DYNAMIC` に分類され、確保戦略も `BEST_FIT` になる。
固定長型が通る `FIXED_SIZE` 経路とは別の実装を踏むため、この不具合は
cyclonedds-rs のようにシリアライズ経路を使う利用者に固有の可能性が高い。

なお `psmx_iox2_impl.c` には未解決の疑問がコメントとして残っている
(`set_history_size` に「受信順が乱れる原因では」、`set_enable_safe_overflow` は
ブロッキング write を招くとしてコメントアウト)。成熟度は `psmx_iox` より低い。

## 参考: iceoryx1 側の対照結果

同一ビルド、同一ハーネスで `psmx_iox` を使った場合:

| ケース | 件数 | 結果 |
| --- | --- | --- |
| `throughput-1mib-shm` | 110 | ok / 欠落0 / 重複0 / 順序違反0 |
| `order-256b-shm` | 2010 | ok / 欠落0 / 重複0 / 順序違反0 |

`ENABLE_ICEORYX2=ON` を足しても iceoryx1 経路に回帰は無い。

## 次に試すなら

1. Cyclone DDS 11.0.x の後続版で `psmx_iox2` が更新されているか確認する。
   このスパイク時点の 11.0.0 では `psmx_iox2_impl.c` は 2025-09-16 以降
   触られていない。
2. `DYNAMIC` 型バリアントでの user header 取得が上流の既知問題か確認し、
   必要なら Cyclone DDS へ再現手順を報告する。
3. 上流が iceoryx2 の版を固定するまでは、この組み合わせを CI に入れない。

## この PR に含めたもの

採否は未定だが、次に試すときの起点として残している。

- `vendor/iceoryx2` submodule (v0.8.1) と `make -C vendor build-iox2`
- bench の `psmx` フィールドと `iox` / `iox2` の切り替え
- `throughput-1mib-iox2` / `order-256b-shm` / `order-256b-iox2` の3ケース
- `make test-local-iox2`

CI と deb パッケージは変更していない。`build-iox2` は既定の `build` の依存に
入れていないため、`ENABLE_ICEORYX2` を有効にしない限り従来どおりの成果物になる。
