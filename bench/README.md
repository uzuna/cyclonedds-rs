# ベンチマーク

`cyclonedds-bench` は、Cyclone DDS のUDP、共有メモリ、CDR処理を同じケース定義で測定します。

## ローカル実行

```sh
make test-local-smoke
make test-local-cdr
make test-local-udp
make test-local-shm
make test-local-all
```

結果は `target/bench-results/<run-id>/` に保存されます。`BENCH_RUN_ID` と `BENCH_OUT` は上書きできます。

UDP境界ケースは、loの65,535B境界に対して、RTPSサンプルのシリアライズサイズが65,532Bと65,536Bになる payload を比較します。Cyclone DDSの大きなサンプル処理に合わせ、4バイト整列済みのシリアライズサイズを使用します。

共有メモリケースはベンチマーク実行時にRouDiを起動します。RouDiの起動に失敗した場合はUDPへのフォールバックを結果として採用せず、ケースとコマンドを失敗させます。

## 出力

- `cases.json`: ケースとQoSの定義
- `metadata.json`: commit、vendor、ツールチェーン、ホスト、実行時設定
- `results.jsonl`: ケースごとの結果。失敗ケースも1行のエラー記録を出力
