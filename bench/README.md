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

## ローカルで履歴サイトを表示

既存のサイトを表示する場合:

```sh
python3 scripts/serve_bench_site.py --site-dir site
```

`site/index.html` がなくても、`target/bench-results/` に結果があれば最新の結果から自動生成します。

ローカル実行結果からサイトを生成して表示する場合:

```sh
BENCH_RUN_ID=local make test-local-all
python3 scripts/serve_bench_site.py \
  --input-dir target/bench-results/local \
  --site-dir target/bench-site
```

表示された `http://127.0.0.1:8000/` をブラウザで開いてください。`--port` でポートを変更できます。終了するには Ctrl-C を押します。

UDP境界ケースは、loの65,535B境界に対して、RTPSサンプルのシリアライズサイズが65,532Bと65,536Bになる payload を比較します。Cyclone DDSの大きなサンプル処理に合わせ、4バイト整列済みのシリアライズサイズを使用します。

共有メモリケースはベンチマーク実行時にRouDiを起動します。RouDiの起動に失敗した場合はUDPへのフォールバックを結果として採用せず、ケースとコマンドを失敗させます。

## 出力

- `cases.json`: ケースとQoSの定義
- `metadata.json`: commit、vendor、ツールチェーン、ホスト、実行時設定
- `results.jsonl`: ケースごとの結果。失敗ケースも1行のエラー記録を出力

## GitHub Pages の履歴サイト

`main` への push で公開される履歴サイトでは、ケースとメトリクスを選択して、直近100回分の値をコミット単位の折れ線グラフで確認できます。各点のツールチップと履歴表にはコミット、コミットメッセージ、実行時刻、値、前回値との差分率、ワークフローへのリンクが表示されます。

スループットは大きいほど良く、レイテンシ・シリアライズ・デシリアライズ時間は小さいほど良いものとして差分を表示します。現在は閾値による警告やCIの失敗は行いません。表示中のグラフデータはJSONとしてダウンロードできます。
