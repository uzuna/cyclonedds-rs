# CycloneDDSのドメイン寿命とスレッド安全性

`DdsParticipant::create` / `DdsDomain::create` が `unsafe` である理由と、
安全な代替である `get_or_create` の設計をまとめる。

## 現象

同一プロセス内の複数スレッドが、同じDDSドメインIDに対して同時に生成
（`dds_create_participant`）と破棄（そのドメインの最後の参加者の解放によるドメインの
暗黙的な破棄）を行うと、cyclonedds C実装内部でレースが起きSEGVする。

これはRustバインディング側のバグではなく、cyclonedds本体が「同一プロセス内で同じ
ドメインIDを複数スレッドから同時に生成/破棄する」ケースに対してスレッドセーフでない
ことに起因する。したがって対処はRust側のAPIの形を変えることになる。

## 原因

採取したスタックトレース（抜粋）:

```
Thread 4 received signal SIGSEGV, Segmentation fault.
0x00007ffff7c79c75 in ddsi_serdata_unref (serdata=0x7fffe800b7a0)
    at .../core/ddsi/include/dds/ddsi/ddsi_serdata.h:251
#2  free_tkmap_instance (...) at .../core/ddsi/src/ddsi_tkmap.c:102
#3  ddsrt_chh_enum_unsafe (...) at .../ddsrt/src/hopscotch.c:597
#4  ddsi_tkmap_free (map=...) at .../core/ddsi/src/ddsi_tkmap.c:108
#5  rtps_fini (gv=...) at .../core/ddsi/src/q_init.c:2299
#6  dds_domain_free (vdomain=...) at .../core/ddsc/src/dds_domain.c:332
#7  dds_entity_deriver_delete (...)
#8  really_delete_pinned_closed_locked (...) at .../core/ddsc/src/dds_entity.c:545
#11 dds_delete (...)
#12 cyclonedds_rs::dds_participant::{impl}::drop (...)
```

あるスレッドで「そのドメインの最後の参加者」がdropされ、cycloneddsがドメインオブジェクト
（プロセス内でドメインIDごとに共有される内部状態）を `rtps_fini` で解体している最中に、
別スレッドが同じドメインIDで新たな参加者を作ろうとしてキー→サンプルのマップ
（tkmap, ハッシュテーブル）に同時アクセスし、解放済み/解放中の `serdata` を参照して落ちる。

重要な性質:

- クラッシュするのは **そのドメインの参加者refcountが1→0に落ちてドメインが解体される瞬間**
  だけ。refcountを常に1以上に保てば、この遷移そのものが起きない。
- Topic/Publisher/Subscriber/Reader/Writer はドメインのrefcountに関与しない子エンティティ
  なので、複数スレッドから自由に生成/破棄してよい（後述のストレステストで確認済み）。
- レースは同一プロセス内の話。`cargo test` は各テストバイナリを1つずつ実行するため、
  バイナリを跨いだドメインID重複は無関係。ただしテストバイナリ自体を並列実行する
  ランナー（cargo-nextest等）に切り替える場合は再評価が必要。
- `DdsDomain` を明示的に作って破棄する場合も同じ経路（`dds_domain_free` → `rtps_fini`）を
  通るため、同じ危険がある。

## 再現方法

参加者の生成/破棄だけを繰り返しても再現しない。**tkmapにserdataが載っている状態**
（＝実際にwriteされたデータがある）でないと `free_tkmap_instance` の経路に入らないため、
Topic/Writerを含む実データのやり取りが必要。

```rust
// tests/ に置いて単独バイナリとして実行する
const DOMAIN: u32 = 77;

#[derive(Debug, Clone, PartialEq, Topic, Serialize, Deserialize)]
struct StressTopic {
    #[topic_key]
    id: u32,
    value: String,
}

#[test]
fn stress_unsafe_create_drop() {
    let handles = (0..8).map(|t| std::thread::spawn(move || {
        for i in 0..100 {
            let p = unsafe { DdsParticipant::create(Some(DOMAIN), None, None) }.unwrap();
            let topic = StressTopic::create_topic(&p, None, None, None).unwrap();
            let pb = DdsPublisher::create(&p, None, None).unwrap();
            let sb = DdsSubscriber::create(&p, None, None).unwrap();
            let mut wr = DdsWriter::create(&pb, topic.clone(), None, None).unwrap();
            let re = DdsReader::create(&sb, topic, None, None).unwrap();
            wr.write(Arc::new(StressTopic { id: t, value: format!("{}", i) })).unwrap();
            let mut buf = SampleBuffer::new(4);
            let _ = re.take_now(&mut buf);
            drop(re); drop(wr); drop(sb); drop(pb); drop(p);
        }
    })).collect::<Vec<_>>();
    for h in handles { h.join().unwrap(); }
}
```

```sh
cargo build --tests --features derive
BIN=$(ls -t target/debug/deps/<test_name>-* | grep -v '\.d$' | head -1)
for i in $(seq 1 20); do timeout 180 $BIN >/dev/null 2>&1 || echo "run $i rc=$?"; done
```

実測: **20回中2回 SIGSEGV (rc=139)**。参加者を `get_or_create` に置き換えただけの
同一ワークロードでは **16回中0回**。

## APIの形

| API | safety | 挙動 |
|---|---|---|
| `DdsParticipant::create` / `DdsDomain::create` | `unsafe` | 所有権を持ち、dropでドメインから離脱/ドメインを解体する |
| `DdsParticipant::get_or_create(_with)` | safe | ドメインIDごとの共有インスタンス（`&'static`）を返す |
| `DdsDomain::get_or_create` | safe | 同上 |

`get_or_create` は `SharedRegistry`（`src/shared_registry.rs`）を使い、生成したエンティティを
`Box::leak` して登録簿に保持する。**プロセス終了までdropしない**ため、ドメインの参加者
refcountが0に落ちる瞬間そのものが無くなり、上記のレースに構造的に当たらない。
生成も登録簿のミューテックス内で行うので、「最初の1つ」を作る側のレースも同時に潰れる。

`DdsParticipant` の `domain=None`（`DDS_DOMAIN_DEFAULT`）は、実際のドメインIDが設定
（`CYCLONEDDS_URI`）依存で生成後にしか確定しない。生成後に `dds_get_domainid` で実IDを引き、
実IDのエントリとエイリアスを張ることで「デフォルト指定」と「実IDの明示指定」が
別インスタンスに分裂しないようにしている。

### 使い分け

`get_or_create` を既定とし、`unsafe create` は次の場合にだけ使う。

- 参加者/ドメインの**生成・破棄ライフサイクルそのものを検証する**テスト
  （`tests/raii.rs`、`dds_builtin::test_discovery_participant` の離脱検知など）
- エンティティごとに異なるQoS/Listenerが必要な場合
  （`get_or_create` はこれらを初回生成時にしか反映しない）

`unsafe create` を使う場合は、次のいずれかを呼び出し側で保証すること。

- 同じドメインIDに対する生成とdropがプロセス内で決して並行しない
- あるいは、そのドメインの参加者refcountが実行中に0へ落ちない
  （例: 別の参加者を常に生存させておく）

### トレードオフ

- 共有インスタンスはdropされないため、`dds_delete` によるドメイン離脱通知は
  プロセス終了まで送られない。**参加者の離脱を観測するテストには使えない**。
- 同じ参加者を複数箇所が共有するため、**トピック名の衝突**に注意する
  （同名・同型でもQoSが異なると `dds_create_topic` は失敗しうる）。
- **既に共有インスタンスがある状態で`maybe_qos`/`maybe_listener`（参加者）や
  既存と異なる`config`（ドメイン）を指定すると`Err(DDSError::PreconditionNotMet)`を
  返す**。黙って無視すると「指定したのに反映されていない」ことに気付けないため。
  `DdsDomain`は生成時の`config`を保持しており、`None`または同じ文字列を指定した
  場合のみ既存インスタンスをそのまま返す。
- `DdsDomain::get_or_create` は「そのドメインに初めて触る側」でなければ失敗する。
  cycloneddsの `dds_create_domain` は既にドメインが存在すると（参加者が暗黙的に
  作った場合も含む）`PreconditionNotMet` を返すため、参加者より先に呼ぶ必要がある。
  `DdsDomain`と`DdsParticipant`の登録簿は別ミューテックスで生成順序を保証しないため、
  設定付きドメインを使うテストは`common::tests::shared_participant_with_config`を通し、
  1つのロックで「ドメイン生成 → 参加者取得」の順序を保証すること。

## テスト用ドメインIDの割り当て

`src/common.rs` の `TestDomain`（`#[repr(u32)]`のenum）で一元管理し、`.id()`で`u32`に
変換して使う。同一テストバイナリ内で同じドメインIDを複数のテストが使い、かつ実体を
dropするとSEGVするため、`unsafe create` を使うテストには必ず固有のvariantを割り当てる。
`get_or_create` しか使わないテスト同士は同じvariantを共有してよい
（dropされないのでrefcountの0↔1遷移が起きない）。
IDの重複は判別子の重複としてコンパイルエラー（E0081）になるため、明示値は外部設定と
対応が必要なもの（`WriterLoan = 2`）だけに留め、残りは自動採番に任せる。

明示的な`config`が必要なテストは`std::env::set_var("CYCLONEDDS_URI", ...)`を使わず、
`common::tests::shared_participant_with_config(domain, config)`で
`DdsDomain::get_or_create(domain, Some(config))`を明示的に呼ぶこと。
`std::env::set_var`の並行呼び出しはRust自身が未定義動作であり、cycloneddsはドメインの
初回参加者生成時にこの値を読む窓を防げないため、libテストバイナリからは使わない
方針にしている（結合テストである`tests/raii.rs`は別プロセスなので対象外）。
