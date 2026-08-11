# 受信が「足りない」ときに疑うところ

「n件書いたのにn件受け取れない」は運用でよく踏む。原因は2つあり、どちらもDDSの仕様どおりの
挙動なので、待ち方とQoSの両方を押さえる必要がある。

言語バインディングに依らない話なので、Rust以外から使う場合も同じ。実行できる形は
[tests/practical.rs](../cyclonedds/tests/practical.rs) にある。

---

## 1. 1回の読み出しで全件揃うとは限らない

### 現象

writerが3件連続で書いたのに、readerの読み出しが1件で返ってくる。もう一度読むと残りが取れる。

### 原因

DDSの「データが読める」通知は**到着ごと**に立つ。読み出しAPIはその時点でreaderのキャッシュに
入っている分を返して完了するので、送信側が何件書いたかとは無関係に、
**通知が立った瞬間に届いていた分**しか得られない。バッファの容量を大きくしても変わらない。

### 対処

**前提**: readerの`HISTORY`深さが必要件数以上であること。既定は`KEEP_LAST(1)`で、
深さが足りないとRHC(reader側キャッシュ)で古いサンプルが上書きされるため、
ループをいくら回しても揃わない。

必要な件数が揃うまで読み出しを繰り返し、**呼び出し側で上限時間を掛ける**。

```rust
let mut ids = Vec::new();
let mut buf = SampleBuffer::<Seq>::new(want);
let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

while ids.len() < want {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        break;
    }
    match tokio::time::timeout(remaining, reader.take_async(&mut buf)).await {
        Ok(Ok(_)) => ids.extend(buf.iter().map(|s| s.id)),
        Ok(Err(_)) | Err(_) => break,
    }
}
```

上限は「正常時の待ち時間」ではなく「来なければ失敗と判断する線」なので、正常時(数ms〜数十ms)を
大きく上回る値を置く。固定の `sleep` で代用すると、実行環境の速度が変わった時に
足りなくなったり無駄に遅くなったりする。

---

## 2. TransientLocalで、あとから参加したreaderに履歴が届かない

### 現象

TransientLocalかつ履歴3件の設定にしたのに、writerが3件書いたあとに起動したreaderには
**1件しか届かない**。

### 原因

CycloneDDSでは2つのQoSが別の役目を持つ。

| QoS | 役目 |
| --- | --- |
| `HISTORY` | マッチが成立した**あと**の再送用バッファの深さ |
| `DURABILITY_SERVICE` | 接続確立時に、**あとから参加したreaderへ同期する**件数 |

`HISTORY` をいくら深くしても late joiner には効かない。`DURABILITY_SERVICE` を設定しなければ
DDS既定の KEEP_LAST(1) が残るため、届くのは常に直近1件になる。

一般的なDDSの解説で `HISTORY` に期待される機能を、CycloneDDSは `DURABILITY_SERVICE` 側で
担っている。仕様差なので、他実装からの移植時にも踏みやすい。

Reference: <https://github.com/eclipse-cyclonedds/cyclonedds/issues/49>

### 対処

late joinerへ届けたい件数を `DURABILITY_SERVICE` にも設定する。

```rust
// Policy経由なら、同期件数は`Durability::TransientLocal`が持つ(0.14.0以降)。
// `create_transient_local`はhistoryとsync_depthの両方を埋める
let qos = Policy::create_transient_local(3, None)?.to_qos()?;

// 「マッチ後は全数保証、late joinerには直近3件だけ」のようにずらす場合
let qos = Policy {
    history: History::KeepAll,
    reliability: Reliability::Reliable(Duration::from_millis(100)),
    durability: Durability::TransientLocal {
        sync_depth: NonZeroU16::new(3).unwrap(),
    },
}
.to_qos()?;

// DdsQosを直に組む場合は、両方を明示する必要がある
let mut qos = DdsQos::create()?;
qos.set_durability(dds_durability_kind::DDS_DURABILITY_TRANSIENT_LOCAL);
qos.set_history(dds_history_kind::DDS_HISTORY_KEEP_LAST, 3)?;
qos.set_durability_service(
    Duration::ZERO,
    dds_history_kind::DDS_HISTORY_KEEP_LAST,
    3,
    -1,
    -1,
    -1,
)?;
```

件数nの受信そのものに実用上の上限はない。`Policy::create_transient_local(n, None)` の n は
[tests/practical.rs](../cyclonedds/tests/practical.rs) で 1〜99 を、手元では 5000 まで
「直近n件が順序どおり届く」ことを確認している。

Cyclone DDS 11では PSMX/Iceoryx の共有メモリ経路を使う。cyclonedds-rs は
XCDR1のシリアライズ済みデータを転送するため、旧実装固有の16件境界は適用されない。
実際に保持できる件数は、利用するIceoryx/PSMXの設定と必要メモリ量で決まる。

---

## 避けるべき状態: マッチ成立の途中で書く

writerが書いている最中にreaderが参加すると、そのreaderは「`DURABILITY_SERVICE` で同期された
履歴」と「参加後に流れてきたライブデータ」の両方を受け取る。**何件届くかが参加した瞬間で
変わる**ため、受信件数から設定の正しさを判断できなくなる。

「たいてい1件だが、たまに3件届く」という不安定な観測はこれが原因のことが多い。
先に `DURABILITY_SERVICE` を正しく設定して件数を決定的にすること。
タイミングに依存した受信件数を仕様として当てにしてはいけない。

---

## 関連

- [tests/practical.rs](../cyclonedds/tests/practical.rs) - 上記を表形式で固定した統合テスト。
  Durability・History深さ・writeタイミングごとの受信列と、QoS対パターンごとの
  マッチ可否・非互換となったQoS種別が読める
- [domain-lifecycle.md](domain-lifecycle.md) - ドメインの寿命とスレッド安全性
- [breaking-changes.md](breaking-changes.md) - 0.14.0 の `Policy::to_qos()` の変更
