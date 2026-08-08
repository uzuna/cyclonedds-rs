# 対応予定の改善案件

着手前の改善案件を、理由と対処案つきで残す。実施したら該当節を削除し、
公開APIを壊す場合は [breaking-changes.md](breaking-changes.md) へ移す。

方針: **不正な状態はテストで検出するのではなく、型で表現不能にする。**
型で防げることをテストや手書きの防御コードで担保しているのは、型設計が足りていないサインとみなす。

---

## 1. ビルダーの排他オプションをenumにまとめる

対象: `ReaderBuilder` / `WriterBuilder` ([dds_reader.rs](../cyclonedds/src/dds_reader.rs) /
[dds_writer.rs](../cyclonedds/src/dds_writer.rs))

### 現状

排他的な選択を4つのフィールドで表し、優先順位を `create()` の `if / else if` で決めている。

```rust
pub struct ReaderBuilder<T: TopicType> {
    maybe_listener: Option<DdsListener>,
    maybe_listener_builder: Option<DdsListenerBuilder>,
    is_async: bool,
    absence_timeout: Option<Duration>,
    // ...
}
```

型としては「hook済みリスナーとビルダーの両方を指定」「非同期化なしで `absence_timeout` だけ指定」
といった無意味な組み合わせも作れてしまい、それを `create()` の分岐と
`test_builder_option_priority_selects_reader_kind` /
`test_builder_option_priority_enables_absence_watch` の表形式テストで押さえている。
**組み合わせテストの存在自体が型不足のサイン。**

### 対処案

```rust
enum ListenerSpec { None, Hooked(DdsListener), Builder(DdsListenerBuilder) } // 両方指定が表現不能になる
enum ReaderMode   { Sync, Async { absence_timeout: Option<Duration> } }      // asyncなしtimeoutが表現不能になる
```

この2軸を潰すと、表形式テストに残るのは「hook済みリスナー × 非同期化」という
**本質的に両立しない**組み合わせの1軸だけになる(hook済みの `DdsListener` は
コールバックを後から連鎖できないため)。テストはそこに絞る。

### 補足

挙動そのものは上記の表形式テストで固定済みなので、内部表現の変更で回帰が出れば捕まえられる。
公開APIの形は変わらない見込み。

---

## 2. 同期リーダーと非同期リーダーを型で分ける

対象: `DdsReader` / `ReaderType` / `ReaderError::ReaderNotAsync`

### 現状

同期リーダーに対して `read_async` / `take_async` を呼ぶと、**静的に分かる誤用が
実行時エラー**(`ReaderError::ReaderNotAsync`)として返る。

同じ根の問題として、`DdsReader::is_writer_absent()` は同期リーダーでは常に `false` を返す。
`stop_when_no_writer` が非同期化を強制するため「監視付きの同期リーダー」は作れず、
このメソッドは同期リーダーでは意味を持たない。現状は doc の注意書きで回避している。

### 対処案

`DdsReader` と非同期リーダーを別の型にする、または `ReaderBuilder` を typestate 化して
`create()` の戻り値型を分ける。非同期リーダーが独立した型になれば、
`read_async` / `is_writer_absent` はそこにしか生えず、doc で警告する必要が無くなる。

### 補足

公開APIの形が大きく変わるため**独立したPR**とする。
案件1の `ReaderMode` enum 化は、この分離への足がかりになる。

---

## 3. 所有権つきparticipantの生存を型で保証する

対象: `DdsParticipant` ([dds_participant.rs](../cyclonedds/src/dds_participant.rs))

### 現状

0.12.0 で publisher/subscriber/topic はdropでC側エンティティを削除するようになり、
子(reader/writer)は `Keepalive` で親を保持するため順序は型で守られている。
一方 **`unsafe DdsParticipant::create` で作った所有権つきparticipantだけは残っている**。
`DdsPublisher::create` などが `&DdsParticipant` の借用で受け取るうえ、
`DdsParticipant` は `Arc` を持たないので子に保持させられない。

participantを先にdropすると配下のエンティティがC側で一斉に削除され、その後に子の
Rust側 `Drop` が古いハンドル値で `dds_delete` を呼ぶ。cycloneddsのハンドルは擬似乱数で
採番され再利用されうる(`dds_handles.c`)ため、その値が別エンティティへ再割り当て済みなら
**無関係なエンティティを削除しうる**。

共有participant(`get_or_create`)はプロセス終了までdropされないので該当しない。

### 対処案

`DdsParticipant` を `Arc<ParticipantInner>` にして `keepalive()` を実装し、
publisher/subscriber/topic 側に保持させる。共有participantは今までどおり
`&'static` を返せるので、利用側のコードは変わらない見込み。

### 補足

`unsafe` な生成APIを使う場合にしか影響しないため、優先度は高くない。

---

## 4. テスト終了時のSEGVを追う

対象: テストのテアダウン全般([docs/domain-lifecycle.md](domain-lifecycle.md)の既知事象)

### 現状

CI(self-hosted ARM64)の `cargo test --workspace --all-features` が、全テストが `ok` を
出し切った**後のプロセス終了時**に `SIGSEGV` で落ちることがある。

```
test untyped::tests::test_untyped_transient_local ... ok
process didn't exit successfully: ... (signal: 11, SIGSEGV: invalid memory reference)
```

同一コミットの再実行では成功するため決定的な失敗ではない。手元(x86_64)では
lib テストを20回連続で回しても master・作業ブランチとも再現しない。
症状は`domain-lifecycle.md`に記録済みの「同一ドメインの参加者を並行して生成/破棄すると
cyclonedds本体が`rtps_fini`中の解放済みメモリを踏む」と一致する。

### 対処案

まず再現条件を絞る(どのテストバイナリで落ちるか、`--test-threads=1` で発生率が変わるか)。
そのうえでテアダウン順を決定的にする。共有participant/ドメインの意図的リークと、
テストごとの`unsafe create`が混在していることが疑わしい。

### 補足

同じ署名の失敗は0.12.0の変更途中(`bumpup`時点)でも観測されており、
エンティティのRAII化で持ち込んだものではない。ただしdropで`dds_delete`を呼ぶ
エンティティが増えたため、既知レースを踏む確率に影響していないかは未確認。
