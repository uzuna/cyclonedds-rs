# cyclonedds-rs

[Eclipse Cyclone DDS](https://github.com/eclipse-cyclonedds/cyclonedds) の safe Rust バインディング。

IDLコンパイラもコード生成ステップも不要で、Rustの構造体に `#[derive(Topic)]` を付けるだけで
publish / subscribe を始められる。Cyclone の serialization インターフェース (serdata/sertype) を
Rust側で直接実装しているため、IDLから生成したC構造体を経由しない。

## 特徴

- **IDL不要**: `#[derive(Topic)]` でトピック型を定義する。キー・ネストしたキー・複数キーに対応
- **定義生成**: `#[derive(DdsInterface)]` から DDS IDL と ROS 2 `.msg`
  （mcap/rosbag2 が使う連結メッセージ定義を含む）を導出できる
- **非同期リーダー**: tokio上で `take_async` / `read_async` を待てる
- **QoS**: 生の `DdsQos` に加えて、通信可否に効く要素だけをまとめた `Policy` を提供
- **Listener**: クロージャでイベントコールバックを登録できる
- **untypedリーダー**: 型を知らないままCDRバイト列として購読できる（記録・中継用途）
- **builtinトピック**: 参加者・publication・subscription の生成/破棄を監視できる
- **共有メモリ転送**: iceoryx 経由（`shm` feature、デフォルト有効）

## 動作要件

Linuxのみ。ビルド前に以下をインストールしておくこと。

- **Cyclone DDS 11.0.0** — [11.0.0](https://github.com/eclipse-cyclonedds/cyclonedds/releases/tag/11.0.0)
  （検証済み: [1f7f75c](https://github.com/eclipse-cyclonedds/cyclonedds/commit/1f7f75c0fa7fc9070dafaea43e14924d2537b59e)）。
  PSMX/Iceoryx有効でビルドする: `cmake -DENABLE_ICEORYX=YES ..`
- **iceoryx 2.0.2** — [f756b7c](https://github.com/eclipse-iceoryx/iceoryx/commit/f756b7c99ddf714d05929374492b34c5c69355bb)。
  他のバージョンは使わないこと
- git / cmake / make / libclang / C・C++コンパイラ（cmakeとbindgenが使う）

Cyclone DDSの場所は `CYCLONEDDS_LIB_DIR` / `CYCLONEDDS_INCLUDE_DIR` で指定できる。
未指定の場合はworkspace内の `cyclonedds-sys` のbuild.rsが探索・取得する。

## セットアップ

```toml
[dependencies]
cyclonedds-rs = { git = "ssh://git@github.com/uzuna/cyclonedds-rs.git", features = ["derive"] }
```

| feature | デフォルト | 内容 |
| --- | --- | --- |
| `derive` | 無効 | `Topic` / `DdsInterface` deriveマクロを再エクスポートする。実質必須 |
| `shm` | 有効 | iceoryxによる共有メモリ転送を有効にする |

## 使い方

### 1. トピック型を定義する

`Topic` はDDSの通信に必要な実装（キーのCDRエンコード・keyhash・型名）を生成する。
`DdsInterface` は同じ構造体からIDL/`.msg` 定義を導出する（通信だけなら不要）。

```rust
use cyclonedds_rs::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Topic, DdsInterface)]
#[cdds(package = "my_robot_interfaces")]
pub struct RobotStatus {
    /// キーになるフィールド。値ごとに別インスタンスとして扱われる
    #[topic_key]
    pub robot_id: u32,
    pub battery_level: f64,
    pub position: Point,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Topic, DdsInterface)]
#[cdds(package = "geometry_msgs")]
pub struct Point {
    pub x: f64,
    pub y: f64,
}
```

### 2. Publish する

```rust
use std::sync::Arc;
use cyclonedds_rs::*;

let participant = DdsParticipant::get_or_create(Some(0))?;
let publisher = DdsPublisher::create(participant, None, None)?;

// トピック名を省略する `create_topic` では型のパスから `/my_crate/RobotStatus` のような名前になる
let topic = RobotStatus::create_topic_with_name(participant, "/robot_status", None, None)?;
let mut writer = DdsWriter::create(&publisher, topic, None, None)?;

writer.write(Arc::new(RobotStatus {
    robot_id: 1,
    battery_level: 0.87,
    position: Point { x: 1.0, y: 2.0 },
}))?;
```

### 3. Subscribe する（非同期）

`SampleBuffer` は複数サンプルをまとめて受け取るためのバッファで、
キーを持つトピックではキーの数だけサンプルが積まれる。

```rust
let subscriber = DdsSubscriber::create(participant, None, None)?;
let topic = RobotStatus::create_topic_with_name(participant, "/robot_status", None, None)?;
let reader = DdsReader::create_async(&subscriber, topic, None)?;

let mut samples = RobotStatus::create_sample_buffer(8);
loop {
    reader.take_async(&mut samples).await?;
    for s in samples.iter() {
        println!("{s:?}");
    }
    samples.clear();
}
```

### 4. QoS を設定する

通信が成立するかどうかに効くQoSは `Policy` にまとめてある。細かい設定が必要なら
`DdsQos` のセッターを直接使う。

```rust
// 後から起動したReaderにも直近10件を配信する
let qos = Policy {
    history: History::KeepLast(10),
    reliability: Reliability::Reliable(std::time::Duration::from_millis(100)),
    durability: Durability::TransientLocal {
        sync_depth: std::num::NonZeroU16::new(10).unwrap(),
    },
}
.to_qos()?;

let topic = RobotStatus::create_topic_with_name(participant, "/robot_status", Some(qos), None)?;
```

### 5. 対向が居ないときの挙動

画像や点群のように1件あたりの生成・シリアライズが重いトピックでは、聞き手(reader)が
居ない間の送信はCPUと帯域をそのまま捨てることになる。逆に受信側も、送り手(writer)が
居ないまま待ち続けるタスクを畳めた方がよい。`WriterBuilder::watch_reader_absence` /
`ReaderBuilder::stop_when_no_writer` は**構築コストの高いメッセージを事前にスキップする**
ための機能で、対向が0件になってから指定した猶予時間が経過すると不在を確定させる。

Writer側は判定材料を渡すだけで、送信を止めるかどうかの判断は呼び出し側に委ねられる。

```rust
use std::time::Duration;
use cyclonedds_rs::*;

let mut writer = WriterBuilder::new()
    .watch_reader_absence(Duration::from_secs(1))
    .create(&publisher, topic)?;

let mut interval = tokio::time::interval(Duration::from_millis(100));
loop {
    interval.tick().await;
    // 不在の間は重いメッセージ生成ごと飛ばす。復帰は次のtickで拾える
    if writer.is_reader_absent() {
        continue;
    }
    writer.write(build_expensive_message())?;
}
```

Reader側は `stop_when_no_writer` を指定すると、マッチするwriterが`timeout`の間1つも
居ない状態が続いた時点で `read_async` / `take_async` が `Err(ReaderError::NoMatchedWriter)`
を返すようになる。

```rust
let reader = ReaderBuilder::new()
    .stop_when_no_writer(Duration::from_secs(1))
    .create(&subscriber, topic)?;
```

猶予時間は不在方向にしか掛からない。対向が0件から1件以上に戻った瞬間、`timeout`を
待たず即座に復帰したものとして扱われる。

判定に使うのはQoS互換でマッチした対向の数なので、QoSが噛み合わない相手も不在として
扱われる(相手が起動していても検知されない)。

`liveliness_changed_status` はLIVELINESS QoS(`set_liveliness`で`MANUAL_BY_*`を使う場合)の
生存監視用のAPIで、不在検知の代替手段ではない。writerがそもそも居ない・discoveryごと
消えたことを知りたい場合は上記のオプションを使うこと。

### 6. IDL / ROS 2 `.msg` 定義を生成する

`#[derive(DdsInterface)]` を付けた型は、依存する型を辿って定義を組み立てられる。
ROS 2と相互運用する際のIDL登録や、mcap記録の埋め込みスキーマに使う。

```rust
println!("{}", RobotStatus::idl(NsMode::ros2dds())); // rosidl相当のIDL
println!("{}", RobotStatus::ros2_msg());             // 自身の .msg のみ
println!("{}", RobotStatus::full_ros2_msg());        // 依存を連結した mcap/rosbag2 形式
```

`cargo run --example dump_interfaces --features derive` の出力（抜粋）:

```
=== RobotStatus.msg ===
uint8 status_code
float64 battery_level
geometry_msgs/Point position
float32[] history
float64[16] matrix

=== RobotStatus full (mcap/rosbag2向け連結メッセージ定義) ===
uint8 status_code
float64 battery_level
geometry_msgs/Point position
float32[] history
float64[16] matrix
================================================================================
MSG: geometry_msgs/Point
float64 x
float64 y
```

`NsMode` で名前空間の埋め込み方を選ぶ。`NsMode::Raw` は `module pkg { struct T { ... }; };`、
`NsMode::ros2idl()` は `msg` を、`NsMode::ros2dds()` は `msg::dds_` と型名suffix `_` を挿入する。

## 属性リファレンス

`cdds` 属性は `Topic` と `DdsInterface` で共有する（型名の情報源を一本化するため）。

| 属性 | 対象 | 内容 |
| --- | --- | --- |
| `#[cdds(package = "...")]` | struct | パッケージ名。型名は `"{package}/{name}"` になる。`DdsInterface` では必須（REP 144準拠の小文字snake_case） |
| `#[cdds(name = "...")]` | struct | ワイヤ上の型名の上書き。省略時はRustのstruct名（UpperCamelCase必須） |
| `#[cdds(fixed_size)]` | struct | 固定長トピックであることを示す。型のサイズ制約を検査する |
| `#[topic_key]` | field | キーフィールド。プリミティブ・`[プリミティブ; N]`・`Topic` を derive した構造体が使える |
| `#[topic_key_enum]` | field | キーが列挙型であることを示す（プリミティブとして扱う） |

## Examples

すべて [cyclonedds/examples/](cyclonedds/examples/) にある。

| example | 実行 | 内容 |
| --- | --- | --- |
| [pubsub](cyclonedds/examples/pubsub/) | `cargo run --example pubsub -- pub any` / `cargo run --example pubsub -- sub untyped` | 送受信のCLI。可変長・固定長トピック、大きなペイロードのレイテンシ計測、untypedリーダー |
| [monitor](cyclonedds/examples/monitor.rs) | `cargo run --example monitor` | builtinトピックでpublicationの生成/破棄を監視する |
| [dump_interfaces](cyclonedds/examples/dump_interfaces.rs) | `cargo run --example dump_interfaces --features derive` | `DdsInterface` からIDL/`.msg` 定義をダンプする |

`pubsub` に `-s` を付けると PSMX/Iceoryx 共有メモリ転送の設定
([cyclonedds/testdata/cyclonedds_shm.xml](cyclonedds/testdata/cyclonedds_shm.xml)) を使う。別途 `iox-roudi` の起動が必要。

## ドキュメント

- [docs/domain-lifecycle.md](docs/domain-lifecycle.md) — `DdsParticipant::create` が `unsafe` な理由と、
  安全な代替である `get_or_create` の設計
- [docs/breaking-changes.md](docs/breaking-changes.md) — バージョンごとの破壊的変更と移行方法
- [docs/planned-improvements.md](docs/planned-improvements.md) — 対応予定の改善案件

## License

Apache License 2.0. [LICENSE](LICENSE) を参照。
