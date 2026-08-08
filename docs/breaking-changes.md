# 破壊的変更

公開APIの互換性を壊す変更を、バージョンごとに記録する。
移行に必要な書き換えを載せることを目的とし、機能追加は対象としない。

## 0.14.0

### 1. `Durability::TransientLocal` が同期件数を持つようになった

```rust
// Before
Durability::TransientLocal

// After
Durability::TransientLocal { sync_depth: NonZeroU16 }
```

`Policy::to_qos()` が `DURABILITY_SERVICE` を設定していなかったため、
`Policy::create_transient_local(3, None)` としても**あとから参加したreaderには
直近1件しか届かなかった**。CycloneDDSでは同期件数を決めるのは `DURABILITY_SERVICE` で、
`HISTORY` はマッチ成立後の再送バッファにしか効かない。設定しないとDDS既定の
`KEEP_LAST(1)` が残る。詳細は [receiving-samples.md](receiving-samples.md) を参照。

`sync_depth` を `History` から導出しないのは、`History::KeepAll` が同期件数の
無制限(`tldepth = 0`)を意味してしまい、**全readerがackしてもwriter側の履歴が
解放されない**状態を作れるため。`NonZeroU16` で「0件」と「無制限」の両方を
構築不能にしている。

`Policy::create_transient_local(history, deadline)` のシグネチャは変わらない
(`history` から `sync_depth` を導出する)。ただし `NonZeroU16` に収まらない値は
`DDSError::BadParameter` になる。`Durability` を直接構築している箇所は書き換えが必要。

### 2. `DdsQos::durability_service()` を追加した

```rust
pub fn durability_service(&self) -> Option<(dds_history_kind, i32)>
```

未設定時に `None` を返す。DDS既定の `KEEP_LAST(1)` で代替しないのは、
「未設定」と「明示的に `KEEP_LAST(1)` を設定した」を呼び出し側が区別できなくなるため。
0.13.0 で `Option` 化した他のgetterと規約を揃えている。

なお `Policy::from(&DdsQos)` は未設定を `sync_depth: 1`(DDS既定と同義)、
`KEEP_ALL` と `NonZeroU16` 超過を `NonZeroU16::MAX` に丸める。`KEEP_LAST`で
`depth <= 0`(他ベンダ/不正なdiscoveryデータでのみ起こりうる)は「無制限」ではなく
不正値のため、`NonZeroU16::MAX` ではなく未設定と同じ `1` に丸める。

### 3. `to_qos()` が `DURABILITY_SERVICE` を設定するようになったことによる共有メモリへの影響

上記1の修正で `Policy::create_transient_local(17)` のように iceoryx の既定深さ上限
(16件、`vendor/iceoryx/iceoryx_posh/cmake/IceoryxPoshDeployment.cmake:56`)を超える
`sync_depth` を渡すと、`to_qos()` が `DURABILITY_SERVICE` に同じ深さを設定する。

0.13.0までは `DURABILITY_SERVICE` が未設定のままだったため、cyclonedds側の
`dds_writer_supports_shm`(`vendor/cyclonedds/src/core/ddsc/src/dds_writer.c:332-337`)
が見る深さは常にDDS既定の `KEEP_LAST(1)` で、iceoryxの上限チェックには絶対に
引っかからなかった。0.14.0では**コードを変更していない既存のwriterでも**、
この上限を超える設定であれば共有メモリのゼロコピー経路を失う(通信自体は
ネットワーク経由にフォールバックするため失敗はしない)。詳細は
[receiving-samples.md](receiving-samples.md) を参照。

## 0.13.0

### 1. `DdsQos` のQoS取得系が `Option` を返すようになった

```rust
// Before
fn durability(&self) -> dds_durability_kind
fn history(&self) -> (dds_history_kind, i32)
fn reliability(&self) -> (dds_reliability_kind, Duration)
fn lifespan(&self) -> Duration
fn deadline(&self) -> Duration
fn liveliness(&self) -> (dds_liveliness_kind, Duration)

// After: いずれも Option<...> でくるまれる
fn durability(&self) -> Option<dds_durability_kind>
```

cycloneddsの `dds_qget_*` は、該当policyが未設定なら**出力先に一切書かずに `false` を返す**。
戻り値を検査せず `MaybeUninit::assume_init` していたため、未設定のQoSに対する呼び出しが
未初期化メモリの読み出しになっていた。`dds_*_kind` はbindgenの `rustified_enum` 指定で
真のRust enumなので、不正なdiscriminantの生成という即時UBになる。
`DdsQos` の `Debug` は6つのgetterをすべて呼ぶため、`DdsQos::create()` 直後の
デバッグ出力だけでも踏める経路だった。

設定済みかどうかを型で表せるよう、既存の `userdata()` に揃えて `Option` を返す。
未設定を既定値として扱ってよい場合は `unwrap_or_default()` で従来相当になる。

なお `Policy::from(&DdsQos)` は未設定policyを各型の `Default` に落とすため、
`Policy` 経由で使っている箇所に書き換えは不要。

## 0.12.0

エンティティのC側の寿命をRustの所有権に合わせる変更。

### 1. `DdsListener` から `Clone` を削除した

`Clone` を導出しつつ `Drop` で `dds_delete_listener` していたため、cloneすると**同じ
リスナーが二重に削除されていた**。また、同じリスナーを複数エンティティへ明示的に
登録すると、コールバックの排他がエンティティ単位のため同一のコールバック実体が
同時に呼ばれうる状態だった。

複数のエンティティで同じ処理をしたい場合は、エンティティごとにリスナーを作ること。

```rust
// Before
let listener = DdsListenerBuilder::new().on_data_available(handler).build();
let reader1 = ReaderBuilder::new().with_listener(listener.clone()).create(&sub, topic.clone())?;
let reader2 = ReaderBuilder::new().with_listener(listener).create(&sub, topic)?;

// After
let reader1 = ReaderBuilder::new()
    .with_listener(DdsListenerBuilder::new().on_data_available(handler1).build())
    .create(&sub, topic.clone())?;
let reader2 = ReaderBuilder::new()
    .with_listener(DdsListenerBuilder::new().on_data_available(handler2).build())
    .create(&sub, topic)?;
```

**注意**: cycloneddsは`dds_entity_init`時に親エンティティ(participant/subscriber/
publisher)のリスナーを関数ポインタと`arg`ごと子エンティティへコピーする
(`dds_inherit_listener`)ため、**親に1つ付けたリスナーは配下の子エンティティごとに
並行して呼ばれうる**。これは`Clone`の有無とは無関係に起きるので、下記3で
コールバック自体を並行呼び出しに耐える型にしている。

### 2. publisher / subscriber / topic がdropでエンティティを削除するようになった

これまではRust側の値が消えてもC側エンティティが残っていた。リスナーを渡していた場合は
コールバックの実体だけが解放され、生き残ったエンティティから解放済みメモリを
参照しうる状態だった。

cycloneddsは親エンティティを削除すると子も再帰的に削除するが、**子は生成に使った親の
ハンドルを保持する**ので、利用者が親を先に手放しても子は動き続ける。

- reader / writer は生成元の publisher / subscriber と topic を保持する
- そのため「親を生かすためだけに保持していたフィールド」は不要になる

```rust
// Before (topicやpublisherを生かすために保持する必要があった)
struct Sub<T> { subscriber: DdsSubscriber, reader: DdsReader<T>, topic: DdsTopic<T> }

// After
struct Sub<T> { reader: DdsReader<T> }
```

**残る注意**: `unsafe DdsParticipant::create` で作った所有権つきparticipantだけは子が
保持できないため、これを先にdropすると配下のエンティティが一緒に消える。
`DdsParticipant::get_or_create` の共有participantはプロセス終了までdropされないので該当しない。

なお子が親を保持するための型として `Keepalive` を公開し、`DdsWritable` / `DdsReadable` に
デフォルト実装つきの `keepalive()` を追加した。独自実装がある場合、dropでエンティティを
削除する親であれば `keepalive()` を実装すること(既定は「保持しない」)。

### 3. コールバックが `Fn + Send + Sync` になった

`DdsListenerBuilder` の `on_*` / `chain_*`、および非推奨の `DdsListener::on_*`
(13メソッドすべて)に渡すクロージャは `FnMut` ではなく `Fn + Send + Sync` を要求する。

- `Send`: トランポリンはcycloneddsの内部スレッドから呼ばれるため、`Rc` のように
  スレッドをまたげない値をキャプチャしたクロージャは登録できない
- `Fn + Sync`: 上記1の注意のとおり、同じコールバック実体が複数エンティティから
  並行に呼ばれうる。`FnMut` だと内部で `&mut` のエイリアスが同時に生きてしまう

可変な状態を持つクロージャは、キャプチャを内部可変性へ置き換えること。`Mutex` も使えるが、
ローカル配送は write 側のスレッドで同期実行されるため、ロックを保持したままコールバック内から
`write` すると同じクロージャが同一スレッドで再入して自己デッドロックしうる。

```rust
// Before
let mut count = 0;
builder.on_data_available(move |_entity| {
    count += 1;
});

// After
let count = Arc::new(AtomicUsize::new(0));
builder.on_data_available({
    let count = count.clone();
    move |_entity| {
        count.fetch_add(1, Ordering::Relaxed);
    }
});
```

### 補足: `chain_*` を全イベントで公開した

1 の移行で「同じリスナーを使い回す」代わりに、ビルダー上でコールバックを合成できる。
`on_*` は上書き、`chain_*` は連鎖(渡した方が先に呼ばれる)で、13イベントすべてに揃えた。

## 0.11.0

対向エンティティ(writerにとってのreader、readerにとってのwriter)の不在検知オプションの
追加にともなう変更。3件とも既存コードの書き換えが必要になる。

### 1. `DdsWriter` から `Clone` を削除した

`Clone` を導出しつつ `Drop` で `dds_delete` していたため、cloneすると**同じエンティティ
ハンドルが二重に削除されていた**。`AlreadyDeleted` は握り潰されるので表面化しにくいが、
ハンドルが再利用されていれば無関係なエンティティを消しうる。

複数箇所で共有する場合は呼び出し側で包むこと。`write` が `&mut self` を取るため
`Arc` だけでは書き込めない。

```rust
// Before
let w2 = writer.clone();

// After
let writer = Arc::new(Mutex::new(writer));
let w2 = Arc::clone(&writer);
```

### 2. `ReaderError` に `NoMatchedWriter` を追加した

```rust
ReaderError::NoMatchedWriter { timeout: Duration }
```

`ReaderError` を**網羅的に `match` している**コードはコンパイルエラーになる。
`ReaderBuilder::stop_when_no_writer` を指定していなければ発生しないため、
使わない場合は `_ => {}` 等で受けてよい。

`timeout` には設定値が入る(実際の経過時間ではない)。経過時間だと呼び出しのたびに
値が変わり、構造体比較にも分岐にも使えないため。

`#[non_exhaustive]` は付けていない。付けると既存のvariantに対する網羅的な `match` も
同時に壊れるため、今回の追加に対しては代償が大きい。

### 3. `DdsListenerBuilder` を値渡しビルダーにした

`build` が `&mut self` ではなく `self` を取り、`on_*` / `chain_*` も
`&mut self -> &mut Self` から `mut self -> Self` になった。

```rust
// Before
let mut builder = DdsListenerBuilder::new();
builder.on_data_available(|_| {});
let listener = builder.build();

// After
let listener = DdsListenerBuilder::new()
    .on_data_available(|_| {})
    .build();
```

内部フィールドが `Option<DdsListener>` だったため、`build()` 後のビルダーを再利用すると
**全メソッドがパニックする**状態を作れてしまっていた。また導出 `Default` が
`listener: None` という不正な状態を作るため、手書きの `Default` で防ぐ必要があった。

所有権で「値を奪う」ことを表すと、この2つはどちらも**型として表現不能**になる。
不正な状態を実行時に検出するのではなく作らせない、という方針にもとづく変更。

---

## 参照

- 今後予定している型設計の改善は [planned-improvements.md](planned-improvements.md) を参照
