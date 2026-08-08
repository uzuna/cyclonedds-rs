# 破壊的変更

公開APIの互換性を壊す変更を、バージョンごとに記録する。
移行に必要な書き換えを載せることを目的とし、機能追加は対象としない。

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
