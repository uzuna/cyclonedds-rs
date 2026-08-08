/*
    Copyright 2020 Sojan James

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

//! A Listner can be attached to different types of entities. The callbacks that
//! are supported depends on the type of entity. There is no checking for whether
//! an entity supports the callback.
//! # Example
//! ```
//! use cyclonedds_rs::DdsListener;
//! let listener = DdsListener::new()
//!   .on_subscription_matched(|a,b| {
//!     println!("Subscription matched!");
//! }).on_publication_matched(|a,b|{
//!     println!("Publication matched");
//! }).
//! hook(); // The hook call will finalize the listener. No more callbacks can be attached after this.
//! ```

use cyclonedds_sys::dds_listener_t;
use cyclonedds_sys::*;
use std::convert::From;

/*
 Each listener has its own set of callbacks.
*/

/// The callbacks are in a different structure that is always
/// heap allocated.
///
/// クロージャを`FnMut`ではなく`Fn + Send + Sync`にしているのは、同じ`Callbacks`が
/// 複数のエンティティから並行に呼ばれうるため。cycloneddsは`dds_entity_init`で親の
/// リスナーを関数ポインタと`arg`ごと子へコピーし(`dds_inherit_listener`)、
/// コールバックの排他はエンティティ単位(`m_cb_count`)なので、親に1つ付けたリスナーは
/// 配下の子エンティティごとに別スレッドから同時に走る。`FnMut`だとトランポリンが
/// `&mut Callbacks`を作ることになり、この状況でエイリアスが同時に生きてしまう。
///
/// 状態を持ちたい場合は`Atomic`をキャプチャすること。`Mutex`も使えるが、ローカル配送は
/// write側のスレッドで同期実行されるため、ロックを保持したままコールバック内から
/// `write`すると同じクロージャが同一スレッドで再入して自己デッドロックしうる
#[derive(Default)]
struct Callbacks {
    // Callbacks for readers
    on_sample_lost:
        Option<Box<dyn Fn(DdsEntity, dds_sample_lost_status_t) + Send + Sync + 'static>>,
    on_data_available: Option<Box<dyn Fn(DdsEntity) + Send + Sync + 'static>>,
    on_sample_rejected:
        Option<Box<dyn Fn(DdsEntity, dds_sample_rejected_status_t) + Send + Sync + 'static>>,
    on_liveliness_changed:
        Option<Box<dyn Fn(DdsEntity, dds_liveliness_changed_status_t) + Send + Sync + 'static>>,
    on_requested_deadline_missed: Option<
        Box<dyn Fn(DdsEntity, dds_requested_deadline_missed_status_t) + Send + Sync + 'static>,
    >,
    on_requested_incompatible_qos: Option<
        Box<dyn Fn(DdsEntity, dds_requested_incompatible_qos_status_t) + Send + Sync + 'static>,
    >,
    on_subscription_matched:
        Option<Box<dyn Fn(DdsEntity, dds_subscription_matched_status_t) + Send + Sync + 'static>>,

    //callbacks for writers
    on_liveliness_lost:
        Option<Box<dyn Fn(DdsEntity, dds_liveliness_lost_status_t) + Send + Sync + 'static>>,
    on_offered_deadline_missed: Option<
        Box<dyn Fn(DdsEntity, dds_offered_deadline_missed_status_t) + Send + Sync + 'static>,
    >,
    on_offered_incompatible_qos: Option<
        Box<dyn Fn(DdsEntity, dds_offered_incompatible_qos_status_t) + Send + Sync + 'static>,
    >,
    on_publication_matched:
        Option<Box<dyn Fn(DdsEntity, dds_publication_matched_status_t) + Send + Sync + 'static>>,

    on_inconsistent_topic:
        Option<Box<dyn Fn(DdsEntity, dds_inconsistent_topic_status_t) + Send + Sync + 'static>>,
    on_data_on_readers: Option<Box<dyn Fn(DdsEntity) + Send + Sync + 'static>>,
}

// トランポリンは並行に呼ばれうる`&Callbacks`を作るため、`Sync`でないフィールドが
// 1つでも混ざると即UBになる。追加時にコンパイルエラーで気付けるよう固定する
const _: fn() = || {
    fn assert_sync<T: Sync>() {}
    assert_sync::<Callbacks>();
};

// SAFETY: Innerが持つ生ポインタはCへ渡したlistenerとCallbacksだけで、
// Callbacks側のクロージャは`Send + Sync`を要求しているため、
// スレッドをまたいでも、複数スレッドから同時に呼ばれても安全
unsafe impl Send for Inner {}
struct Inner {
    listener: Option<*mut dds_listener_t>,
    callbacks: Option<Box<Callbacks>>,
    raw_ptr: Option<*mut Callbacks>,
}

/// `Clone`は導出しない。cloneで利用者が同じリスナーを複数エンティティへ明示的に
/// 登録できてしまう経路と、[`Drop`]がclone毎に走って`dds_delete_listener`が
/// 二重に呼ばれる経路を塞ぐため。複数エンティティで同じ処理をしたい場合は
/// エンティティごとにリスナーを作ること。
///
/// なお`Clone`の有無に関わらず、cycloneddsは親エンティティのリスナーを子へコピーする
/// (`dds_inherit_listener`)ため、1つのリスナーが複数エンティティから並行に呼ばれること自体は
/// 起こりうる。それが安全なのは登録するクロージャが`Fn + Send + Sync`だからで、
/// `Clone`廃止が担っているのは二重解放と、利用者が意図せず状態を共有することの防止
pub struct DdsListener {
    inner: std::sync::Arc<std::sync::Mutex<Inner>>,
}

impl DdsListener {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(std::sync::Mutex::new(Inner {
                listener: None,
                callbacks: Some(Box::default()),
                raw_ptr: None,
            })),
        }
    }
}

impl Default for DdsListener {
    fn default() -> Self {
        DdsListener::new()
    }
}

impl From<&DdsListener> for *const dds_listener_t {
    fn from(listener: &DdsListener) -> Self {
        if let Some(listener) = listener.inner.lock().unwrap().listener {
            listener
        } else {
            panic!("Attempt to convert from unitialized &listener");
        }
    }
}

impl DdsListener {
    // take ownership as we're going to do some bad stuff here.
    pub fn hook(self) -> Self {
        // we're going to grab the Boxed callbacks and keep them separately as
        // we will send a pointer to the callback array into C. We convert the
        // pointer back to a box in the Drop function.
        let Some(callbacks) = self.inner.lock().unwrap().callbacks.take() else {
            // 既にhook済み。CはこのCallbacksを`arg`として保持し続けているので、
            // ここで解放するとコールバックが解放済みメモリを参照することになる
            return self;
        };

        let raw = Box::into_raw(callbacks);
        // SAFETY: rawはBox::into_rawで得た有効なポインタ。回収はDropで行う
        unsafe {
            let l = dds_create_listener(raw as *mut std::ffi::c_void);
            if l.is_null() {
                // Cへ渡す前に失敗したので、所有権を戻して解放する
                drop(Box::from_raw(raw));
                panic!("Error creating listener");
            }
            self.register_callbacks(l, &*raw);

            let mut inner = self.inner.lock().unwrap();
            inner.raw_ptr = Some(raw);
            inner.listener = Some(l);
        }
        self
    }

    /// register the callbacks for the closures that have been set.DdsListener
    unsafe fn register_callbacks(&self, listener: *mut dds_listener_t, callbacks: &Callbacks) {
        // 各コールバックの登録有無の判定は安全なので、`unsafe`はdds_lset_*呼び出しのみに絞る
        macro_rules! lset {
            ($field:ident => $setter:ident, $closure:ident) => {
                if callbacks.$field.is_some() {
                    // SAFETY: listenerはdds_create_listenerで生成された有効なポインタであり、
                    // $closureはこのlistenerに紐づくCallbacksのみを参照するトランポリン
                    unsafe {
                        $setter(listener, Some(Self::$closure));
                    }
                }
            };
        }

        lset!(on_data_available => dds_lset_data_available, call_data_available_closure);
        lset!(on_sample_lost => dds_lset_sample_lost, call_sample_lost_closure);
        lset!(on_sample_rejected => dds_lset_sample_rejected, call_sample_rejected_closure);
        lset!(on_liveliness_changed => dds_lset_liveliness_changed, call_liveliness_changed_closure);
        lset!(on_requested_deadline_missed => dds_lset_requested_deadline_missed, call_requested_deadline_missed_closure);
        lset!(on_requested_incompatible_qos => dds_lset_requested_incompatible_qos, call_requested_incompatible_qos_closure);
        lset!(on_subscription_matched => dds_lset_subscription_matched, call_subscription_matched_closure);
        lset!(on_liveliness_lost => dds_lset_liveliness_lost, call_liveliness_lost_closure);
        lset!(on_offered_deadline_missed => dds_lset_offered_deadline_missed, call_offered_deadline_missed_closure);
        lset!(on_offered_incompatible_qos => dds_lset_offered_incompatible_qos, call_offered_incompatible_qos_closure);
        lset!(on_publication_matched => dds_lset_publication_matched, call_publication_matched_closure);
        lset!(on_inconsistent_topic => dds_lset_inconsistent_topic, call_inconsistent_topic_closure);
        lset!(on_data_on_readers => dds_lset_data_on_readers, call_data_on_readers_closure);
    }
}

//////
impl DdsListener {
    #[deprecated]
    pub fn on_data_available<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.inner.lock().unwrap().callbacks {
            callbacks.on_data_available = Some(Box::new(callback));
        }

        self
    }

    unsafe extern "C" fn call_data_available_closure(
        reader: dds_entity_t,
        data: *mut std::ffi::c_void,
    ) {
        // SAFETY: cyclonedds がリスナー登録時に渡した Callbacks へのポインタであり、
        // リスナーの生存期間中は有効であることが呼び出し側で保証される。
        // 継承により複数エンティティから並行に呼ばれうるので共有参照だけを作る
        unsafe {
            let callbacks_ptr = data as *const Callbacks;
            let callbacks = &*callbacks_ptr;
            //        println!("C Callback!");
            if let Some(avail) = &callbacks.on_data_available {
                avail(DdsEntity::new(reader));
            }
        }
    }
}

impl DdsListener {
    /////
    #[deprecated]
    pub fn on_sample_lost<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_sample_lost_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.inner.lock().unwrap().callbacks {
            callbacks.on_sample_lost = Some(Box::new(callback));
        }
        self
    }

    unsafe extern "C" fn call_sample_lost_closure(
        reader: dds_entity_t,
        status: dds_sample_lost_status_t,
        data: *mut std::ffi::c_void,
    ) {
        // SAFETY: cyclonedds がリスナー登録時に渡した Callbacks へのポインタであり、
        // リスナーの生存期間中は有効であることが呼び出し側で保証される。
        // 継承により複数エンティティから並行に呼ばれうるので共有参照だけを作る
        unsafe {
            let callbacks_ptr = data as *const Callbacks;
            let callbacks = &*callbacks_ptr;
            //println!("C Callback - sample lost");
            if let Some(lost) = &callbacks.on_sample_lost {
                lost(DdsEntity::new(reader), status);
            }
        }
    }
}

impl DdsListener {
    //////
    #[deprecated]
    pub fn on_sample_rejected<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_sample_rejected_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.inner.lock().unwrap().callbacks {
            callbacks.on_sample_rejected = Some(Box::new(callback));
        }
        self
    }

    unsafe extern "C" fn call_sample_rejected_closure(
        reader: dds_entity_t,
        status: dds_sample_rejected_status_t,
        data: *mut std::ffi::c_void,
    ) {
        // SAFETY: cyclonedds がリスナー登録時に渡した Callbacks へのポインタであり、
        // リスナーの生存期間中は有効であることが呼び出し側で保証される。
        // 継承により複数エンティティから並行に呼ばれうるので共有参照だけを作る
        unsafe {
            let callbacks_ptr = data as *const Callbacks;
            let callbacks = &*callbacks_ptr;
            //println!("C Callback - sample rejected");
            if let Some(rejected) = &callbacks.on_sample_rejected {
                rejected(DdsEntity::new(reader), status);
            }
        }
    }
}

// Liveliness changed
impl DdsListener {
    #[deprecated]
    pub fn on_liveliness_changed<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_liveliness_changed_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.inner.lock().unwrap().callbacks {
            callbacks.on_liveliness_changed = Some(Box::new(callback));
        }
        self
    }

    unsafe extern "C" fn call_liveliness_changed_closure(
        entity: dds_entity_t,
        status: dds_liveliness_changed_status_t,
        data: *mut std::ffi::c_void,
    ) {
        // SAFETY: cyclonedds がリスナー登録時に渡した Callbacks へのポインタであり、
        // リスナーの生存期間中は有効であることが呼び出し側で保証される。
        // 継承により複数エンティティから並行に呼ばれうるので共有参照だけを作る
        unsafe {
            let callbacks_ptr = data as *const Callbacks;
            let callbacks = &*callbacks_ptr;
            //println!("C Callback - Liveliness changed");
            if let Some(changed) = &callbacks.on_liveliness_changed {
                changed(DdsEntity::new(entity), status);
            }
        }
    }
}

impl DdsListener {
    #[deprecated]
    pub fn on_requested_deadline_missed<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_requested_deadline_missed_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.inner.lock().unwrap().callbacks {
            callbacks.on_requested_deadline_missed = Some(Box::new(callback));
        }
        self
    }

    unsafe extern "C" fn call_requested_deadline_missed_closure(
        entity: dds_entity_t,
        status: dds_requested_deadline_missed_status_t,
        data: *mut std::ffi::c_void,
    ) {
        // SAFETY: cyclonedds がリスナー登録時に渡した Callbacks へのポインタであり、
        // リスナーの生存期間中は有効であることが呼び出し側で保証される。
        // 継承により複数エンティティから並行に呼ばれうるので共有参照だけを作る
        unsafe {
            let callbacks_ptr = data as *const Callbacks;
            let callbacks = &*callbacks_ptr;
            //println!("C Callback - requested deadline missed");
            if let Some(missed) = &callbacks.on_requested_deadline_missed {
                missed(DdsEntity::new(entity), status);
            }
        }
    }
}

impl DdsListener {
    #[deprecated]
    pub fn on_requested_incompatible_qos<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_requested_incompatible_qos_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.inner.lock().unwrap().callbacks {
            callbacks.on_requested_incompatible_qos = Some(Box::new(callback));
        }
        self
    }

    unsafe extern "C" fn call_requested_incompatible_qos_closure(
        entity: dds_entity_t,
        status: dds_requested_incompatible_qos_status_t,
        data: *mut std::ffi::c_void,
    ) {
        // SAFETY: cyclonedds がリスナー登録時に渡した Callbacks へのポインタであり、
        // リスナーの生存期間中は有効であることが呼び出し側で保証される。
        // 継承により複数エンティティから並行に呼ばれうるので共有参照だけを作る
        unsafe {
            let callbacks_ptr = data as *const Callbacks;
            let callbacks = &*callbacks_ptr;
            //println!("C Callback - requested incompatible QOS");
            if let Some(incompatible_qos) = &callbacks.on_requested_incompatible_qos {
                incompatible_qos(DdsEntity::new(entity), status);
            }
        }
    }
}

impl DdsListener {
    #[deprecated]
    pub fn on_subscription_matched<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_subscription_matched_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.inner.lock().unwrap().callbacks {
            callbacks.on_subscription_matched = Some(Box::new(callback));
        }
        self
    }

    unsafe extern "C" fn call_subscription_matched_closure(
        entity: dds_entity_t,
        status: dds_subscription_matched_status_t,
        data: *mut std::ffi::c_void,
    ) {
        // SAFETY: cyclonedds がリスナー登録時に渡した Callbacks へのポインタであり、
        // リスナーの生存期間中は有効であることが呼び出し側で保証される。
        // 継承により複数エンティティから並行に呼ばれうるので共有参照だけを作る
        unsafe {
            let callbacks_ptr = data as *const Callbacks;
            let callbacks = &*callbacks_ptr;
            //println!("C Callback - subscription matched");
            if let Some(matched) = &callbacks.on_subscription_matched {
                matched(DdsEntity::new(entity), status);
            }
        }
    }
}

impl DdsListener {
    #[deprecated]
    pub fn on_liveliness_lost<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_liveliness_lost_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.inner.lock().unwrap().callbacks {
            callbacks.on_liveliness_lost = Some(Box::new(callback));
        }
        self
    }

    unsafe extern "C" fn call_liveliness_lost_closure(
        entity: dds_entity_t,
        status: dds_liveliness_lost_status_t,
        data: *mut std::ffi::c_void,
    ) {
        // SAFETY: cyclonedds がリスナー登録時に渡した Callbacks へのポインタであり、
        // リスナーの生存期間中は有効であることが呼び出し側で保証される。
        // 継承により複数エンティティから並行に呼ばれうるので共有参照だけを作る
        unsafe {
            let callbacks_ptr = data as *const Callbacks;
            let callbacks = &*callbacks_ptr;
            //println!("C Callback - liveliness lost");
            if let Some(lost) = &callbacks.on_liveliness_lost {
                lost(DdsEntity::new(entity), status);
            }
        }
    }
}

impl DdsListener {
    #[deprecated]
    pub fn on_offered_deadline_missed<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_offered_deadline_missed_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.inner.lock().unwrap().callbacks {
            callbacks.on_offered_deadline_missed = Some(Box::new(callback));
        }
        self
    }

    unsafe extern "C" fn call_offered_deadline_missed_closure(
        entity: dds_entity_t,
        status: dds_offered_deadline_missed_status_t,
        data: *mut std::ffi::c_void,
    ) {
        // SAFETY: cyclonedds がリスナー登録時に渡した Callbacks へのポインタであり、
        // リスナーの生存期間中は有効であることが呼び出し側で保証される。
        // 継承により複数エンティティから並行に呼ばれうるので共有参照だけを作る
        unsafe {
            let callbacks_ptr = data as *const Callbacks;
            let callbacks = &*callbacks_ptr;
            //println!("C Callback - offered deadline missed");
            if let Some(missed) = &callbacks.on_offered_deadline_missed {
                missed(DdsEntity::new(entity), status);
            }
        }
    }
}

impl DdsListener {
    #[deprecated]
    pub fn on_offered_incompatible_qos<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_offered_incompatible_qos_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.inner.lock().unwrap().callbacks {
            callbacks.on_offered_incompatible_qos = Some(Box::new(callback));
        }
        self
    }

    unsafe extern "C" fn call_offered_incompatible_qos_closure(
        entity: dds_entity_t,
        status: dds_offered_incompatible_qos_status_t,
        data: *mut std::ffi::c_void,
    ) {
        // SAFETY: cyclonedds がリスナー登録時に渡した Callbacks へのポインタであり、
        // リスナーの生存期間中は有効であることが呼び出し側で保証される。
        // 継承により複数エンティティから並行に呼ばれうるので共有参照だけを作る
        unsafe {
            let callbacks_ptr = data as *const Callbacks;
            let callbacks = &*callbacks_ptr;
            //println!("C Callback - offered incompatible QOS");
            if let Some(incompatible) = &callbacks.on_offered_incompatible_qos {
                incompatible(DdsEntity::new(entity), status);
            }
        }
    }
}

impl DdsListener {
    #[deprecated]
    pub fn on_publication_matched<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_publication_matched_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.inner.lock().unwrap().callbacks {
            callbacks.on_publication_matched = Some(Box::new(callback));
        }
        self
    }

    unsafe extern "C" fn call_publication_matched_closure(
        entity: dds_entity_t,
        status: dds_publication_matched_status_t,
        data: *mut std::ffi::c_void,
    ) {
        // SAFETY: cyclonedds がリスナー登録時に渡した Callbacks へのポインタであり、
        // リスナーの生存期間中は有効であることが呼び出し側で保証される。
        // 継承により複数エンティティから並行に呼ばれうるので共有参照だけを作る
        unsafe {
            let callbacks_ptr = data as *const Callbacks;
            let callbacks = &*callbacks_ptr;
            //println!("C Callback - publication matched");
            if let Some(matched) = &callbacks.on_publication_matched {
                matched(DdsEntity::new(entity), status);
            }
        }
    }
}

impl DdsListener {
    #[deprecated]
    pub fn on_inconsistent_topic<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_inconsistent_topic_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.inner.lock().unwrap().callbacks {
            callbacks.on_inconsistent_topic = Some(Box::new(callback));
        }
        self
    }

    unsafe extern "C" fn call_inconsistent_topic_closure(
        entity: dds_entity_t,
        status: dds_inconsistent_topic_status_t,
        data: *mut std::ffi::c_void,
    ) {
        // SAFETY: cyclonedds がリスナー登録時に渡した Callbacks へのポインタであり、
        // リスナーの生存期間中は有効であることが呼び出し側で保証される。
        // 継承により複数エンティティから並行に呼ばれうるので共有参照だけを作る
        unsafe {
            let callbacks_ptr = data as *const Callbacks;
            let callbacks = &*callbacks_ptr;
            //println!("C Callback - inconsistent topic");
            if let Some(inconsistent) = &callbacks.on_inconsistent_topic {
                inconsistent(DdsEntity::new(entity), status);
            }
        }
    }
}

impl DdsListener {
    #[deprecated]
    pub fn on_data_on_readers<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.inner.lock().unwrap().callbacks {
            callbacks.on_data_on_readers = Some(Box::new(callback));
        }
        self
    }

    unsafe extern "C" fn call_data_on_readers_closure(
        entity: dds_entity_t,
        data: *mut std::ffi::c_void,
    ) {
        // SAFETY: cyclonedds がリスナー登録時に渡した Callbacks へのポインタであり、
        // リスナーの生存期間中は有効であることが呼び出し側で保証される。
        // 継承により複数エンティティから並行に呼ばれうるので共有参照だけを作る
        unsafe {
            let callbacks_ptr = data as *const Callbacks;
            let callbacks = &*callbacks_ptr;
            //println!("C Callback - data on readers");
            if let Some(data) = &callbacks.on_data_on_readers {
                data(DdsEntity::new(entity));
            }
        }
    }
}

impl Drop for DdsListener {
    fn drop(&mut self) {
        // delete the listener so we are sure of not
        // getting any callbacks
        if let Some(listener) = &self.inner.lock().unwrap().listener {
            unsafe {
                dds_reset_listener(*listener);
                dds_delete_listener(*listener);
            }
        }
        // gain back control of the Callback structure
        if let Some(raw) = self.inner.lock().unwrap().raw_ptr.take() {
            unsafe {
                // take ownership and free when out of scope
                let _ = Box::from_raw(raw);
            }
        }
    }
}

/// 既に設定済みのコールバックを残したまま`callback`を先に呼ぶよう連鎖させる
/// `DdsListenerBuilder`のメソッドを生成する。全イベントで実装が共通なためマクロにまとめる。
///
/// Why: クレート内部が使うイベント(data_available等)に利用者が同じイベントで
///      コールバックを設定すると、単純な代入ではどちらかが黙って消える。内部の状態更新を
///      先に済ませてから利用者のコールバックへ渡すことで、両立させつつ
///      利用者側のpanicで内部状態が飛ぶことも避ける
macro_rules! chain_callback {
    ($name:ident, $field:ident, $($arg:ident : $ty:ty),+) => {
        #[doc = concat!(
            "既存の[`Self::", stringify!($field), "`]を残したまま`callback`を先に呼ぶよう連鎖させる"
        )]
        pub fn $name<F>(self, callback: F) -> Self
        where
            F: Fn($($ty),+) + Send + Sync + 'static,
        {
            if let Some(callbacks) = &mut self.listener.inner.lock().unwrap().callbacks {
                let previous = callbacks.$field.take();
                callbacks.$field = Some(Box::new(move |$($arg: $ty),+| {
                    // 渡された(=内部の)コールバックを先に呼び、既存(=利用者側)は後で呼ぶ
                    callback($($arg.clone()),+);
                    if let Some(previous) = &previous {
                        previous($($arg),+);
                    }
                }));
            }
            self
        }
    };
}

/// [`DdsListener`]を組み立てるビルダー
///
/// 各イベントには2種類の登録方法がある。`on_*`は設定済みのコールバックを**上書き**し、
/// `chain_*`は設定済みのものを残して**連鎖**させる(渡した方が先に呼ばれる)。
/// [`crate::ReaderBuilder::with_listener_builder`]のようにクレート内部が
/// 同じイベントを使う経路へ渡す場合は、内部のコールバックが`chain_*`で足されるため
/// 利用者が`on_*`で設定した内容も消えずに残る
#[derive(Default)]
pub struct DdsListenerBuilder {
    listener: DdsListener,
}

impl DdsListenerBuilder {
    pub fn new() -> Self {
        Self {
            listener: DdsListener::new(),
        }
    }

    pub fn build(self) -> DdsListener {
        self.listener.hook()
    }

    pub fn on_data_available<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.listener.inner.lock().unwrap().callbacks {
            callbacks.on_data_available = Some(Box::new(callback));
        }

        self
    }

    /////
    pub fn on_sample_lost<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_sample_lost_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.listener.inner.lock().unwrap().callbacks {
            callbacks.on_sample_lost = Some(Box::new(callback));
        }
        self
    }

    //////
    pub fn on_sample_rejected<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_sample_rejected_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.listener.inner.lock().unwrap().callbacks {
            callbacks.on_sample_rejected = Some(Box::new(callback));
        }
        self
    }

    // Liveliness changed
    pub fn on_liveliness_changed<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_liveliness_changed_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.listener.inner.lock().unwrap().callbacks {
            callbacks.on_liveliness_changed = Some(Box::new(callback));
        }
        self
    }

    pub fn on_requested_deadline_missed<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_requested_deadline_missed_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.listener.inner.lock().unwrap().callbacks {
            callbacks.on_requested_deadline_missed = Some(Box::new(callback));
        }
        self
    }

    pub fn on_requested_incompatible_qos<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_requested_incompatible_qos_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.listener.inner.lock().unwrap().callbacks {
            callbacks.on_requested_incompatible_qos = Some(Box::new(callback));
        }
        self
    }

    pub fn on_subscription_matched<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_subscription_matched_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.listener.inner.lock().unwrap().callbacks {
            callbacks.on_subscription_matched = Some(Box::new(callback));
        }
        self
    }

    pub fn on_liveliness_lost<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_liveliness_lost_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.listener.inner.lock().unwrap().callbacks {
            callbacks.on_liveliness_lost = Some(Box::new(callback));
        }
        self
    }

    pub fn on_offered_deadline_missed<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_offered_deadline_missed_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.listener.inner.lock().unwrap().callbacks {
            callbacks.on_offered_deadline_missed = Some(Box::new(callback));
        }
        self
    }

    pub fn on_offered_incompatible_qos<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_offered_incompatible_qos_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.listener.inner.lock().unwrap().callbacks {
            callbacks.on_offered_incompatible_qos = Some(Box::new(callback));
        }
        self
    }

    pub fn on_publication_matched<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_publication_matched_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.listener.inner.lock().unwrap().callbacks {
            callbacks.on_publication_matched = Some(Box::new(callback));
        }
        self
    }

    pub fn on_inconsistent_topic<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity, dds_inconsistent_topic_status_t) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.listener.inner.lock().unwrap().callbacks {
            callbacks.on_inconsistent_topic = Some(Box::new(callback));
        }
        self
    }

    pub fn on_data_on_readers<F>(self, callback: F) -> Self
    where
        F: Fn(DdsEntity) + Send + Sync + 'static,
    {
        if let Some(callbacks) = &mut self.listener.inner.lock().unwrap().callbacks {
            callbacks.on_data_on_readers = Some(Box::new(callback));
        }
        self
    }

    chain_callback!(chain_data_available, on_data_available, entity: DdsEntity);
    chain_callback!(chain_data_on_readers, on_data_on_readers, entity: DdsEntity);
    chain_callback!(chain_sample_lost, on_sample_lost,
        entity: DdsEntity, status: dds_sample_lost_status_t);
    chain_callback!(chain_sample_rejected, on_sample_rejected,
        entity: DdsEntity, status: dds_sample_rejected_status_t);
    chain_callback!(chain_liveliness_changed, on_liveliness_changed,
        entity: DdsEntity, status: dds_liveliness_changed_status_t);
    chain_callback!(chain_requested_deadline_missed, on_requested_deadline_missed,
        entity: DdsEntity, status: dds_requested_deadline_missed_status_t);
    chain_callback!(chain_requested_incompatible_qos, on_requested_incompatible_qos,
        entity: DdsEntity, status: dds_requested_incompatible_qos_status_t);
    chain_callback!(chain_subscription_matched, on_subscription_matched,
        entity: DdsEntity, status: dds_subscription_matched_status_t);
    chain_callback!(chain_liveliness_lost, on_liveliness_lost,
        entity: DdsEntity, status: dds_liveliness_lost_status_t);
    chain_callback!(chain_offered_deadline_missed, on_offered_deadline_missed,
        entity: DdsEntity, status: dds_offered_deadline_missed_status_t);
    chain_callback!(chain_offered_incompatible_qos, on_offered_incompatible_qos,
        entity: DdsEntity, status: dds_offered_incompatible_qos_status_t);
    chain_callback!(chain_publication_matched, on_publication_matched,
        entity: DdsEntity, status: dds_publication_matched_status_t);
    chain_callback!(chain_inconsistent_topic, on_inconsistent_topic,
        entity: DdsEntity, status: dds_inconsistent_topic_status_t);
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::{Arc, Mutex};

    fn call_on_data_available(builder: &DdsListenerBuilder, entity: DdsEntity) {
        let mut inner = builder.listener.inner.lock().unwrap();
        if let Some(cb) = inner.callbacks.as_mut().unwrap().on_data_available.as_mut() {
            cb(entity);
        }
    }

    fn call_on_requested_deadline_missed(
        builder: &DdsListenerBuilder,
        entity: DdsEntity,
        status: dds_requested_deadline_missed_status_t,
    ) {
        let mut inner = builder.listener.inner.lock().unwrap();
        if let Some(cb) = inner
            .callbacks
            .as_mut()
            .unwrap()
            .on_requested_deadline_missed
            .as_mut()
        {
            cb(entity, status);
        }
    }

    /// Why: `chain_*`が既存(利用者)のコールバックを消してしまうと、内部の状態更新と
    ///      利用者処理が両立できない。既存が無い場合は単なる設定と同じ挙動であることも保証する
    /// Method: `chain_data_available`/`chain_requested_deadline_missed`それぞれについて
    ///         既存コールバックの有無ごとに呼び出し順を記録し、期待列と一括比較する
    #[test]
    fn chain_callback_orders_internal_before_existing() {
        let entity = unsafe { DdsEntity::new(0) };

        let record = |with_existing: bool, use_deadline_missed: bool| -> Vec<&'static str> {
            let calls: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
            let mut builder = DdsListenerBuilder::new();

            if with_existing {
                let calls = calls.clone();
                builder = if use_deadline_missed {
                    builder.on_requested_deadline_missed(move |_e, _s| {
                        calls.lock().unwrap().push("existing");
                    })
                } else {
                    builder.on_data_available(move |_e| {
                        calls.lock().unwrap().push("existing");
                    })
                };
            }
            {
                let calls = calls.clone();
                builder = if use_deadline_missed {
                    builder.chain_requested_deadline_missed(move |_e, _s| {
                        calls.lock().unwrap().push("chained");
                    })
                } else {
                    builder.chain_data_available(move |_e| {
                        calls.lock().unwrap().push("chained");
                    })
                };
            }

            if use_deadline_missed {
                call_on_requested_deadline_missed(
                    &builder,
                    entity.clone(),
                    dds_requested_deadline_missed_status_t::default(),
                );
            } else {
                call_on_data_available(&builder, entity.clone());
            }

            calls.lock().unwrap().clone()
        };

        // (with_existing, use_deadline_missed) -> 期待される呼び出し順
        let actual_and_expected = [
            (record(true, false), vec!["chained", "existing"]),
            (record(false, false), vec!["chained"]),
            (record(true, true), vec!["chained", "existing"]),
            (record(false, true), vec!["chained"]),
        ];

        for (actual, expected) in actual_and_expected {
            assert_eq!(actual, expected);
        }
    }
}
