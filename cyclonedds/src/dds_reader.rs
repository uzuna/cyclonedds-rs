/*
    Copyright 2021 Sojan James

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

use cyclonedds_sys::*;
use std::convert::From;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tracing::error;

pub use cyclonedds_sys::{DdsDomainId, DdsEntity};

use crate::futures::{ReaderType, data_reader_listener};
use crate::match_watch::MatchWatch;
use crate::serdes::{SampleBuffer, TopicType};
use crate::{
    DdsReadable, Entity, Keepalive, dds_listener::DdsListener, dds_listener::DdsListenerBuilder,
    dds_qos::DdsQos, dds_topic::DdsTopic,
};

/// Builder structure for reader
pub struct ReaderBuilder<T: TopicType> {
    maybe_qos: Option<DdsQos>,
    maybe_listener: Option<DdsListener>,
    maybe_listener_builder: Option<DdsListenerBuilder>,
    is_async: bool,
    absence_timeout: Option<Duration>,
    phantom: PhantomData<T>,
}

impl<T> Default for ReaderBuilder<T>
where
    T: TopicType,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ReaderBuilder<T>
where
    T: TopicType,
{
    pub fn new() -> Self {
        Self {
            maybe_qos: None,
            maybe_listener: None,
            maybe_listener_builder: None,
            is_async: false,
            absence_timeout: None,
            phantom: PhantomData,
        }
    }
    /// Create a reader with async support.  If this is enabled,
    /// the builder creates listeners internally. Any listener
    /// passed separately via the `with_listener` api will be
    /// ignored.
    pub fn as_async(mut self) -> Self {
        self.is_async = true;
        self
    }

    /// マッチするwriterが`timeout`の間1つも居なければ、[`DdsReader::read_async`]/
    /// [`DdsReader::take_async`]が`Err(ReaderError::NoMatchedWriter)`を返すようにする
    ///
    /// 計測はリーダー生成時から始まるため、writerが一度も現れない場合も検知できる。
    /// writerが居る間はデータが来なくてもエラーにはならない(それはdeadline QoSの領分)。
    ///
    /// 非同期リーダー専用の機能なので、指定すると[`Self::as_async`]と同じ扱いになる。
    /// [`Self::with_listener`]で渡したhook済みリスナーより優先されるため、
    /// 併用したい場合は[`Self::with_listener_builder`]を使うこと。
    ///
    /// 内部で登録するのは`on_subscription_matched`であって`on_liveliness_changed`ではないため、
    /// このオプションを有効にしても[`DdsReader::liveliness_changed_status`]の`*_change`は
    /// 従来どおり正確である。
    ///
    /// 判定に使う対向数はQoS互換でマッチした数のため、QoSが噛み合わない相手も不在として
    /// 扱われる(相手が起動していても検知されない)。理由を知りたい場合は
    /// [`Self::with_listener_builder`]で`on_requested_incompatible_qos`を観測すること。
    pub fn stop_when_no_writer(mut self, timeout: Duration) -> Self {
        self.is_async = true;
        self.absence_timeout = Some(timeout);
        self
    }

    /// Create a reader with the specified Qos
    pub fn with_qos(mut self, qos: DdsQos) -> Self {
        self.maybe_qos = Some(qos);
        self
    }

    /// Created a reader with the specified listener.
    /// Note that this is ignored if an async reader
    /// is created.
    ///
    /// hook済みの`DdsListener`は後からコールバックを連鎖できないため、非同期リーダーや
    /// 今後の内部コールバックと併用したい場合は[`Self::with_listener_builder`]を使うこと。
    pub fn with_listener(mut self, listener: DdsListener) -> Self {
        self.maybe_listener = Some(listener);
        self
    }

    /// hook前のリスナービルダーを渡す。クレートが必要とするコールバック(非同期化)は、
    /// ここで設定済みのものを潰さずに連鎖して追加される。
    ///
    /// [`Self::as_async`]/[`Self::stop_when_no_writer`]と併用でき、指定した場合は
    /// そちらが優先されてこのビルダーへ内部コールバックを連鎖する
    /// ([`Self::with_listener`]で渡したhook済みリスナーは無視される)。
    /// 一方、非同期化を使わない場合に[`Self::with_listener`]も指定していると、
    /// hook済みの方が優先されこちらの内容は無効になる(hook済みのリスナーは後から
    /// コールバックを連鎖できないため)。併用したい場合は`with_listener`を使わず
    /// このメソッドだけを使うこと。
    pub fn with_listener_builder(mut self, builder: DdsListenerBuilder) -> Self {
        self.maybe_listener_builder = Some(builder);
        self
    }

    pub fn create(
        self,
        entity: &dyn DdsReadable,
        topic: DdsTopic<T>,
    ) -> Result<DdsReader<T>, DDSError> {
        if self.is_async {
            DdsReader::create_async_with_watch(
                entity,
                topic,
                self.maybe_qos,
                self.maybe_listener_builder.unwrap_or_default(),
                self.absence_timeout,
            )
        } else if let Some(listener) = self.maybe_listener {
            DdsReader::create_sync_or_async(
                entity,
                topic,
                self.maybe_qos,
                Some(listener),
                ReaderType::Sync,
            )
        } else if let Some(listener_builder) = self.maybe_listener_builder {
            DdsReader::create_sync_or_async(
                entity,
                topic,
                self.maybe_qos,
                Some(listener_builder.build()),
                ReaderType::Sync,
            )
        } else {
            DdsReader::create_sync_or_async(entity, topic, self.maybe_qos, None, ReaderType::Sync)
        }
    }
}

struct Inner<T> {
    entity: DdsEntity,
    /// 生成に使ったトピック。手放すとUAFになりうる理由は[`DdsTopic`]を参照
    _topic: DdsTopic<T>,
    /// 生成元のsubscriber。先に消えるとC側で子のreaderごと削除されるため保持する
    _parent: Keepalive,
    _listener: Option<DdsListener>,
    reader_type: ReaderType,
}

impl<T> Inner<T> {
    fn new(
        entity: DdsEntity,
        topic: DdsTopic<T>,
        parent: Keepalive,
        maybe_listener: Option<DdsListener>,
        reader_type: ReaderType,
    ) -> Self {
        Inner {
            entity,
            _topic: topic,
            _parent: parent,
            _listener: maybe_listener,
            reader_type,
        }
    }
}

pub struct DdsReader<T> {
    inner: Arc<Inner<T>>,
}

/// マッチしているwriterの生存状況
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LivelinessChangedStatus {
    /// 生存しているwriterの数
    pub alive_count: u32,
    /// マッチしているが生存が確認できていないwriterの数
    pub not_alive_count: u32,
    /// 前回の読み出し以降の`alive_count`の変化量
    pub alive_count_change: i32,
    /// 前回の読み出し以降の`not_alive_count`の変化量
    pub not_alive_count_change: i32,
}

impl From<dds_liveliness_changed_status_t> for LivelinessChangedStatus {
    fn from(status: dds_liveliness_changed_status_t) -> Self {
        Self {
            alive_count: status.alive_count,
            not_alive_count: status.not_alive_count,
            alive_count_change: status.alive_count_change,
            not_alive_count_change: status.not_alive_count_change,
        }
    }
}

/// deadline QoSを守れなかった回数
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RequestedDeadlineMissedStatus {
    /// 違反の累計回数
    pub total_count: u32,
    /// 前回の読み出し以降の違反回数
    pub total_count_change: i32,
}

impl From<dds_requested_deadline_missed_status_t> for RequestedDeadlineMissedStatus {
    fn from(status: dds_requested_deadline_missed_status_t) -> Self {
        Self {
            total_count: status.total_count,
            total_count_change: status.total_count_change,
        }
    }
}

/// マッチしているwriterの数(discoveryの結果)
///
/// [`ReaderBuilder::stop_when_no_writer`]を有効にすると、内部で`on_subscription_matched`
/// リスナーを登録するため、DDSの仕様上コールバック時点で`*_change`がリセットされ、
/// 以降このAPIを読んでも変化量は常に0付近になる。`total_count`/`current_count`
/// (累計/現在数)自体は影響を受けない。
///
/// **このAPIを「writerが戻ったか」の判定に使わないこと。** FFIを経由するポーリング専用の
/// 窓口であり、復帰の検出には[`DdsReader::is_writer_absent`]を使う。
/// 用途は調査・監視・ログ出力に限る。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SubscriptionMatchedStatus {
    /// マッチしたwriterの累計数
    pub total_count: u32,
    /// 前回の読み出し以降の`total_count`の変化量
    pub total_count_change: i32,
    /// 現在マッチしているwriterの数
    pub current_count: u32,
    /// 前回の読み出し以降の`current_count`の変化量
    pub current_count_change: i32,
}

impl From<dds_subscription_matched_status_t> for SubscriptionMatchedStatus {
    fn from(status: dds_subscription_matched_status_t) -> Self {
        Self {
            total_count: status.total_count,
            total_count_change: status.total_count_change,
            current_count: status.current_count,
            current_count_change: status.current_count_change,
        }
    }
}

impl<T> DdsReader<T> {
    pub fn create(
        entity: &dyn DdsReadable,
        topic: DdsTopic<T>,
        maybe_qos: Option<DdsQos>,
        maybe_listener: Option<DdsListener>,
    ) -> Result<Self, DDSError> {
        Self::create_sync_or_async(entity, topic, maybe_qos, maybe_listener, ReaderType::Sync)
    }

    fn create_sync_or_async(
        entity: &dyn DdsReadable,
        topic: DdsTopic<T>,
        maybe_qos: Option<DdsQos>,
        maybe_listener: Option<DdsListener>,
        reader_type: ReaderType,
    ) -> Result<Self, DDSError> {
        unsafe {
            let w = dds_create_reader(
                entity.entity().entity(),
                topic.entity().entity(),
                maybe_qos.map_or(std::ptr::null(), |q| q.into()),
                maybe_listener
                    .as_ref()
                    .map_or(std::ptr::null(), |l| l.into()),
            );

            // `dds_create_reader`は成功時に正のエンティティハンドルを返す。
            // 0(DDS_RETCODE_OK)は有効なハンドルではないのでエラー扱いにする
            if w > 0 {
                Ok(DdsReader {
                    inner: Arc::new(Inner::new(
                        DdsEntity::new(w),
                        topic,
                        entity.keepalive(),
                        maybe_listener,
                        reader_type,
                    )),
                })
            } else {
                Err(DDSError::from(w))
            }
        }
    }

    /// マッチしているwriterの生存状況を取得する(LIVELINESS QoSのリース管理)
    ///
    /// `*_change`は前回の読み出し以降の変化量で、読み出すとリセットされる(DDSの仕様)。
    /// マッチしたまま生存表明が途絶えたwriter(`not_alive_count`)を検知するためのAPIで、
    /// [`crate::dds_qos::DdsQos::set_liveliness`]で`MANUAL_BY_*`を使う場合に意味を持つ。
    ///
    /// writerがそもそも居ない・discoveryごと消えたことを検知したい場合は
    /// [`ReaderBuilder::stop_when_no_writer`]または[`DdsReader::is_writer_absent`]を使うこと。
    /// 不在検知が内部で登録するのは`on_subscription_matched`だけで`on_liveliness_changed`は
    /// 登録しないため、これらを併用してもこちらの`*_change`は従来どおり正確に読める。
    pub fn liveliness_changed_status(&self) -> Result<LivelinessChangedStatus, DDSError> {
        let mut status = dds_liveliness_changed_status_t::default();
        let ret = unsafe { dds_get_liveliness_changed_status(self.entity().entity(), &mut status) };
        if ret == 0 {
            Ok(LivelinessChangedStatus::from(status))
        } else {
            Err(DDSError::from(ret))
        }
    }

    /// deadline QoSを守れなかった回数を取得する
    ///
    /// `total_count_change`は、違反が[`crate::error::ReaderError::RequestedDeadLineMissed`]
    /// として読み出し側に通知される際にリセットされるため0になりうる。累計は`total_count`で見ること。
    pub fn requested_deadline_missed_status(
        &self,
    ) -> Result<RequestedDeadlineMissedStatus, DDSError> {
        let mut status = dds_requested_deadline_missed_status_t::default();
        let ret = unsafe {
            dds_get_requested_deadline_missed_status(self.entity().entity(), &mut status)
        };
        if ret == 0 {
            Ok(RequestedDeadlineMissedStatus::from(status))
        } else {
            Err(DDSError::from(ret))
        }
    }

    /// マッチしているwriterの数を取得する
    ///
    /// 注意点は[`SubscriptionMatchedStatus`]を参照。
    pub fn subscription_matched_status(&self) -> Result<SubscriptionMatchedStatus, DDSError> {
        let mut status = dds_subscription_matched_status_t::default();
        let ret =
            unsafe { dds_get_subscription_matched_status(self.entity().entity(), &mut status) };
        if ret == 0 {
            Ok(SubscriptionMatchedStatus::from(status))
        } else {
            Err(DDSError::from(ret))
        }
    }

    /// Create an async reader. This constructor must be used if using any of the async functions.
    pub fn create_async(
        entity: &dyn DdsReadable,
        topic: DdsTopic<T>,
        maybe_qos: Option<DdsQos>,
    ) -> Result<Self, DDSError> {
        Self::create_async_with_watch(entity, topic, maybe_qos, DdsListenerBuilder::new(), None)
    }

    /// [`Self::create_async`]の内部実装。`listener_builder`には利用者が
    /// [`ReaderBuilder::with_listener_builder`]経由で設定済みのコールバックが入りうるため、
    /// 内部コールバックはchainして追加する。`absence_timeout`は
    /// [`ReaderBuilder::stop_when_no_writer`]経由で渡される
    pub(crate) fn create_async_with_watch(
        entity: &dyn DdsReadable,
        topic: DdsTopic<T>,
        maybe_qos: Option<DdsQos>,
        listener_builder: DdsListenerBuilder,
        absence_timeout: Option<Duration>,
    ) -> Result<Self, DDSError> {
        let watch = absence_timeout.map(MatchWatch::new);
        let (listener, waker) = data_reader_listener(listener_builder, watch);

        Self::create_sync_or_async(entity, topic, maybe_qos, Some(listener), waker)
    }

    /// マッチしているwriterが不在確定しているか
    /// ([`ReaderBuilder::stop_when_no_writer`]未指定なら常に`false`)
    ///
    /// ログ出力・監視用の窓口。`stop_when_no_writer`は非同期リーダーを強制するため、
    /// 同期リーダーではこの値は常に`false`のままになる。非同期リーダーは`read_async`/
    /// `take_async`のエラーで不在を知れるため、通常はそちらを使えば足りる。
    pub fn is_writer_absent(&self) -> bool {
        self.inner.reader_type.is_writer_absent()
    }

    /// CDRバッファ(デシリアライズ前)を読み出す
    pub fn readcdrn_from_entity_now(
        entity: &DdsEntity,
        buf: &mut SampleBuffer<T>,
        take: bool,
    ) -> Result<usize, DDSError> {
        use cyclonedds_sys::{
            DDS_ALIVE_INSTANCE_STATE, DDS_ANY_SAMPLE_STATE, DDS_ANY_VIEW_STATE,
            DDS_NOT_READ_SAMPLE_STATE, dds_readcdr, dds_takecdr,
        };
        let max_caps = buf.begin_read();
        if max_caps == 0 {
            return Err(DDSError::BadParameter);
        }
        // dds_readcdr/dds_takecdrの場合は内部で`to_sample`が呼び出されないため、SerDataで受信を行う
        let mut data = Box::<[*mut ddsi_serdata]>::new_uninit_slice(max_caps);
        let data_ptr = data.as_mut_ptr().cast();
        let info_ptr = buf.sample_info.as_mut_ptr();
        let ret = unsafe {
            if take {
                // 保持しているサンプルのうち、まだ読んでいないものを読む
                let mask =
                    DDS_NOT_READ_SAMPLE_STATE | DDS_ANY_VIEW_STATE | DDS_ALIVE_INSTANCE_STATE;
                dds_takecdr(entity.entity(), data_ptr, max_caps as u32, info_ptr, mask)
            } else {
                // 保持しているサンプルすべてを読む
                let mask = DDS_ANY_SAMPLE_STATE | DDS_ANY_VIEW_STATE | DDS_ALIVE_INSTANCE_STATE;
                dds_readcdr(entity.entity(), data_ptr, max_caps as u32, info_ptr, mask)
            }
        };
        match ret {
            ..0 => Err(DDSError::from(ret)),
            0 => Err(DDSError::NoData),
            1.. => {
                // 受信データを公開構造体であるSampleBufferにセットする
                for (i, data) in data.iter().enumerate().take(ret as usize) {
                    unsafe {
                        let serdata = data.assume_init();
                        buf.buffer[i].set_serdata(serdata);
                    }
                }
                buf.size = ret as usize;
                Ok(ret as usize)
            }
        }
    }

    /// CDRバッファ(デシリアライズ前)を同期で読み出す。データを消費しないので2回目でも同じデータが得られる
    pub fn readcdr_now(&self, buf: &mut SampleBuffer<T>) -> Result<usize, DDSError> {
        Self::readcdrn_from_entity_now(self.entity(), buf, false)
    }

    /// CDRバッファ(デシリアライズ前)を同期で取り出す
    pub fn takecdr_now(&self, buf: &mut SampleBuffer<T>) -> Result<usize, DDSError> {
        Self::readcdrn_from_entity_now(self.entity(), buf, true)
    }

    /// 保持しているサンプルを非同期で読み出す
    ///
    /// wakerが設定されていない場合は`Err(ReaderError::ReaderNotAsync)`を返す
    pub async fn readcdr_async(
        &self,
        samples: &mut SampleBuffer<T>,
    ) -> Result<usize, crate::error::ReaderError> {
        crate::futures::read(&self.inner.reader_type, || {
            Self::readcdrn_from_entity_now(self.entity(), samples, false)
        })
        .await
    }

    /// 保持しているサンプルを非同期で取り出す
    ///
    /// wakerが設定されていない場合はErr(ReaderError::ReaderNotAsync)を返す
    pub async fn takecdr_async(
        &self,
        samples: &mut SampleBuffer<T>,
    ) -> Result<usize, crate::error::ReaderError> {
        crate::futures::read(&self.inner.reader_type, move || {
            Self::readcdrn_from_entity_now(self.entity(), samples, true)
        })
        .await
    }
}

/// 型付きリーダー
impl<'a, T> DdsReader<T>
where
    T: Sized + TopicType,
{
    /// データを同期で読み出す
    pub fn read_now(&self, buf: &mut SampleBuffer<T>) -> Result<usize, DDSError> {
        Self::readn_from_entity_now(self.entity(), buf, false)
    }

    /// データを同期で取り出す
    pub fn take_now(&self, buf: &mut SampleBuffer<T>) -> Result<usize, DDSError> {
        Self::readn_from_entity_now(self.entity(), buf, true)
    }

    /// データのシリアライズ読み出し
    pub fn readn_from_entity_now(
        entity: &DdsEntity,
        buf: &mut SampleBuffer<T>,
        take: bool,
    ) -> Result<usize, DDSError> {
        let maxs = buf.begin_read();
        if maxs == 0 {
            return Err(DDSError::BadParameter);
        }
        let (mut data, info_ptr) = buf.as_mut_recv_ptr();
        let data_ptr = data.as_mut_ptr().cast();

        let ret = unsafe {
            if take {
                dds_take(
                    entity.entity(),
                    data_ptr,
                    info_ptr as *mut _,
                    maxs,
                    maxs as u32,
                )
            } else {
                dds_read(
                    entity.entity(),
                    data_ptr,
                    info_ptr as *mut _,
                    maxs,
                    maxs as u32,
                )
            }
        };
        match ret {
            // データがないことと本物のエラーを区別する。
            // 一律`OutOfResources`にすると`BAD_PARAMETER`や`ALREADY_DELETED`が
            // 「データなし」として握り潰されてしまう
            ..0 => Err(DDSError::from(ret)),
            0 => Err(DDSError::NoData),
            1.. => {
                // If first sample is value we assume all are
                if buf.is_valid_sample(0) {
                    buf.size = ret as usize;
                    Ok(ret as usize)
                } else {
                    // 先頭が無効サンプル(dispose/unregisterの通知)なら「データなし」として扱う。
                    //
                    // 注意: `take`の場合、この無効サンプルは既にリーダーから取り出されており、
                    // ここで捨てられる(呼び出し側からは最初から届かなかったように見える)。
                    // インスタンスの生存状態やwriterの消滅をこの経路で取ることはできないため、
                    // 必要なら`dds_get_*_status`系を包むstatus APIを使うこと
                    Err(DDSError::NoData)
                }
            }
        }
    }

    pub fn create_readcondition(
        &'a mut self,
        mask: StateMask,
    ) -> Result<DdsReadCondition<'a, T>, DDSError> {
        DdsReadCondition::create(self, mask)
    }

    /// データを非同期で読み出す
    ///
    /// wakerが設定されていない場合は`Err(ReaderError::ReaderNotAsync)`を返す
    ///
    /// # 完了条件
    /// **データが届いたときだけ**完了する。writerの出現/消滅(liveliness変化)や
    /// インスタンスのdisposeでは完了せずPendingのままになる。
    ///
    /// [`ReaderBuilder::stop_when_no_writer`]未指定時は、writerが全ていなくなっても
    /// この関数は返らない。writerの消滅を検知したい場合は
    /// [`ReaderBuilder::stop_when_no_writer`]を指定するか、呼び出し側で
    /// `tokio::time::timeout`等の上限を掛けること。指定した場合は、マッチするwriterが
    /// `timeout`の間1つも居ない状態が続くと`Err(ReaderError::NoMatchedWriter)`で完了する。
    pub async fn read_async(
        &self,
        samples: &mut SampleBuffer<T>,
    ) -> Result<usize, crate::error::ReaderError> {
        crate::futures::read(&self.inner.reader_type, || {
            Self::readn_from_entity_now(self.entity(), samples, false)
        })
        .await
    }

    /// データを非同期で取り出す
    ///
    /// wakerが設定されていない場合はErr(ReaderError::ReaderNotAsync)を返す
    ///
    /// # 完了条件
    /// 完了条件は[`DdsReader::read_async`]と同じ。加えて、dispose/unregisterの無効サンプルは
    /// ここで**取り出された上で捨てられる**(呼び出し側からは届かなかったように見える)ので、
    /// インスタンスの状態遷移をこの経路で観測することはできない
    pub async fn take_async(
        &self,
        samples: &mut SampleBuffer<T>,
    ) -> Result<usize, crate::error::ReaderError> {
        crate::futures::read(&self.inner.reader_type, move || {
            Self::readn_from_entity_now(self.entity(), samples, true)
        })
        .await
    }
}

impl<T> Entity for DdsReader<T>
where
    T: std::marker::Sized,
{
    fn entity(&self) -> &DdsEntity {
        &self.inner.entity
    }
}

impl<T> Drop for DdsReader<T>
where
    T: Sized,
{
    fn drop(&mut self) {
        unsafe {
            let ret: DDSError = cyclonedds_sys::dds_delete(self.inner.entity.entity()).into();
            if DDSError::DdsOk != ret {
                error!("cannot delete Reader: {}", ret);
            }
        }
    }
}

#[allow(dead_code)]
pub struct DdsReadCondition<'a, T: Sized>(DdsEntity, &'a DdsReader<T>);

impl<'a, T> DdsReadCondition<'a, T>
where
    T: Sized,
{
    fn create(reader: &'a DdsReader<T>, mask: StateMask) -> Result<Self, DDSError> {
        unsafe {
            let mask: u32 = *mask;
            let p = cyclonedds_sys::dds_create_readcondition(reader.entity().entity(), mask);
            if p > 0 {
                Ok(DdsReadCondition(DdsEntity::new(p), reader))
            } else {
                Err(DDSError::from(p))
            }
        }
    }
}

impl<'a, T> Entity for DdsReadCondition<'a, T>
where
    T: std::marker::Sized,
{
    fn entity(&self) -> &DdsEntity {
        &self.0
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::{DdsParticipant, DdsSubscriber};
    use crate::{DdsPublisher, DdsWriter, WriterBuilder};

    use cdds_derive::Topic;
    use serde::{Deserialize, Serialize};
    use tokio::runtime::Runtime;

    use crate::common::TestDomain;
    use crate::error::ReaderError;

    #[repr(C)]
    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
    enum Position {
        #[default]
        Front,
        Back,
    }

    #[derive(Serialize, Deserialize, Topic, Debug, PartialEq)]
    struct TestTopic {
        a: u32,
        b: u16,
        c: String,
        d: Vec<u8>,
        #[topic_key]
        e: u32,
        #[topic_key_enum]
        pos: Position,
    }

    impl Default for TestTopic {
        fn default() -> Self {
            Self {
                a: 10,
                b: 20,
                c: "TestTopic".to_owned(),
                d: vec![1, 2, 3, 4, 5],
                e: 0,
                pos: Position::default(),
            }
        }
    }

    #[derive(Serialize, Deserialize, Topic, Debug, PartialEq)]
    struct AnotherTopic {
        pub value: u32,
        pub name: String,
        pub arr: [String; 2],
        pub vec: Vec<String>,
        #[topic_key]
        pub key: u32,
    }

    impl Default for AnotherTopic {
        fn default() -> Self {
            assert!(Self::has_key());
            Self {
                value: 42,
                name: "the answer".to_owned(),
                arr: ["one".to_owned(), "two".to_owned()],
                vec: vec!["Hello".to_owned(), "world".to_owned()],
                key: 0,
            }
        }
    }

    #[test]
    fn test_reader_async() {
        let participant =
            DdsParticipant::get_or_create(Some(TestDomain::ReaderAsync.id())).unwrap();

        let topic = TestTopic::create_topic(participant, Some("test_topic"), None, None).unwrap();
        let another_topic = AnotherTopic::create_topic(participant, None, None, None).unwrap();

        let publisher = DdsPublisher::create(participant, None, None).unwrap();

        let mut writer = DdsWriter::create(&publisher, topic.clone(), None, None).unwrap();
        let mut another_writer =
            DdsWriter::create(&publisher, another_topic.clone(), None, None).unwrap();

        let subscriber = DdsSubscriber::create(participant, None, None).unwrap();
        let reader = DdsReader::create_async(&subscriber, topic, None).unwrap();
        let another_reader = DdsReader::create_async(&subscriber, another_topic, None).unwrap();

        let rt = Runtime::new().unwrap();

        rt.block_on(async {
            let _task = tokio::spawn(async move {
                // writerの出現(liveliness変化)では起きず、データが届くまで待つ
                let mut samplebuffer = SampleBuffer::new(1);
                let count = reader.take_async(&mut samplebuffer).await.unwrap();
                assert_eq!(count, 1);
                let (sample, info) = samplebuffer.iter_items().take(1).next().unwrap();
                assert!(*sample == TestTopic::default());
                assert!(info.is_valid());
                assert!(info.source_timestamp() > Duration::from_nanos(0));

                // writerの生存はstatusとして別途読める
                let status = reader.liveliness_changed_status().unwrap();
                assert_eq!(status.alive_count, 1);
                assert_eq!(status.alive_count_change, 1);
            });

            let _another_task = tokio::spawn(async move {
                let mut samples = AnotherTopic::create_sample_buffer(5);
                let count = another_reader.take_async(&mut samples).await.unwrap();
                assert_eq!(count, 1);
                for s in samples.iter() {
                    println!("Got sample {}", s.key);
                }
            });

            // add a delay to make sure the data is not ready immediately
            tokio::time::sleep(Duration::from_millis(100)).await;
            let data = Arc::new(TestTopic::default());
            writer.write(data).unwrap();

            another_writer
                .write(Arc::new(AnotherTopic::default()))
                .unwrap();

            tokio::time::sleep(Duration::from_millis(300)).await;
        });
    }

    /// Why: writerの出現は正常なイベントなので読み出しを起こしてはならない
    /// Method: writerを作るだけの状態で読み出しがタイムアウトすること、
    /// その後writeすれば読めること、生存状況はstatusで別途読めることを確認する
    #[tokio::test]
    async fn test_take_async_ignores_liveliness_change() {
        let participant =
            DdsParticipant::get_or_create(Some(TestDomain::ReaderLiveliness.id())).unwrap();
        let topic =
            TestTopic::create_topic(participant, Some("liveliness_topic"), None, None).unwrap();
        let subscriber = DdsSubscriber::create(participant, None, None).unwrap();
        let reader = DdsReader::create_async(&subscriber, topic.clone(), None).unwrap();

        let publisher = DdsPublisher::create(participant, None, None).unwrap();
        let mut writer = DdsWriter::create(&publisher, topic, None, None).unwrap();

        // writerが現れてもデータがなければ読み出しは返らない
        let mut buf = SampleBuffer::new(1);
        let elapsed =
            tokio::time::timeout(Duration::from_millis(300), reader.take_async(&mut buf)).await;
        assert!(
            elapsed.is_err(),
            "writerの出現だけで読み出しが返ってはいけない: {:?}",
            elapsed
        );

        // liveliness変化はstatusとして読める
        let status = reader.liveliness_changed_status().unwrap();
        assert_eq!(status.alive_count, 1);
        assert_eq!(status.alive_count_change, 1);
        // 読み出すと変化量はリセットされる
        let status = reader.liveliness_changed_status().unwrap();
        assert_eq!(status.alive_count, 1);
        assert_eq!(status.alive_count_change, 0);

        // データが届けば読める
        writer.write(Arc::new(TestTopic::default())).unwrap();
        let count = tokio::time::timeout(Duration::from_secs(5), reader.take_async(&mut buf))
            .await
            .expect("timeout waiting for the sample")
            .unwrap();
        assert_eq!(count, 1);
    }

    /// Why: `as_async`/`stop_when_no_writer`は`with_listener`/`with_listener_builder`より
    ///      優先されるべきで、逆転しているとhook済み`with_listener`を渡しただけで
    ///      非同期化が黙って無効化される(コンパイルは通り、以後`take_async`が常に
    ///      `ReaderNotAsync`を返す実行時の破壊になる)
    /// Method: 排他オプションの組み合わせでreaderを生成し、短いtimeoutで`take_async`を
    ///         probeして`ReaderNotAsync`が即座に返るか(Sync)否か(Async)を期待値と比較する
    #[tokio::test]
    async fn test_builder_option_priority_selects_reader_kind() {
        #[derive(Debug, Clone, Copy, PartialEq)]
        enum ReaderKind {
            Sync,
            Async,
        }

        async fn probe_kind(reader: &DdsReader<TestTopic>) -> ReaderKind {
            let mut buf = SampleBuffer::new(1);
            match tokio::time::timeout(Duration::from_millis(30), reader.take_async(&mut buf)).await
            {
                Ok(Err(ReaderError::ReaderNotAsync)) => ReaderKind::Sync,
                // Pending(timeout)/NoMatchedWriter/Okのいずれも、非同期リーダーとして
                // 生成できていることの証跡になる
                _ => ReaderKind::Async,
            }
        }

        struct Case {
            as_async: bool,
            with_listener: bool,
            with_listener_builder: bool,
            stop_when_no_writer: bool,
            expected: ReaderKind,
        }

        let cases = vec![
            // オプション未指定はSync
            Case {
                as_async: false,
                with_listener: false,
                with_listener_builder: false,
                stop_when_no_writer: false,
                expected: ReaderKind::Sync,
            },
            Case {
                as_async: true,
                with_listener: false,
                with_listener_builder: false,
                stop_when_no_writer: false,
                expected: ReaderKind::Async,
            },
            Case {
                as_async: false,
                with_listener: true,
                with_listener_builder: false,
                stop_when_no_writer: false,
                expected: ReaderKind::Sync,
            },
            Case {
                as_async: false,
                with_listener: false,
                with_listener_builder: true,
                stop_when_no_writer: false,
                expected: ReaderKind::Sync,
            },
            Case {
                as_async: false,
                with_listener: false,
                with_listener_builder: false,
                stop_when_no_writer: true,
                expected: ReaderKind::Async,
            },
            // as_async/stop_when_no_writerが優先され、with_listenerを渡していてもAsyncになる
            Case {
                as_async: true,
                with_listener: true,
                with_listener_builder: false,
                stop_when_no_writer: false,
                expected: ReaderKind::Async,
            },
            Case {
                as_async: false,
                with_listener: true,
                with_listener_builder: false,
                stop_when_no_writer: true,
                expected: ReaderKind::Async,
            },
            // 非同期化が無い場合、with_listenerとwith_listener_builderの併用でも種別はSync
            Case {
                as_async: false,
                with_listener: true,
                with_listener_builder: true,
                stop_when_no_writer: false,
                expected: ReaderKind::Sync,
            },
        ];

        let participant =
            DdsParticipant::get_or_create(Some(TestDomain::ReaderBuilderPriority.id())).unwrap();
        let subscriber = DdsSubscriber::create(participant, None, None).unwrap();

        for (i, case) in cases.into_iter().enumerate() {
            let topic = TestTopic::create_topic(
                participant,
                Some(&format!("builder_priority_{i}")),
                None,
                None,
            )
            .unwrap();
            let mut builder = ReaderBuilder::new();
            if case.as_async {
                builder = builder.as_async();
            }
            if case.with_listener {
                builder = builder.with_listener(DdsListenerBuilder::new().build());
            }
            if case.with_listener_builder {
                builder = builder.with_listener_builder(DdsListenerBuilder::new());
            }
            if case.stop_when_no_writer {
                builder = builder.stop_when_no_writer(Duration::from_millis(20));
            }
            let reader = builder.create(&subscriber, topic).unwrap();
            let actual = probe_kind(&reader).await;
            assert_eq!(actual, case.expected, "case {i}");
        }
    }

    /// Why: `stop_when_no_writer`はwriter不在をエラーとして返す新機能で、非対向・対向消滅・
    ///      データ受信直後の消滅・対向ありでデータなし・エラー後の復帰と再消滅、の各状態遷移で
    ///      正しく判定できることを保証する(復帰と再消滅はWatchdogの起こし忘れが最も出やすい経路)
    /// Method: 同一ドメイン上でシナリオを順に実行し、`take_async`の結果と`is_writer_absent()`を
    ///         期待値と構造体比較する。オプション未指定時の挙動は
    ///         `test_take_async_ignores_liveliness_change`が別途保証する
    #[tokio::test]
    async fn test_stop_when_no_writer() {
        #[derive(Debug, PartialEq)]
        struct Outcome {
            take_result: Result<usize, ReaderError>,
            is_writer_absent: bool,
        }

        let timeout = Duration::from_millis(300);
        let margin = timeout * 3;
        let participant =
            DdsParticipant::get_or_create(Some(TestDomain::ReaderNoWriter.id())).unwrap();
        let subscriber = DdsSubscriber::create(participant, None, None).unwrap();
        let publisher = DdsPublisher::create(participant, None, None).unwrap();

        // ケース1: writerを一度も作らない -> timeout後にNoMatchedWriterが確定する
        {
            let topic =
                TestTopic::create_topic(participant, Some("stop_when_no_writer_none"), None, None)
                    .unwrap();
            let reader = ReaderBuilder::new()
                .stop_when_no_writer(timeout)
                .create(&subscriber, topic)
                .unwrap();
            let mut buf = SampleBuffer::new(1);
            let take_result = tokio::time::timeout(margin, reader.take_async(&mut buf))
                .await
                .expect("不在判定自体がtimeoutした");
            let actual = Outcome {
                take_result,
                is_writer_absent: reader.is_writer_absent(),
            };
            assert_eq!(
                actual,
                Outcome {
                    take_result: Err(ReaderError::NoMatchedWriter { timeout }),
                    is_writer_absent: true,
                }
            );
        }

        // ケース2: writerが居てデータが来ない -> timeoutを超えてもPendingのまま
        {
            let topic = TestTopic::create_topic(
                participant,
                Some("stop_when_no_writer_pending"),
                None,
                None,
            )
            .unwrap();
            let reader = ReaderBuilder::new()
                .stop_when_no_writer(timeout)
                .create(&subscriber, topic.clone())
                .unwrap();
            let _writer = DdsWriter::create(&publisher, topic, None, None).unwrap();

            let mut buf = SampleBuffer::new(1);
            let elapsed = tokio::time::timeout(margin, reader.take_async(&mut buf)).await;
            assert!(
                elapsed.is_err(),
                "writerが居る間はデータが無くてもエラーになってはいけない: {elapsed:?}"
            );
            assert!(!reader.is_writer_absent());
        }

        // ケース3: writerをdropして消す -> dropの概ねtimeout後にNoMatchedWriterが確定する
        {
            let topic =
                TestTopic::create_topic(participant, Some("stop_when_no_writer_gone"), None, None)
                    .unwrap();
            let reader = ReaderBuilder::new()
                .stop_when_no_writer(timeout)
                .create(&subscriber, topic.clone())
                .unwrap();
            let writer = DdsWriter::create(&publisher, topic, None, None).unwrap();
            // マッチが成立するのを待ってからwriterを消す
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(writer);

            let mut buf = SampleBuffer::new(1);
            let take_result = tokio::time::timeout(margin, reader.take_async(&mut buf))
                .await
                .expect("不在判定自体がtimeoutした");
            assert_eq!(take_result, Err(ReaderError::NoMatchedWriter { timeout }));
            assert!(reader.is_writer_absent());
        }

        // ケース4: データを受けた直後にwriterがdrop -> 先にOk(1)が返り、読み切った後にエラーになる
        {
            let topic = TestTopic::create_topic(
                participant,
                Some("stop_when_no_writer_data_then_gone"),
                None,
                None,
            )
            .unwrap();
            let reader = ReaderBuilder::new()
                .stop_when_no_writer(timeout)
                .create(&subscriber, topic.clone())
                .unwrap();
            let mut writer = DdsWriter::create(&publisher, topic, None, None).unwrap();

            let mut buf = SampleBuffer::new(1);
            writer.write(Arc::new(TestTopic::default())).unwrap();
            let count = tokio::time::timeout(Duration::from_secs(5), reader.take_async(&mut buf))
                .await
                .expect("timeout waiting for the sample")
                .unwrap();
            assert_eq!(count, 1);

            drop(writer);
            let take_result = tokio::time::timeout(margin, reader.take_async(&mut buf))
                .await
                .expect("不在判定自体がtimeoutした");
            assert_eq!(take_result, Err(ReaderError::NoMatchedWriter { timeout }));
        }

        // ケース5: エラー後にwriterを作り直すと通常の待機へ戻ってデータを読め、
        // 再びdropすれば二度目の不在も検知できる(Watchdogが起こし忘れると
        // この二度目の不在検知だけが永久に来なくなる)
        {
            let topic = TestTopic::create_topic(
                participant,
                Some("stop_when_no_writer_recovers"),
                None,
                None,
            )
            .unwrap();
            let reader = ReaderBuilder::new()
                .stop_when_no_writer(timeout)
                .create(&subscriber, topic.clone())
                .unwrap();

            let mut buf = SampleBuffer::new(1);
            let take_result = tokio::time::timeout(margin, reader.take_async(&mut buf))
                .await
                .expect("不在判定自体がtimeoutした");
            assert_eq!(take_result, Err(ReaderError::NoMatchedWriter { timeout }));

            // writerを作り直すとエラーが解け、届いたデータを読める
            let mut writer = DdsWriter::create(&publisher, topic, None, None).unwrap();
            writer.write(Arc::new(TestTopic::default())).unwrap();
            let count = tokio::time::timeout(Duration::from_secs(5), reader.take_async(&mut buf))
                .await
                .expect("timeout waiting for the sample")
                .unwrap();
            assert_eq!(count, 1);
            assert!(!reader.is_writer_absent());

            // 再度writerを消すと、二度目の不在も確定する
            drop(writer);
            let take_result = tokio::time::timeout(margin, reader.take_async(&mut buf))
                .await
                .expect("2回目の不在判定自体がtimeoutした(Watchdogの起こし忘れの疑い)");
            assert_eq!(take_result, Err(ReaderError::NoMatchedWriter { timeout }));
        }
    }

    /// Why: `current_count`はQoS互換でマッチした対向の数なので、非互換な相手は
    ///      「居ても居ないものとして」不在判定に入る。書かないと「相手は動いているのに
    ///      不在扱いされる」という驚きになるため、挙動を固定した上で、理由を知る手段が
    ///      `with_listener_builder`経由で残っていることも示す
    /// Method: RxOの規則上非互換になるreader=Reliable/writer=BestEffortの組で
    ///         不在判定を確認し、同じ組み合わせで非互換コールバックが発火することと、
    ///         reader/writerともReliable(互換)なら不在にならないことを1テストにまとめる
    #[tokio::test]
    async fn test_qos_incompatible_is_treated_as_absent() {
        #[derive(Debug, PartialEq)]
        struct Outcome {
            take_result: Result<usize, ReaderError>,
            is_writer_absent: bool,
            is_reader_absent: bool,
        }

        fn reliable_qos() -> DdsQos {
            let mut qos = DdsQos::create().unwrap();
            qos.set_reliability(
                dds_reliability_kind::DDS_RELIABILITY_RELIABLE,
                Duration::from_millis(100),
            );
            qos
        }

        fn best_effort_qos() -> DdsQos {
            let mut qos = DdsQos::create().unwrap();
            qos.set_reliability(
                dds_reliability_kind::DDS_RELIABILITY_BEST_EFFORT,
                Duration::ZERO,
            );
            qos
        }

        let timeout = Duration::from_millis(300);
        let margin = timeout * 3;
        let participant =
            DdsParticipant::get_or_create(Some(TestDomain::QosIncompatible.id())).unwrap();
        let subscriber = DdsSubscriber::create(participant, None, None).unwrap();
        let publisher = DdsPublisher::create(participant, None, None).unwrap();

        // ケース1: reader=Reliable, writer=BestEffort -> RxOの規則上マッチせず、
        // 双方が相手を不在と判定する
        {
            let topic =
                TestTopic::create_topic(participant, Some("qos_incompatible_absent"), None, None)
                    .unwrap();
            let reader = ReaderBuilder::new()
                .with_qos(reliable_qos())
                .stop_when_no_writer(timeout)
                .create(&subscriber, topic.clone())
                .unwrap();
            let writer = WriterBuilder::new()
                .with_qos(best_effort_qos())
                .watch_reader_absence(timeout)
                .create(&publisher, topic)
                .unwrap();

            let mut buf = SampleBuffer::new(1);
            let take_result = tokio::time::timeout(margin, reader.take_async(&mut buf))
                .await
                .expect("不在判定自体がtimeoutした");
            let actual = Outcome {
                take_result,
                is_writer_absent: reader.is_writer_absent(),
                is_reader_absent: writer.is_reader_absent(),
            };
            assert_eq!(
                actual,
                Outcome {
                    take_result: Err(ReaderError::NoMatchedWriter { timeout }),
                    is_writer_absent: true,
                    is_reader_absent: true,
                }
            );
        }

        // ケース2: 同じ非互換設定でon_requested_incompatible_qos/on_offered_incompatible_qosを
        // 観測する -> マッチしない理由を知る手段は残っている
        {
            let topic =
                TestTopic::create_topic(participant, Some("qos_incompatible_reason"), None, None)
                    .unwrap();
            let reader_notified = Arc::new(AtomicBool::new(false));
            let writer_notified = Arc::new(AtomicBool::new(false));

            let reader_listener = DdsListenerBuilder::new().on_requested_incompatible_qos({
                let notified = reader_notified.clone();
                move |_entity, _status| notified.store(true, Ordering::SeqCst)
            });
            let _reader = ReaderBuilder::new()
                .with_qos(reliable_qos())
                .with_listener_builder(reader_listener)
                .create(&subscriber, topic.clone())
                .unwrap();

            let writer_listener = DdsListenerBuilder::new().on_offered_incompatible_qos({
                let notified = writer_notified.clone();
                move |_entity, _status| notified.store(true, Ordering::SeqCst)
            });
            let _writer = WriterBuilder::new()
                .with_qos(best_effort_qos())
                .with_listener_builder(writer_listener)
                .create(&publisher, topic)
                .unwrap();

            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(
                reader_notified.load(Ordering::SeqCst),
                "readerに非互換の通知が届くはず"
            );
            assert!(
                writer_notified.load(Ordering::SeqCst),
                "writerに非互換の通知が届くはず"
            );
        }

        // ケース3(対照): reader/writerともReliable(互換) -> マッチして不在にならない
        {
            let topic =
                TestTopic::create_topic(participant, Some("qos_compatible"), None, None).unwrap();
            let reader = ReaderBuilder::new()
                .with_qos(reliable_qos())
                .stop_when_no_writer(timeout)
                .create(&subscriber, topic.clone())
                .unwrap();
            let mut writer = WriterBuilder::new()
                .with_qos(reliable_qos())
                .create(&publisher, topic)
                .unwrap();

            writer.write(Arc::new(TestTopic::default())).unwrap();
            let mut buf = SampleBuffer::new(1);
            let count = tokio::time::timeout(Duration::from_secs(5), reader.take_async(&mut buf))
                .await
                .expect("timeout waiting for the sample")
                .unwrap();
            assert_eq!(count, 1);
            assert!(!reader.is_writer_absent());
        }
    }
}
