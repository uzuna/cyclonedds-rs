//! CycloneDDSの組み込み型データリーダーを扱うモジュール
//!
//! 通常のDataReaderと同じ関数を使いながら、DDSの内部の情報を取り出す実装となっている
//! ここではDataReaderの特殊な実装であるBuiltinDataReaderを実装している
use std::{ffi::CStr, fmt::Debug, marker::PhantomData, sync::Arc};

use cyclonedds_sys::{
    DDSError, DdsEntity, dds_copy_qos, dds_create_qos, dds_create_reader, dds_delete_qos, dds_free,
    dds_read, dds_return_loan, dds_sample_info, dds_take,
};

use crate::{
    DdsListener, DdsParticipant, DdsQos, DdsReadable, Entity, Policy,
    futures::{ReaderType, participant_reader_listener},
};

// QoSのユーザー定義型を使って、内部状態に関するプロパティを読み出すための型
pub struct QoSPropertyRef<'a> {
    pub name: &'a CStr,
    pub value: &'a CStr,
}

impl QoSPropertyRef<'_> {
    /// DDSの組み込みトピックで定義されているプロパティ名
    pub const PROCESS_NAME: &'static CStr =
        cyclonedds_sys::DDS_BUILTIN_TOPIC_PARTICIPANT_PROPERTY_PROCESS_NAME;
    pub const PID: &'static CStr = cyclonedds_sys::DDS_BUILTIN_TOPIC_PARTICIPANT_PROPERTY_PID;
    pub const HOST_NAME: &'static CStr =
        cyclonedds_sys::DDS_BUILTIN_TOPIC_PARTICIPANT_PROPERTY_HOSTNAME;
    pub const NETWORK_ADDRESS: &'static CStr =
        cyclonedds_sys::DDS_BUILTIN_TOPIC_PARTICIPANT_PROPERTY_NETWORKADDRESSES;
}

/// BuiltinDataReaderとしての実装
pub trait BuiltinContainer {
    // 読み出したいデータに対応する型
    type Item;
    // BuiltinTopicの規定のID
    const TOPIC: i32;
}

/// DDSの参加インスタンスの型
///
/// 参加者が増えたことだけがわかり、不在になったことはわからない
pub struct Participants;

impl BuiltinContainer for Participants {
    type Item = cyclonedds_sys::dds_builtintopic_participant;
    const TOPIC: i32 = cyclonedds_sys::BUILTIN_TOPIC_DCPSPARTICIPANT;
}

/// DDSのPublishエンドポイントの型
///
/// qosの有無で追加or削除が区別できる
pub struct Publications;

impl BuiltinContainer for Publications {
    type Item = cyclonedds_sys::dds_builtintopic_endpoint;
    const TOPIC: i32 = cyclonedds_sys::BUILTIN_TOPIC_DCPSPUBLICATION;
}

/// DDSのSubscribeエンドポイントの型
///
/// qosの有無で追加or削除が区別できる
pub struct Subscriptions;

impl BuiltinContainer for Subscriptions {
    type Item = cyclonedds_sys::dds_builtintopic_endpoint;
    const TOPIC: i32 = cyclonedds_sys::BUILTIN_TOPIC_DCPSSUBSCRIPTION;
}

/// CycloneDDSを使う場合にほぼ必ず設定される参加者プロパティ
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CddsParticipantProperty {
    pub id: uuid::Uuid,
    pub process_name: String,
    pub pid: u32,
    pub host_name: String,
    pub network_addresses: String,
}

/// BuiltinSamplesの1サンプル単位にアクセスする型
pub struct BuiltinSample<'a, T> {
    sample: &'a T,
}

impl BuiltinSample<'_, cyclonedds_sys::dds_builtintopic_participant> {
    /// DDS参加者を識別するGUIDを取得する
    pub fn guid(&self) -> uuid::Uuid {
        crate::dds_participant::parse_guid(&self.sample.key)
    }

    /// 参加者が生存中ならtrueを返す
    pub fn is_alive(&self) -> bool {
        !self.sample.qos.is_null()
    }

    /// 参加者のプロパティをイテレータで取得する
    ///
    /// HostName,PIDが読み出せる
    /// 参加者が離脱した場合はNoneを返す
    pub fn props(&self) -> Option<impl Iterator<Item = QoSPropertyRef<'_>>> {
        if self.sample.qos.is_null() {
            return None;
        }
        unsafe {
            let value = &(*self.sample.qos).property.value;
            Some(
                std::slice::from_raw_parts(value.props, value.n as usize)
                    .iter()
                    .map(|prop| QoSPropertyRef {
                        name: CStr::from_ptr(prop.name),
                        value: CStr::from_ptr(prop.value),
                    }),
            )
        }
    }

    /// 参加者の一般的なプロパティを取得する
    pub fn property(&self) -> Result<Option<CddsParticipantProperty>, std::str::Utf8Error> {
        if self.sample.qos.is_null() {
            return Ok(None);
        }
        let guid = self.guid();
        let mut process_name = None;
        let mut pid = None;
        let mut host_name = None;
        let mut network_addresses = None;
        if let Some(props) = self.props() {
            for prop in props {
                if prop.name == QoSPropertyRef::PROCESS_NAME {
                    process_name = Some(prop.value.to_str()?.to_string());
                } else if prop.name == QoSPropertyRef::PID {
                    pid = Some(prop.value.to_str()?.parse().unwrap_or(0));
                } else if prop.name == QoSPropertyRef::HOST_NAME {
                    host_name = Some(prop.value.to_str()?.to_string());
                } else if prop.name == QoSPropertyRef::NETWORK_ADDRESS {
                    network_addresses = Some(prop.value.to_str()?.to_string());
                }
            }
        }

        let Some(process_name) = process_name else {
            return Ok(None);
        };
        let Some(pid) = pid else {
            return Ok(None);
        };
        let Some(host_name) = host_name else {
            return Ok(None);
        };
        let Some(network_addresses) = network_addresses else {
            return Ok(None);
        };
        Ok(Some(CddsParticipantProperty {
            id: guid,
            process_name,
            pid,
            host_name,
            network_addresses,
        }))
    }
}

impl BuiltinSample<'_, cyclonedds_sys::dds_builtintopic_endpoint> {
    /// DDSエンドポイントを識別するGUIDを取得する
    ///
    /// エンドポイント
    pub fn guid(&self) -> uuid::Uuid {
        crate::dds_participant::parse_guid(&self.sample.key)
    }

    /// エンドポイントを所属する参加者のGUIDを取得する
    pub fn participant_guid(&self) -> uuid::Uuid {
        crate::dds_participant::parse_guid(&self.sample.participant_key)
    }

    /// 離脱したイベントの場合はfalseになる
    pub fn is_alive(&self) -> bool {
        !self.sample.qos.is_null()
    }

    /// オンラインの場合はトピック名が取得できる
    pub fn name(&self) -> Option<&CStr> {
        if self.sample.topic_name.is_null() {
            return None;
        }
        Some(unsafe { CStr::from_ptr(self.sample.topic_name) })
    }

    /// オンラインの場合はトピックの型が取得できる
    pub fn type_name(&self) -> Option<&CStr> {
        if self.sample.type_name.is_null() {
            return None;
        }
        Some(unsafe { CStr::from_ptr(self.sample.type_name) })
    }

    /// トピックのQoSPolicyを取得する
    pub fn policy(&self) -> Option<Policy> {
        if self.sample.qos.is_null() {
            return None;
        }
        Some(Policy::from(self.sample.qos))
    }

    /// トピックのQoSを取得する
    pub fn qos(&self) -> Option<DdsQos> {
        if self.sample.qos.is_null() {
            return None;
        }
        unsafe {
            let q = dds_create_qos();
            let err: DDSError = dds_copy_qos(q, self.sample.qos).into();
            if let DDSError::DdsOk = err {
                Some(DdsQos::from_ptr(q))
            } else {
                dds_delete_qos(q);
                None
            }
        }
    }
}

/// read/take用の構造体
pub struct BuiltinSamples<T>
where
    T: BuiltinContainer,
{
    samples: *mut *mut T::Item,
    info: *mut dds_sample_info,
    len: u32,
    max: u32,
    /// 借りているサンプルの返却先(直前にread/takeしたリーダー)。
    /// リーダー実体を`Arc`で保持することで、`BuiltinDataReader`が先にdropされても
    /// エンティティ自体はローンを返却するまで生存する
    loaned_from: Option<Arc<ReaderInner<T>>>,
}

// `ReaderInner<T>`がDebugを実装しないため、`loaned_from`は借用有無だけを表示する
impl<T> std::fmt::Debug for BuiltinSamples<T>
where
    T: BuiltinContainer,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuiltinSamples")
            .field("samples", &self.samples)
            .field("info", &self.info)
            .field("len", &self.len)
            .field("max", &self.max)
            .field("loaned_from", &self.loaned_from.is_some())
            .finish()
    }
}

unsafe fn dds_alloc<T>(len: usize) -> *mut T {
    unsafe { cyclonedds_sys::dds_alloc(size_of::<T>() * len).cast() }
}

impl<T> BuiltinSamples<T>
where
    T: BuiltinContainer,
{
    /// 取得用のメモリ領域を確保
    pub fn new(len: usize) -> Self {
        unsafe {
            Self {
                samples: dds_alloc(len),
                info: dds_alloc(len),
                len: 0,
                max: len as u32,
                loaned_from: None,
            }
        }
    }

    /// 借りているサンプルをリーダーに返却する
    ///
    /// Why: `dds_read`/`dds_take`はサンプル本体をリーダーからのローンとして貸し出す
    /// (ポインタ配列が0初期化されているため常にローン経路になる)。`dds_return_loan`を
    /// 呼ばないとサンプル本体(QoSや文字列を含む)が読み出しの度にリークする。
    /// 返却するとポインタ配列の先頭がNULLに戻り、次の読み出しで再びローンが使われる
    fn return_loan(&mut self) {
        let Some(inner) = self.loaned_from.take() else {
            return;
        };
        if self.len > 0 {
            unsafe { dds_return_loan(inner.entity.entity(), self.samples.cast(), self.len as i32) };
        }
        self.len = 0;
    }

    /// 参加者をイテレータで取得
    pub fn iter(&self) -> impl Iterator<Item = BuiltinSample<'_, T::Item>> + '_ {
        unsafe {
            std::slice::from_raw_parts(self.samples, self.len as usize)
                .iter()
                .map(|&s| BuiltinSample { sample: &*s })
        }
    }

    /// 取得したサンプルの開放
    ///
    /// BuiltinSamplesを使いまわす場合は適宜呼び出すこと
    /// (読み出し時にも自動で返却されるため、明示的な呼び出しは必須ではない)
    pub fn clear(&mut self) {
        self.return_loan();
    }
}

impl<T> Drop for BuiltinSamples<T>
where
    T: BuiltinContainer,
{
    fn drop(&mut self) {
        // ポインタ配列を解放する前にサンプル本体を返却する
        self.return_loan();
        unsafe {
            dds_free(self.samples.cast());
            dds_free(self.info.cast());
        }
    }
}

pub(crate) struct ReaderInner<T> {
    entity: DdsEntity,
    // 登録している場合はそのメモリを確保するために保持
    _listener: Option<DdsListener>,
    reader_type: ReaderType,
    _phantom: PhantomData<T>,
}

impl<T> ReaderInner<T>
where
    T: BuiltinContainer,
{
    // readerの作成
    // listenerは事前に作り登録時からメモリ位置が変わらないようにその場で構造体に入れてArcで保持する
    fn create_reader(
        p: &DdsParticipant,
        qos: Option<DdsQos>,
        listener: Option<DdsListener>,
        reader_type: ReaderType,
    ) -> Result<Arc<Self>, DDSError> {
        unsafe {
            let r = dds_create_reader(
                DdsReadable::entity(p).entity(),
                T::TOPIC,
                qos.map_or(std::ptr::null(), Into::into),
                listener.as_ref().map_or(std::ptr::null(), Into::into),
            );

            if r >= 0 {
                Ok(Arc::new(Self {
                    entity: DdsEntity::new(r),
                    _listener: listener,
                    reader_type,
                    _phantom: PhantomData,
                }))
            } else {
                Err(DDSError::from(r))
            }
        }
    }

    fn create_async(p: &DdsParticipant, qos: Option<DdsQos>) -> Result<Arc<Self>, DDSError> {
        let (listener, waker) = participant_reader_listener();
        Self::create_reader(p, qos, Some(listener), waker)
    }
}

/// メタ情報を取得するための構造体
pub struct BuiltinDataReader<T> {
    inner: Arc<ReaderInner<T>>,
}

impl<T> BuiltinDataReader<T>
where
    T: BuiltinContainer,
{
    /// 同期リーダーを作成
    pub fn create(p: &DdsParticipant, qos: Option<DdsQos>) -> Result<Self, DDSError> {
        let inner = ReaderInner::create_reader(p, qos, None, ReaderType::Sync)?;
        Ok(BuiltinDataReader { inner })
    }

    /// 非同期リーダーを作成
    pub fn create_async(p: &DdsParticipant, qos: Option<DdsQos>) -> Result<Self, DDSError> {
        let inner = ReaderInner::create_async(p, qos)?;
        Ok(BuiltinDataReader { inner })
    }

    /// 同期読み出し
    pub fn read_now(&self, c: &mut BuiltinSamples<T>) -> Result<usize, DDSError> {
        Self::readn_from_entity_now(&self.inner, c, false)
    }

    /// 同期取り出し
    pub fn take_now(&self, c: &mut BuiltinSamples<T>) -> Result<usize, DDSError> {
        Self::readn_from_entity_now(&self.inner, c, true)
    }

    /// 読み出しor取り出し
    pub(crate) fn readn_from_entity_now(
        inner: &Arc<ReaderInner<T>>,
        c: &mut BuiltinSamples<T>,
        take: bool,
    ) -> Result<usize, DDSError> {
        if c.max == 0 {
            return Err(DDSError::BadParameter);
        }
        // 前回の読み出しで借りたままのサンプルを先に返す。返さずに読むと
        // サンプル本体がリークし、ポインタ配列も「アプリ提供バッファ」として
        // 扱われてローンの管理から外れる
        c.return_loan();
        let ret = unsafe {
            let len = c.max as usize;
            if take {
                dds_take(
                    inner.entity.entity(),
                    c.samples.cast(),
                    c.info as *mut _,
                    len,
                    len as u32,
                )
            } else {
                dds_read(
                    inner.entity.entity(),
                    c.samples.cast(),
                    c.info as *mut _,
                    len,
                    len as u32,
                )
            }
        };
        match ret {
            // データなしと本物のエラーを区別する(`DdsReader`と同じ方針)
            ..0 => Err(DDSError::from(ret)),
            0 => Err(DDSError::NoData),
            1.. => {
                c.len = ret as u32;
                // リーダー実体をArcで保持し、BuiltinDataReaderが先にdropされても
                // ローン返却まで実体を生存させる
                c.loaned_from = Some(inner.clone());
                Ok(ret as usize)
            }
        }
    }

    /// 保持しているサンプルを非同期で読み出す
    ///
    /// wakerが設定されていない場合は`Err(ReaderError::ReaderNotAsync)`を返す
    pub async fn read_async(
        &self,
        samples: &mut BuiltinSamples<T>,
    ) -> Result<usize, crate::error::ReaderError> {
        crate::futures::read(&self.inner.reader_type, || {
            Self::readn_from_entity_now(&self.inner, samples, false)
        })
        .await
    }

    /// 保持しているサンプルを非同期で取り出す
    ///
    /// wakerが設定されていない場合はErr(ReaderError::ReaderNotAsync)を返す
    pub async fn take_async(
        &self,
        samples: &mut BuiltinSamples<T>,
    ) -> Result<usize, crate::error::ReaderError> {
        crate::futures::read(&self.inner.reader_type, move || {
            Self::readn_from_entity_now(&self.inner, samples, true)
        })
        .await
    }
}

impl<T> Entity for BuiltinDataReader<T> {
    fn entity(&self) -> &DdsEntity {
        &self.inner.entity
    }
}

impl<T> Drop for ReaderInner<T> {
    fn drop(&mut self) {
        unsafe {
            // Listenerより先にReaderを先にDropしなければ、Listnerのコールバックが先に開放されてSEGVが起きる
            let ret: DDSError = cyclonedds_sys::dds_delete(self.entity.entity()).into();
            if DDSError::DdsOk != ret {
                eprint!("Ignoring dds_delete failure for BuiltinDataReader");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use cdds_derive::Topic;

    use super::*;
    use crate::*;

    #[derive(Debug, Clone, PartialEq, Topic, Serialize, Deserialize)]
    struct TestDiscoveryTopic {
        a: u32,
        b: String,
    }

    impl Default for TestDiscoveryTopic {
        fn default() -> Self {
            Self {
                a: 1,
                b: "test".to_string(),
            }
        }
    }

    // 他の影響を避けるためにloopbackのみを使う設定
    const CYCLONE_LOOPBACK_CONFIG: &str = r###"<?xml version="1.0" encoding="UTF-8" ?>
    <CycloneDDS xmlns="https://cdds.io/config"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="https://cdds.io/config https://raw.githubusercontent.com/eclipse-cyclonedds/cyclonedds/iceoryx/etc/cyclonedds.xsd">
        <Domain id="any">
            <General>
                <Interfaces>
                    <NetworkInterface name="lo" priority="default" />
                </Interfaces>
            </General>
        </Domain>
    </CycloneDDS>"###;

    /// discoveryイベントの待ち受けに掛けるタイムアウト。
    /// これが無いと検知に失敗したテストが永久にハングする
    const DISCOVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    // builtinのテストは副作用を避けるためそれぞれ独自のドメイン内で行う
    // (ドメインIDの実体はcommon.rsのTestDomainで一元管理)
    use crate::common::TestDomain;

    // 参加者の検知が期待通りか確認
    #[tokio::test]
    async fn test_discovery_participant() -> anyhow::Result<()> {
        // Make sure iox-roudi is running
        let participant = crate::common::tests::shared_participant_with_config(
            TestDomain::BuiltinDiscoveryParticipant.id(),
            CYCLONE_LOOPBACK_CONFIG,
        );
        let id = participant.guid();

        let reader_partic = BuiltinDataReader::<Participants>::create_async(participant, None)?;
        let mut sample_paric = BuiltinSamples::<Participants>::new(20);
        reader_partic.take_async(&mut sample_paric).await?;
        // 自身が見つかる。ただし、別プロセスで実行していたタスクが残っている場合は複数見つかるケースがあるので
        // 自身が含まれていたら良しとする
        // (件数のassertは、Okなら必ず1件以上という実装のため意味を持たない)
        let res = sample_paric
            .iter()
            .find(|p| p.guid() == id)
            .expect("自身の参加者が見つかるべき");
        assert!(res.is_alive());
        let props: Vec<_> = res.props().unwrap().collect();
        assert!(
            props
                .iter()
                .any(|prop| prop.name == QoSPropertyRef::HOST_NAME)
        );
        assert!(props.iter().any(|prop| prop.name == QoSPropertyRef::PID));

        // 非同期が期待通り0データを無視して待つことを確認
        sample_paric.clear();
        let res = reader_partic.take_now(&mut sample_paric);
        if res.is_ok() {
            for p in sample_paric.iter() {
                println!("Found participant({:?}): {:?}", id, p.guid());
            }
        }
        assert!(matches!(res, Err(DDSError::NoData)));

        let token = tokio_util::sync::CancellationToken::new();
        let cancel = token.clone();
        // create_taskが作成する参加者のguidをread_task側で待ち受けるために共有する
        let new_id: std::cell::Cell<Option<uuid::Uuid>> = std::cell::Cell::new(None);

        // create_taskで作成される参加者(new_id)の検知を確認する。
        //
        // CycloneDDS内部では自身の参加者についても生成から100ms後にSPDPの定期再送が
        // スケジュールされる(ddsi_participant.c)。また、他プロセスがたまたま同じドメインID
        // で動いていた場合も無関係なサンプルが混ざる可能性がある。よって「次に届くサンプルが
        // ちょうど1件で、それが新規参加者である」という前提では実行環境の遅延でフレーキーになる。
        // 代わりに、create_taskが実際に作成したguid(new_id)が見つかるまでループし、
        // それ以外のサンプル(自身の再送や無関係な参加者)は無視する
        //
        // ループにはタイムアウトを掛ける。掛けないと、検知に失敗した場合に
        // read_taskは`take_async`でPendingのまま、create_taskは`token.cancelled()`で
        // 待ち続けて相互に待ち合い、テストが永久にハングする
        let read_task = async {
            tokio::time::timeout(DISCOVERY_TIMEOUT, async {
                let mut sample_paric = BuiltinSamples::<Participants>::new(20);
                loop {
                    reader_partic.take_async(&mut sample_paric).await?;
                    let mut found = false;
                    for p in sample_paric.iter() {
                        if new_id.get() == Some(p.guid()) {
                            assert!(p.is_alive());
                            assert!(p.props().is_some());
                            found = true;
                        }
                    }
                    if found {
                        break;
                    }
                    sample_paric.clear();
                }
                Ok::<(), anyhow::Error>(())
            })
            .await
            .map_err(|_| anyhow::anyhow!("timeout waiting for the new participant discovery"))??;
            cancel.cancel();
            Ok::<(), anyhow::Error>(())
        };

        // create_taskで参加者を作成
        let create_task = async {
            // 想定通りなら待たなくても動作は変化しないが、read開始をなんとなく待つ。
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            // SAFETY: このテストは参加者の生成/離脱の検知を検証するためdropが必要で、
            // 共有参加者は使えない。生成/破棄は本タスクのみが行い、かつ外側の
            // `participant`(共有)が常に生存しているのでドメインのrefcountは0にならない
            let participant = unsafe {
                DdsParticipant::create(
                    Some(TestDomain::BuiltinDiscoveryParticipant.id()),
                    None,
                    None,
                )
            }?;
            new_id.set(Some(participant.guid()));
            // readの受信を待つ
            token.cancelled().await;
            drop(participant);
            Ok::<(), anyhow::Error>(())
        };

        tokio::try_join!(read_task, create_task)?;
        let new_id = new_id.get().expect("create_task should have set new_id");

        // 参加者の離脱(drop)はdds_delete_participant経由で即座にdispose通知が送られるが、
        // CycloneDDS内部ではSPDPの定期再送が生成後100msでスケジュールされており
        // (vendor/cyclonedds/src/core/ddsi/src/ddsi_participant.c)、実行環境の遅延次第で
        // そのalive再送がdispose通知と一緒に届くことがある。よって「0件」ではなく、
        // 「届いたサンプルが全てnew_id(今回dropした参加者)に関するものであること」を確認する
        sample_paric.clear();
        let res = reader_partic.take_now(&mut sample_paric);
        match res {
            Ok(_) => {
                for p in sample_paric.iter() {
                    assert_eq!(
                        p.guid(),
                        new_id,
                        "予期しない参加者の検知があった: {:?}",
                        p.guid()
                    );
                }
            }
            Err(DDSError::NoData) => {}
            Err(e) => panic!("unexpected error: {:?}", e),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_discovery_endpoint() -> anyhow::Result<()> {
        let participant = crate::common::tests::shared_participant_with_config(
            TestDomain::BuiltinDiscoveryEndpoint.id(),
            CYCLONE_LOOPBACK_CONFIG,
        );
        let id = participant.guid();

        // publisherが不在ならNoDataになることを確認
        let reader_partic = BuiltinDataReader::<Publications>::create_async(participant, None)?;
        let mut sample_paric = BuiltinSamples::<Publications>::new(20);

        std::thread::sleep(std::time::Duration::from_millis(100));
        let res = reader_partic.read_now(&mut sample_paric);
        if res.is_ok() {
            for p in sample_paric.iter() {
                println!(
                    "Found publication endpoint({:?}): {:?} {:?}",
                    id,
                    p.guid(),
                    p.name()
                );
            }
        }
        assert_eq!(res, Err(DDSError::NoData));

        // listener登録して一度も読まずに破棄する。listenerの解放が適切にできているか確認
        // 不適切な場合は後続のwriter追加/削除時にSEGVが起きる
        let reader_partic_drop_check =
            BuiltinDataReader::<Publications>::create_async(participant, None)?;
        drop(reader_partic_drop_check);

        let token = tokio_util::sync::CancellationToken::new();
        let cancel = token.clone();
        let policy = Policy::create_transient_local(10, None)?;

        // 参加者が見つかり次第タスクが完了する
        let expect_policy = policy.clone();
        // 読み出しに失敗しても検証を素通りしないようアサーションは`if let Ok`の外に置く。
        // 読み出せないまま止まるとcreate_taskと待ち合ってハングするのでタイムアウトも掛ける
        let read_task = async {
            let mut sample_paric = BuiltinSamples::<Publications>::new(20);
            let count = tokio::time::timeout(
                DISCOVERY_TIMEOUT,
                reader_partic.take_async(&mut sample_paric),
            )
            .await
            .map_err(|_| anyhow::anyhow!("timeout waiting for the publication discovery"))??;
            assert_eq!(count, 1);
            for p in sample_paric.iter() {
                assert_eq!(p.participant_guid(), id);
                assert_ne!(p.guid(), id);
                assert!(p.is_alive());
                let policy = p.policy().unwrap();
                assert_eq!(policy, expect_policy);

                assert_eq!(p.name(), Some(c"/dds_builtin/tests/TestDiscoveryTopic"));
            }
            cancel.cancel();
            Ok::<(), anyhow::Error>(())
        };

        let create_task = async {
            // 想定通りなら待たなくても動作は変化しないが、read開始をなんとなく待つ。
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            let topic =
                TestDiscoveryTopic::create_topic(participant, None, Some(policy.to_qos()?), None)?;
            let publisher = DdsPublisher::create(participant, None, None)?;
            let mut writer = DdsWriter::create(&publisher, topic, None, None)?;
            writer.write(Arc::new(TestDiscoveryTopic::default()))?;
            token.cancelled().await;
            drop(writer);
            drop(publisher);
            Ok::<(), anyhow::Error>(())
        };
        tokio::try_join!(read_task, create_task)?;

        // writerを削除の検知を確認
        let count = tokio::time::timeout(
            DISCOVERY_TIMEOUT,
            reader_partic.take_async(&mut sample_paric),
        )
        .await
        .map_err(|_| anyhow::anyhow!("timeout waiting for the publication dispose"))??;
        assert_eq!(count, 1);
        for p in sample_paric.iter() {
            assert_eq!(p.participant_guid(), id);
            assert_ne!(p.guid(), id);
            assert!(!p.is_alive());
        }
        sample_paric.clear();
        Ok(())
    }

    // UserData QoSがマッチングに影響せず、builtin discoveryデータ経由で他参加者から
    // 読み取れることを確認する
    #[tokio::test]
    async fn test_discovery_userdata() -> anyhow::Result<()> {
        let participant = crate::common::tests::shared_participant_with_config(
            TestDomain::BuiltinDiscoveryUserdata.id(),
            CYCLONE_LOOPBACK_CONFIG,
        );

        let reader_partic = BuiltinDataReader::<Publications>::create_async(participant, None)?;

        let mut writer_qos = DdsQos::create()?;
        writer_qos.set_userdata(b"role=logger");

        let topic = TestDiscoveryTopic::create_topic(participant, None, None, None)?;
        let publisher = DdsPublisher::create(participant, None, None)?;
        let writer = DdsWriter::create(&publisher, topic, Some(writer_qos), None)?;

        let mut sample_paric = BuiltinSamples::<Publications>::new(20);
        let count = tokio::time::timeout(
            DISCOVERY_TIMEOUT,
            reader_partic.take_async(&mut sample_paric),
        )
        .await
        .map_err(|_| anyhow::anyhow!("timeout waiting for the publication discovery"))??;
        assert_eq!(count, 1);
        for p in sample_paric.iter() {
            let qos = p.qos().unwrap();
            assert_eq!(qos.userdata(), Some(b"role=logger".to_vec()));
        }

        drop(writer);
        drop(publisher);
        Ok(())
    }
}
