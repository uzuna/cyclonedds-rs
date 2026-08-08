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

use crate::shared_registry::{Shared, SharedRegistry};
use crate::{DdsReadable, DdsWritable, Entity, dds_listener::DdsListener, dds_qos::DdsQos};
pub use cyclonedds_sys::{DDSError, DdsDomainId, DdsEntity};
use std::convert::From;
use std::sync::LazyLock;

/// `dds_create_participant`にドメイン指定なしを伝える値(`DDS_DOMAIN_DEFAULT`)
const DDS_DOMAIN_DEFAULT: DdsDomainId = 0xFFFF_FFFF;

/// プロセス寿命で共有するParticipantの登録簿
static SHARED_PARTICIPANTS: LazyLock<SharedRegistry<DdsParticipant>> =
    LazyLock::new(SharedRegistry::new);

/// Builder struct for a Participant.
/// #Example
/// ```text
/// use cyclonedds_rs::{DdsListener, ParticipantBuilder};
/// let listener = DdsListener::new()
///   .on_subscription_matched(|a,b| {
///     println!("Subscription matched!");
/// }).on_publication_matched(|a,b|{
///     println!("Publication matched");
/// }).
/// hook();
/// let participant = ParticipantBuilder::new()
///         .with_listener(listener)
///         .get_or_create()
///         .expect("Unable to create participant");
///
///```
///
/// 参加者はドメインIDごとにプロセス内で共有される([`ParticipantBuilder::get_or_create`])。
/// 所有権を持つ参加者が必要な場合は`unsafe`な[`ParticipantBuilder::create`]を使うが、
/// [`DdsParticipant::create`]のSafety節の制約を満たす必要がある。
///
pub struct ParticipantBuilder {
    maybe_domain: Option<DdsDomainId>,
    maybe_qos: Option<DdsQos>,
    maybe_listener: Option<DdsListener>,
}

impl Default for ParticipantBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ParticipantBuilder {
    pub fn new() -> Self {
        ParticipantBuilder {
            maybe_domain: None,
            maybe_qos: None,
            maybe_listener: None,
        }
    }

    pub fn with_domain(mut self, domain: DdsDomainId) -> Self {
        self.maybe_domain = Some(domain);
        self
    }

    pub fn with_qos(mut self, qos: DdsQos) -> Self {
        self.maybe_qos = Some(qos);
        self
    }

    pub fn with_listener(mut self, listener: DdsListener) -> Self {
        self.maybe_listener = Some(listener);
        self
    }

    /// 所有権を持つParticipantを生成する
    ///
    /// # Safety
    /// [`DdsParticipant::create`]と同じ制約を持つ。特別な理由がなければ
    /// [`ParticipantBuilder::get_or_create`]を使うこと。
    pub unsafe fn create(self) -> Result<DdsParticipant, DDSError> {
        unsafe { DdsParticipant::create(self.maybe_domain, self.maybe_qos, self.maybe_listener) }
    }

    /// プロセス内で共有されるParticipantを取得する(なければ生成する)
    ///
    /// 詳細は[`DdsParticipant::get_or_create_with`]を参照。
    pub fn get_or_create(self) -> Result<&'static DdsParticipant, DDSError> {
        DdsParticipant::get_or_create_with(self.maybe_domain, self.maybe_qos, self.maybe_listener)
    }
}

#[allow(dead_code)]
pub struct DdsParticipant(DdsEntity, Option<DdsListener>);

impl DdsParticipant {
    /// 所有権を持つDDS Participantを作成する(dropでドメインから離脱する)
    ///
    /// # Safety
    /// cyclonedds本体は「同一プロセス内で同じドメインIDの参加者を並行して
    /// 生成/破棄する」ケースに対してスレッドセーフではない。あるスレッドで
    /// そのドメインの**最後の**参加者がdropされると、cycloneddsはドメインの
    /// 内部状態を`rtps_fini`で解体するが、その最中に別スレッドが同じドメインの
    /// 参加者を生成/使用すると解放済みメモリを参照してSEGVする
    /// (再現手順とスタックトレースは`docs/domain-lifecycle.md`参照)。
    ///
    /// 呼び出し側は次のいずれかを保証すること:
    /// - 同じドメインIDに対する`create`とdrop(参加者・[`crate::DdsDomain`]の両方)が
    ///   プロセス内で決して並行しない
    /// - あるいは、そのドメインの参加者refcountが実行中に0へ落ちない
    ///   (例: 別の参加者を常に生存させておく)
    ///
    /// 参加者の生成/破棄ライフサイクル自体を検証したい場合を除き、
    /// 安全な[`DdsParticipant::get_or_create`]を使うこと。
    pub unsafe fn create(
        maybe_domain: Option<DdsDomainId>,
        maybe_qos: Option<DdsQos>,
        maybe_listener: Option<DdsListener>,
    ) -> Result<Self, DDSError> {
        unsafe {
            let p = cyclonedds_sys::dds_create_participant(
                maybe_domain.unwrap_or(DDS_DOMAIN_DEFAULT),
                maybe_qos.map_or(std::ptr::null(), |d| d.into()),
                maybe_listener
                    .as_ref()
                    .map_or(std::ptr::null(), |l| l.into()),
            );
            if p > 0 {
                Ok(DdsParticipant(DdsEntity::new(p), maybe_listener))
            } else {
                Err(DDSError::from(p))
            }
        }
    }

    /// 指定ドメインの共有Participantを取得する(初回のみ生成される)
    ///
    /// 詳細は[`DdsParticipant::get_or_create_with`]を参照。
    pub fn get_or_create(maybe_domain: Option<DdsDomainId>) -> Result<&'static Self, DDSError> {
        Self::get_or_create_with(maybe_domain, None, None)
    }

    /// 指定ドメインの共有Participantを取得する(初回のみ生成される)
    ///
    /// [`DdsParticipant::create`]のsafeな代替。ドメインIDごとに参加者を1つだけ生成し、
    /// プロセス終了までdropしない(意図的にリークする)ことで、
    /// 「そのドメインの参加者refcountが0に落ちる瞬間」を無くし、cyclonedds側の
    /// 生成/破棄レースによるSEGVを構造的に回避する。生成自体もプロセス内の
    /// ミューテックスで直列化されるため、複数スレッドから同時に呼んでも安全。
    ///
    /// 注意点:
    /// - **`maybe_qos` / `maybe_listener`は最初の生成時のみ反映される**。
    ///   既に同じドメインの参加者があるのにどちらかを指定した場合、
    ///   黙って無視するのではなく`Err(DDSError::PreconditionNotMet)`を返す。
    ///   参加者ごとに異なるQoS/Listenerが必要なら`unsafe`な
    ///   [`DdsParticipant::create`]を使うこと。
    /// - 返す参加者はdropされないため、`dds_delete`によるドメイン離脱通知は
    ///   プロセス終了まで送られない。参加者の離脱を観測するテスト等には使えない。
    /// - 同じ参加者を複数箇所が共有するため、トピック名の衝突に注意すること
    ///   (同名・同型でもQoSが異なると`dds_create_topic`は失敗しうる)。
    /// - このAPIは[`crate::DdsDomain`]のdropまでは防げない。明示的に作成した
    ///   `DdsDomain`をdropすると、共有参加者ごとドメインが解体される点は変わらない。
    ///
    /// # Errors
    /// 既に同じドメインの共有参加者が存在し、かつ`maybe_qos`/`maybe_listener`の
    /// いずれかが`Some`の場合は`DDSError::PreconditionNotMet`を返す。
    pub fn get_or_create_with(
        maybe_domain: Option<DdsDomainId>,
        maybe_qos: Option<DdsQos>,
        maybe_listener: Option<DdsListener>,
    ) -> Result<&'static Self, DDSError> {
        let has_explicit_args = maybe_qos.is_some() || maybe_listener.is_some();
        let shared = SHARED_PARTICIPANTS.get_or_create(
            maybe_domain.unwrap_or(DDS_DOMAIN_DEFAULT),
            // SAFETY: 生成は登録簿のロックで直列化され、作った参加者はリークして
            // 保持されるかdropされるかのどちらかだが、dropするのは「同じドメインに
            // 既存の共有参加者がある」場合だけなのでrefcountは0にならない
            || unsafe { Self::create(maybe_domain, maybe_qos, maybe_listener) },
            // `DDS_DOMAIN_DEFAULT`で要求された場合、実際のドメインIDは設定
            // (`CYCLONEDDS_URI`)依存で生成後にしか分からない
            |participant| participant.domain_id(),
        )?;
        match shared {
            Shared::Created(participant) => Ok(participant),
            Shared::Existing(_) if has_explicit_args => Err(DDSError::PreconditionNotMet),
            Shared::Existing(participant) => Ok(participant),
        }
    }

    /// この参加者が所属するドメインIDを取得する
    pub fn domain_id(&self) -> Result<DdsDomainId, DDSError> {
        let mut id: DdsDomainId = 0;
        let ret = unsafe { cyclonedds_sys::dds_get_domainid(self.0.entity(), &mut id) };
        if ret == 0 {
            Ok(id)
        } else {
            Err(DDSError::from(ret))
        }
    }

    pub fn guid(&self) -> uuid::Uuid {
        let guid = &mut cyclonedds_sys::dds_guid_t::default();
        unsafe { cyclonedds_sys::dds_get_guid(self.0.entity(), guid) };
        parse_guid(guid)
    }
}

impl Drop for DdsParticipant {
    fn drop(&mut self) {
        unsafe {
            let ret: DDSError = cyclonedds_sys::dds_delete(self.0.entity()).into();
            if DDSError::DdsOk != ret {
                panic!("cannot delete participant: {}", ret);
            } else {
                //println!("Participant dropped");
            }
        }
    }
}

impl DdsWritable for DdsParticipant {
    fn entity(&self) -> &DdsEntity {
        &self.0
    }
}

impl DdsReadable for DdsParticipant {
    fn entity(&self) -> &DdsEntity {
        &self.0
    }
}

impl Entity for DdsParticipant {
    fn entity(&self) -> &DdsEntity {
        &self.0
    }
}

pub(crate) fn parse_guid(guid_t: &cyclonedds_sys::dds_guid_t) -> uuid::Uuid {
    uuid::Uuid::from_bytes(guid_t.v)
}

#[cfg(test)]
mod dds_participant_tests {
    use super::*;

    const DDS_PARTICIPANT_TEST_CREATE: DdsDomainId = 26;
    const DDS_PARTICIPANT_TEST_GET_OR_CREATE: DdsDomainId = 27;

    #[test]
    fn test_create() {
        let _domain = crate::common::tests::create_loopback_domain(DDS_PARTICIPANT_TEST_CREATE)
            .expect("failed to create loopback domain");
        let mut qos = DdsQos::create().unwrap();
        qos.set_lifespan(std::time::Duration::from_nanos(1000));
        // SAFETY: このテストは参加者の生成/破棄ライフサイクル自体を検証するため
        // 共有参加者を使えない。DDS_PARTICIPANT_TEST_CREATEはこのテスト専用の
        // ドメインIDであり、同一プロセス内でこのドメインを触る他のテストはない
        let _participant =
            unsafe { DdsParticipant::create(Some(DDS_PARTICIPANT_TEST_CREATE), Some(qos), None) }
                .expect("failed to create participant");
    }

    /// 同じドメインIDへのget_or_createが常に同じインスタンスを返すことを確認する
    #[test]
    fn test_get_or_create_is_singleton() {
        let first =
            DdsParticipant::get_or_create(Some(DDS_PARTICIPANT_TEST_GET_OR_CREATE)).unwrap();
        let second =
            DdsParticipant::get_or_create(Some(DDS_PARTICIPANT_TEST_GET_OR_CREATE)).unwrap();
        assert!(std::ptr::eq(first, second));
        assert_eq!(first.guid(), second.guid());
        assert_eq!(
            first.domain_id().unwrap(),
            DDS_PARTICIPANT_TEST_GET_OR_CREATE
        );
    }

    /// 既に共有参加者がある状態でQoS/Listenerを指定すると、黙って無視せず
    /// Err(PreconditionNotMet)を返すことを確認する
    #[test]
    fn test_get_or_create_with_returns_err_for_existing_participant_with_args() {
        let _first =
            DdsParticipant::get_or_create(Some(DDS_PARTICIPANT_TEST_GET_OR_CREATE)).unwrap();
        let qos = DdsQos::create().unwrap();
        let result = DdsParticipant::get_or_create_with(
            Some(DDS_PARTICIPANT_TEST_GET_OR_CREATE),
            Some(qos),
            None,
        );
        assert_eq!(result.err(), Some(DDSError::PreconditionNotMet));
    }

    /// 複数スレッドから同時にget_or_createしても1インスタンスに収束することを確認する
    #[test]
    fn test_get_or_create_is_singleton_concurrent() {
        const THREADS: usize = 8;
        let handles = (0..THREADS)
            .map(|_| {
                std::thread::spawn(|| {
                    DdsParticipant::get_or_create(Some(DDS_PARTICIPANT_TEST_GET_OR_CREATE))
                        .unwrap()
                        .guid()
                })
            })
            .collect::<Vec<_>>();
        let guids = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>();
        assert!(guids.windows(2).all(|w| w[0] == w[1]), "{:?}", guids);
    }
}
