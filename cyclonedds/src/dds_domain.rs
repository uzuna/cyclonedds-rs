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
use cyclonedds_sys::{DdsDomainId, DdsEntity, dds_error::DDSError};
use std::convert::From;
use std::ffi::CString;
use std::sync::LazyLock;
use tracing::error;

/// プロセス寿命で共有するDomainの登録簿
static SHARED_DOMAINS: LazyLock<SharedRegistry<DdsDomain>> = LazyLock::new(SharedRegistry::new);

pub struct DdsDomain(DdsEntity, Option<String>);

impl DdsDomain {
    /// 所有権を持つドメインを作成する(dropでドメインを解体する)
    ///
    /// # Safety
    /// dropするとそのドメインの内部状態が解体されるため、`DdsParticipant::create`と
    /// 同じレースを持つ。他スレッドが同じドメインの参加者を生成/使用している最中に
    /// dropすると解放済みメモリを参照してSEGVする
    /// (再現手順とスタックトレースは`docs/domain-lifecycle.md`参照)。
    ///
    /// 呼び出し側は、同じドメインIDに対する生成・破棄・使用がプロセス内で
    /// 決して並行しないことを保証すること。ドメインの生成/破棄ライフサイクル自体を
    /// 検証したい場合を除き、安全な[`DdsDomain::get_or_create`]を使うこと。
    pub unsafe fn create(domain: DdsDomainId, config: Option<&str>) -> Result<Self, DDSError> {
        unsafe {
            let config_cstring =
                config.map(|cfg| CString::new(cfg).expect("Unable to create new config string"));
            let config_ptr = config_cstring
                .as_ref()
                .map_or(std::ptr::null(), |c| c.as_ptr());
            let d = cyclonedds_sys::dds_create_domain(domain, config_ptr);
            // negative return value signify an error
            if d > 0 {
                Ok(DdsDomain(DdsEntity::new(d), config.map(str::to_string)))
            } else {
                Err(DDSError::from(d))
            }
        }
    }

    /// この共有インスタンスを生成した際に渡された`config`
    fn config(&self) -> Option<&str> {
        self.1.as_deref()
    }

    /// 指定ドメインの共有インスタンスを取得する(初回のみ生成される)
    ///
    /// [`DdsDomain::create`]のsafeな代替。ドメインIDごとに1つだけ生成し、プロセス終了まで
    /// dropしない(意図的にリークする)ことで、上記のレースを構造的に回避する。
    ///
    /// 注意点:
    /// - **`config`は最初の生成時のみ実際に使われる**。既に共有インスタンスがある場合、
    ///   `config`が`None`か生成時と同じ文字列であれば既存のものを返すが、
    ///   異なる文字列を指定した場合は`Err(DDSError::PreconditionNotMet)`を返す
    ///   (黙って無視すると「設定したつもりが反映されていない」ことに気付けないため)。
    /// - **そのドメインに初めて触る側でなければ失敗する**。cycloneddsの
    ///   `dds_create_domain`は、既にドメインが存在する場合(参加者が暗黙的に作った場合も
    ///   含む)`PreconditionNotMet`を返すため、参加者を作るより先に呼ぶ必要がある。
    /// - 明示的な設定が要らないなら、この関数ではなく
    ///   [`crate::DdsParticipant::get_or_create`]だけを使えばよい。
    ///
    /// # Errors
    /// そのドメインに初めて触る側でない場合、または既存インスタンスと異なる`config`を
    /// 指定した場合は`DDSError::PreconditionNotMet`を返す。
    pub fn get_or_create(
        domain: DdsDomainId,
        config: Option<&str>,
    ) -> Result<&'static Self, DDSError> {
        let shared = SHARED_DOMAINS.get_or_create(
            domain,
            // SAFETY: 生成は登録簿のロックで直列化され、作ったドメインはリークして
            // 保持されるかdropされるかのどちらかだが、dropするのは「同じIDの共有ドメインが
            // 既にある」場合だけなので、生きているドメインを解体することはない
            || unsafe { Self::create(domain, config) },
            // ドメインIDは要求時点で確定している(`DDS_DOMAIN_DEFAULT`は`dds_create_domain`が
            // `BadParameter`で弾く)
            |_| Ok(domain),
        )?;
        match shared {
            Shared::Created(domain) => Ok(domain),
            Shared::Existing(existing) => match config {
                None => Ok(existing),
                Some(requested) if Some(requested) == existing.config() => Ok(existing),
                Some(_) => Err(DDSError::PreconditionNotMet),
            },
        }
    }
}

impl PartialEq for DdsDomain {
    fn eq(&self, other: &Self) -> bool {
        unsafe { self.0.entity() == other.0.entity() }
    }
}

impl Eq for DdsDomain {}

impl Drop for DdsDomain {
    fn drop(&mut self) {
        unsafe {
            let ret: DDSError = cyclonedds_sys::dds_delete(self.0.entity()).into();
            if DDSError::DdsOk != ret {
                // Why not panic: unwinding中のdropでpanicするとabortしてしまう
                // (詳細は`DdsParticipant`のDrop実装のコメント参照)
                error!("Ignoring dds_delete failure for DdsDomain: {}", ret);
            }
        }
    }
}

#[cfg(test)]
mod dds_domain_tests {
    use crate::common::TestDomain;
    use crate::dds_domain::DdsDomain;
    use cyclonedds_sys::DDSError;

    /// 不正な設定XMLではドメインが作られないことを確認する
    #[test]
    fn test_create_domain_with_bad_config() {
        let result = DdsDomain::get_or_create(TestDomain::DomainBadConfig.id(), Some("blah"));
        assert!(result.is_err());
    }

    /// 同じドメインIDへのget_or_createが常に同じインスタンスを返すことを確認する
    #[test]
    fn test_get_or_create_is_singleton() {
        let first = DdsDomain::get_or_create(TestDomain::DomainGetOrCreate.id(), None).unwrap();
        let second = DdsDomain::get_or_create(TestDomain::DomainGetOrCreate.id(), None).unwrap();
        assert!(std::ptr::eq(first, second));
    }

    const MINIMAL_CONFIG: &str = r###"<?xml version="1.0" encoding="UTF-8" ?>
    <CycloneDDS xmlns="https://cdds.io/config">
        <Domain id="any" />
    </CycloneDDS>"###;

    /// 既存インスタンスと同じconfig文字列を指定した場合は既存を返すことを確認する
    #[test]
    fn test_get_or_create_returns_existing_for_same_config() {
        let first =
            DdsDomain::get_or_create(TestDomain::DomainSameConfig.id(), Some(MINIMAL_CONFIG))
                .unwrap();
        let second =
            DdsDomain::get_or_create(TestDomain::DomainSameConfig.id(), Some(MINIMAL_CONFIG))
                .unwrap();
        assert!(std::ptr::eq(first, second));
    }

    /// 既存インスタンス(config省略で生成)があるのに異なるconfigを指定すると
    /// Err(PreconditionNotMet)を返すことを確認する
    #[test]
    fn test_get_or_create_returns_err_for_different_config() {
        let _first = DdsDomain::get_or_create(TestDomain::DomainConfigMismatch.id(), None).unwrap();
        let result =
            DdsDomain::get_or_create(TestDomain::DomainConfigMismatch.id(), Some(MINIMAL_CONFIG));
        assert_eq!(result.err(), Some(DDSError::PreconditionNotMet));
    }
}
