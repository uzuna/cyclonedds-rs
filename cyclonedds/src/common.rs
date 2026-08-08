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

use cyclonedds_sys::DdsEntity;
use std::any::Any;
use std::sync::Arc;

/// 親エンティティを子より長生きさせるための型消去ハンドル
#[derive(Clone, Default)]
pub struct Keepalive(#[allow(dead_code)] Option<Arc<dyn Any + Send + Sync>>);

impl Keepalive {
    pub(crate) fn shared<T: Any + Send + Sync>(inner: Arc<T>) -> Self {
        Self(Some(inner))
    }
}

/// An entity on which you can attach a DdsWriter
pub trait DdsWritable {
    fn entity(&self) -> &DdsEntity;

    fn keepalive(&self) -> Keepalive {
        Keepalive::default()
    }
}

/// An entity on which you can attach a DdsReader
pub trait DdsReadable {
    fn entity(&self) -> &DdsEntity;

    fn keepalive(&self) -> Keepalive {
        Keepalive::default()
    }
}

pub trait Entity {
    fn entity(&self) -> &DdsEntity;
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use crate::dds_domain::DdsDomain;
    use crate::*;
    use cdds_derive::Topic;
    use serde::{Deserialize, Serialize};

    const CYCLONE_SHM_CONFIG: &str = r###"<?xml version="1.0" encoding="UTF-8" ?>
    <CycloneDDS xmlns="https://cdds.io/config"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="https://cdds.io/config https://raw.githubusercontent.com/eclipse-cyclonedds/cyclonedds/iceoryx/etc/cyclonedds.xsd">
        <Domain id="any">
            <SharedMemory>
                <Enable>true</Enable>
                <LogLevel>info</LogLevel>
            </SharedMemory>
        </Domain>
    </CycloneDDS>"###;

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

    /// 共有メモリを有効化したテスト用ドメインを作成する
    pub fn create_shm_domain(domain: cyclonedds_sys::DdsDomainId) -> Result<DdsDomain, DDSError> {
        DdsDomain::create(domain, Some(CYCLONE_SHM_CONFIG))
    }

    /// loopback のみを使用するテスト用ドメインを作成する
    pub fn create_loopback_domain(
        domain: cyclonedds_sys::DdsDomainId,
    ) -> Result<DdsDomain, DDSError> {
        DdsDomain::create(domain, Some(CYCLONE_LOOPBACK_CONFIG))
    }

    /// Fixedではない型のテストデータ型
    #[derive(Debug, PartialEq, Serialize, Deserialize, Topic)]
    pub struct TestTypeAlloc {
        a: u32,
        b: f32,
        s: String,
    }

    impl Default for TestTypeAlloc {
        fn default() -> Self {
            Self {
                a: 42,
                b: 3.14,
                s: "Hello, world!".to_string(),
            }
        }
    }

    impl TestTypeAlloc {
        pub fn new(a: u32, b: f32, s: impl Into<String>) -> Self {
            Self { a, b, s: s.into() }
        }

        pub fn samples(count: usize) -> impl std::iter::Iterator<Item = Arc<Self>> {
            (0..count).map(|i| {
                std::sync::Arc::new(Self::new(i as u32, i as f32 * 1.1, i.to_string().repeat(i)))
            })
        }

        /// 任意の大きさのバッファを含むサンプルを作成する
        /// 特に大きなバッファの転送のテストに使う
        pub fn sized_sample(id: u32, size: usize) -> Arc<Self> {
            // データの破損がわかるように、0からzまでの文字の順番で指定されたサイズの文字列を作成する
            let step = size / 36;
            let mut s = String::with_capacity(size);
            for i in 0..36 {
                s.push_str(
                    &(std::char::from_u32(b'0' as u32 + i as u32)
                        .unwrap()
                        .to_string()
                        .repeat(step)),
                );
            }
            Arc::new(Self::new(id, id as f32 * 1.1, s))
        }
    }
}
