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

use cyclonedds_sys::{dds_qos_t, *};
use std::convert::From;
use std::mem::MaybeUninit;
use std::num::NonZeroU16;
use std::time::Duration;
use std::{clone::Clone, fmt::Debug};
use tracing::warn;

pub use cyclonedds_sys::{
    dds_destination_order_kind, dds_durability_kind, dds_duration_t, dds_history_kind,
    dds_ignorelocal_kind, dds_liveliness_kind, dds_ownership_kind,
    dds_presentation_access_scope_kind, dds_reliability_kind,
};

/// Safety Check:
/// The dds_qos_t pointer is not accessible externally. I'm assuming the Qos structure created
/// by Cyclone is Sendable here.
unsafe impl Send for DdsQos {}

pub struct DdsQos(*mut dds_qos_t);

/// DDSの「無限」を表すduration (`DDS_INFINITY`, dds/ddsrt/time.h では `INT64_MAX`)
pub const DDS_INFINITY: dds_duration_t = i64::MAX;

/// [`Duration`]をDDSのナノ秒duration(i64)に変換する
///
/// `as i64`で素朴に変換すると、i64ナノ秒の上限(約292年)を超える値が
/// ラップアラウンドして**負値**になる(例: `Duration::MAX` → `-1`)。
/// DDSの負のdurationは不正値で、失敗はQoSセット時ではなくエンティティ生成時に
/// `BAD_PARAMETER`として遅れて現れるため、飽和させて`DDS_INFINITY`に丸める
fn to_dds_duration(duration: Duration) -> dds_duration_t {
    duration.as_nanos().min(DDS_INFINITY as u128) as dds_duration_t
}

/// DDSのナノ秒duration(i64)を[`Duration`]に変換する
///
/// `as u64`で素朴に変換すると、負値が巨大な正の値に化けて
/// 数百年の[`Duration`]になる。負のdurationはDDSでは不正値なので0に丸める
fn from_dds_duration(duration: dds_duration_t) -> Duration {
    Duration::from_nanos(duration.max(0) as u64)
}

/// `KEEP_LAST`のdepthとして有効な値かを検証する
///
/// cycloneddsの`dds_qset_history`は検証しないため、不正値はエンティティ生成時の
/// `BAD_PARAMETER`として遅れて現れる。設定した箇所で気付けるよう、同じ`BadParameter`を
/// その場で返す。
///
/// Why not panic: QoSは設定ファイルやCLI引数から組み立てられることがあり、
/// 不正値は呼び出し側が回復すべき入力エラーであってプログラムのバグとは限らない
fn validate_history(history: dds_history_kind, depth: i32) -> Result<(), DDSError> {
    if history == dds_history_kind::DDS_HISTORY_KEEP_LAST && depth <= 0 {
        return Err(DDSError::BadParameter);
    }
    Ok(())
}

impl DdsQos {
    pub fn create() -> Result<Self, DDSError> {
        unsafe {
            let p = cyclonedds_sys::dds_create_qos();
            if !p.is_null() {
                Ok(DdsQos(p))
            } else {
                Err(DDSError::OutOfResources)
            }
        }
    }

    pub fn merge(&mut self, src: &Self) {
        unsafe {
            dds_merge_qos(self.0, src.0);
        }
    }

    pub fn set_durability(&mut self, durability: dds_durability_kind) -> &mut Self {
        unsafe {
            dds_qset_durability(self.0, durability);
        }
        self
    }

    /// 信頼性のための再送用バッファのサイズを設定する
    ///
    /// 同期のための履歴保持は [Self::set_durability_service] を利用すること
    ///
    /// # Errors
    /// `KEEP_LAST`で`depth <= 0`を指定した場合(DDS的に不正な組み合わせ)は
    /// [`DDSError::BadParameter`]を返す
    pub fn set_history(
        &mut self,
        history: dds_history_kind,
        depth: i32,
    ) -> Result<&mut Self, DDSError> {
        validate_history(history, depth)?;
        unsafe {
            dds_qset_history(self.0, history, depth);
        }
        Ok(self)
    }

    pub fn set_resource_limits(
        &mut self,
        max_samples: i32,
        max_instances: i32,
        max_samples_per_instance: i32,
    ) -> &mut Self {
        unsafe {
            dds_qset_resource_limits(self.0, max_samples, max_instances, max_samples_per_instance);
        }
        self
    }

    pub fn set_presentation(
        &mut self,
        access_scope: dds_presentation_access_scope_kind,
        coherent_access: bool,
        ordered_access: bool,
    ) -> &mut Self {
        unsafe {
            dds_qset_presentation(self.0, access_scope, coherent_access, ordered_access);
        }
        self
    }

    pub fn set_lifespan(&mut self, lifespan: std::time::Duration) -> &mut Self {
        unsafe {
            dds_qset_lifespan(self.0, to_dds_duration(lifespan));
        }
        self
    }

    pub fn set_deadline(&mut self, deadline: std::time::Duration) -> &mut Self {
        unsafe {
            dds_qset_deadline(self.0, to_dds_duration(deadline));
        }
        self
    }

    pub fn set_latency_budget(&mut self, duration: dds_duration_t) -> &mut Self {
        unsafe {
            dds_qset_latency_budget(self.0, duration);
        }
        self
    }

    pub fn set_ownership(&mut self, kind: dds_ownership_kind) -> &mut Self {
        unsafe {
            dds_qset_ownership(self.0, kind);
        }
        self
    }

    pub fn set_ownership_strength(&mut self, value: i32) -> &mut Self {
        unsafe {
            dds_qset_ownership_strength(self.0, value);
        }
        self
    }

    pub fn set_liveliness(
        &mut self,
        kind: dds_liveliness_kind,
        lease_duration: dds_duration_t,
    ) -> &mut Self {
        unsafe {
            dds_qset_liveliness(self.0, kind, lease_duration);
        }
        self
    }

    pub fn set_time_based_filter(&mut self, minimum_separation: dds_duration_t) -> &mut Self {
        unsafe {
            dds_qset_time_based_filter(self.0, minimum_separation);
        }
        self
    }

    pub fn set_reliability(
        &mut self,
        kind: dds_reliability_kind,
        max_blocking_time: std::time::Duration,
    ) -> &mut Self {
        unsafe {
            dds_qset_reliability(self.0, kind, to_dds_duration(max_blocking_time));
        }
        self
    }

    pub fn set_transport_priority(&mut self, value: i32) -> &mut Self {
        unsafe {
            dds_qset_transport_priority(self.0, value);
        }
        self
    }

    pub fn set_destination_order(&mut self, kind: dds_destination_order_kind) -> &mut Self {
        unsafe {
            dds_qset_destination_order(self.0, kind);
        }
        self
    }

    pub fn set_writer_data_lifecycle(&mut self, autodispose: bool) -> &mut Self {
        unsafe {
            dds_qset_writer_data_lifecycle(self.0, autodispose);
        }
        self
    }

    pub fn set_reader_data_lifecycle(
        &mut self,
        autopurge_nowriter_samples_delay: dds_duration_t,
        autopurge_disposed_samples_delay: dds_duration_t,
    ) -> &mut Self {
        unsafe {
            dds_qset_reader_data_lifecycle(
                self.0,
                autopurge_nowriter_samples_delay,
                autopurge_disposed_samples_delay,
            );
        }
        self
    }

    /// 同期（あとから参加しても過去配信データを受信できる）に関わるQoS設定。
    ///
    /// CycloneDDSでは、`TRANSIENT_LOCAL` の永続性レベルにおいて、あとから参加した
    /// Reader向けの履歴保持設定をこのQoSで行う。一般的な解釈では [Self::set_history] に
    /// 期待される機能を、こちらで担っている。
    /// OMGのDCPS QoS定義では、`TRANSIENT` または `PERSISTENT` の永続性レベルにおいて、
    /// データを管理する「仮想的なサービス」の設定として定義されている。
    ///
    /// この仕様差は、CycloneDDSメンテナが `DURABILITY_SERVICE` QoS を
    /// 「接続の確立時（または再確立時）のデータ同期」の設定、`HISTORY` QoS を
    /// 「接続確立後の再送用バッファサイズ」の設定として、意図的に使い分けているためであり、
    /// ライブデータの全数配信のためのKEEP_ALLで保証しつつ、
    /// 後から参加者には直近n件のみといった実用上有用な設定をサポートできる設計となっている。
    ///
    /// Reference: <https://github.com/eclipse-cyclonedds/cyclonedds/issues/49>
    ///
    /// # Errors
    /// `history_kind`が`KEEP_LAST`で`history_depth <= 0`の場合は
    /// [`DDSError::BadParameter`]を返す
    pub fn set_durability_service(
        &mut self,
        service_cleanup_delay: Duration,
        history_kind: dds_history_kind,
        history_depth: i32,
        max_samples: i32,
        max_instances: i32,
        max_samples_per_instance: i32,
    ) -> Result<&mut Self, DDSError> {
        validate_history(history_kind, history_depth)?;
        unsafe {
            dds_qset_durability_service(
                self.0,
                to_dds_duration(service_cleanup_delay),
                history_kind,
                history_depth,
                max_samples,
                max_instances,
                max_samples_per_instance,
            );
        }
        Ok(self)
    }

    pub fn set_ignorelocal(&mut self, ignore: dds_ignorelocal_kind) -> &mut Self {
        unsafe {
            dds_qset_ignorelocal(self.0, ignore);
        }
        self
    }

    pub fn set_partition(&mut self, name: &std::ffi::CStr) -> &mut Self {
        unsafe { dds_qset_partition1(self.0, name.as_ptr()) }
        self
    }

    /// マッチングに影響しない付帯メタデータ(bytes)を設定する。
    ///
    /// Subscription/PublicationのbuiltinトピックData(DCPSSubscription/DCPSPublication)に
    /// 乗って配信されるため、他参加者からも読み取れる。
    /// 空のデータを渡した場合は未設定と同様に扱われる。
    pub fn set_userdata(&mut self, data: &[u8]) -> &mut Self {
        unsafe {
            let ptr: *const std::ffi::c_void = if data.is_empty() {
                std::ptr::null()
            } else {
                data.as_ptr() as *const std::ffi::c_void
            };
            dds_qset_userdata(self.0, ptr, data.len());
        }
        self
    }

    // 以下のgetterが`Option`を返す理由:
    // cycloneddsの`dds_qget_*`は該当policyが未設定なら出力先に一切書かずfalseを返す。
    // 戻り値を見ずに`assume_init`すると未初期化メモリを読むことになり、
    // `dds_*_kind`はrustified enumなので不正なdiscriminantの生成という即時UBになる

    /// `DURABILITY_SERVICE`の履歴設定を得る(あとから参加したreaderへ同期する件数)
    ///
    /// 未設定の場合は`None`を返す。DDS既定の`KEEP_LAST(1)`で代替しないのは、
    /// 「未設定」と「明示的に`KEEP_LAST(1)`を設定した」を呼び出し側が区別できなくなるため
    pub fn durability_service(&self) -> Option<(dds_history_kind, i32)> {
        let mut kind = MaybeUninit::<dds_history_kind>::uninit();
        let mut depth = MaybeUninit::<i32>::uninit();
        unsafe {
            if !dds_qget_durability_service(
                self.0,
                std::ptr::null_mut(),
                kind.as_mut_ptr(),
                depth.as_mut_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            ) {
                return None;
            }
            Some((kind.assume_init(), depth.assume_init()))
        }
    }

    /// 未設定の場合は`None`を返す
    pub fn durability(&self) -> Option<dds_durability_kind> {
        let mut kind = MaybeUninit::<dds_durability_kind>::uninit();
        unsafe {
            if !dds_qget_durability(self.0, kind.as_mut_ptr()) {
                return None;
            }
            Some(kind.assume_init())
        }
    }

    /// 未設定の場合は`None`を返す
    pub fn history(&self) -> Option<(dds_history_kind, i32)> {
        unsafe {
            let mut depth = 0;
            let mut kind = MaybeUninit::<dds_history_kind>::uninit();
            if !dds_qget_history(self.0, kind.as_mut_ptr(), &mut depth as *mut i32) {
                return None;
            }
            Some((kind.assume_init(), depth))
        }
    }

    /// 未設定の場合は`None`を返す
    pub fn reliability(&self) -> Option<(dds_reliability_kind, Duration)> {
        unsafe {
            let mut max_blocking_time = MaybeUninit::<dds_duration_t>::uninit();
            let mut kind = MaybeUninit::<dds_reliability_kind>::uninit();
            if !dds_qget_reliability(self.0, kind.as_mut_ptr(), max_blocking_time.as_mut_ptr()) {
                return None;
            }
            Some((
                kind.assume_init(),
                from_dds_duration(max_blocking_time.assume_init()),
            ))
        }
    }

    /// 未設定の場合は`None`を返す
    pub fn lifespan(&self) -> Option<Duration> {
        unsafe {
            let mut lifespan = MaybeUninit::<dds_duration_t>::uninit();
            if !dds_qget_lifespan(self.0, lifespan.as_mut_ptr()) {
                return None;
            }
            Some(from_dds_duration(lifespan.assume_init()))
        }
    }

    /// 未設定の場合は`None`を返す
    pub fn deadline(&self) -> Option<Duration> {
        unsafe {
            let mut deadline = MaybeUninit::<dds_duration_t>::uninit();
            if !dds_qget_deadline(self.0, deadline.as_mut_ptr()) {
                return None;
            }
            Some(from_dds_duration(deadline.assume_init()))
        }
    }

    pub fn userdata(&self) -> Option<Vec<u8>> {
        unsafe {
            let mut value: *mut std::ffi::c_void = std::ptr::null_mut();
            let mut sz: usize = 0;
            let ok = dds_qget_userdata(self.0, &mut value, &mut sz);
            let result = if ok && !value.is_null() && sz > 0 {
                Some(std::slice::from_raw_parts(value as *const u8, sz).to_vec())
            } else {
                None
            };
            if !value.is_null() {
                dds_free(value);
            }
            result
        }
    }

    /// 未設定の場合は`None`を返す
    pub fn liveliness(&self) -> Option<(dds_liveliness_kind, Duration)> {
        unsafe {
            let mut lease_duration = MaybeUninit::<dds_duration_t>::uninit();
            let mut kind = MaybeUninit::<dds_liveliness_kind>::uninit();
            if !dds_qget_liveliness(self.0, kind.as_mut_ptr(), lease_duration.as_mut_ptr()) {
                return None;
            }
            Some((
                kind.assume_init(),
                from_dds_duration(lease_duration.assume_init()),
            ))
        }
    }

    // 内部でポインタからDdsQosを作成する
    pub(crate) fn from_ptr(ptr: *mut dds_qos_t) -> Self {
        DdsQos(ptr)
    }

    // 借用したQosのポインタは開放しない
    fn forget(mut self) {
        self.0 = std::ptr::null_mut();
    }
}

impl Default for DdsQos {
    fn default() -> Self {
        DdsQos::create().expect("Unable to create DdsQos")
    }
}

impl Drop for DdsQos {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { dds_delete_qos(self.0) }
        }
    }
}

impl PartialEq for DdsQos {
    fn eq(&self, other: &Self) -> bool {
        unsafe { dds_qos_equal(self.0, other.0) }
    }
}

impl Eq for DdsQos {}

impl Clone for DdsQos {
    fn clone(&self) -> Self {
        unsafe {
            let q = dds_create_qos();
            let err: DDSError = dds_copy_qos(q, self.0).into();
            if let DDSError::DdsOk = err {
                DdsQos(q)
            } else {
                dds_delete_qos(q);
                panic!("dds_copy_qos failed. Panicing as Clone should not fail");
            }
        }
    }
}

impl From<DdsQos> for *const dds_qos_t {
    fn from(mut qos: DdsQos) -> Self {
        let q = qos.0;
        // we need to forget the pointer here
        qos.0 = std::ptr::null_mut();
        // setting to zero will ensure drop will not deallocate it
        q
    }
}

impl Debug for DdsQos {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("DdsQos")
            .field("durability", &self.durability())
            .field("history", &self.history())
            .field("reliability", &self.reliability())
            .field("lifespan", &self.lifespan())
            .field("deadline", &self.deadline())
            .field("liveliness", &self.liveliness())
            .finish()
    }
}
/*
impl From<&mut DdsQos> for *const dds_qos_t {
    fn from(qos: &mut DdsQos) -> Self {
        let q = qos.0;
        // we need to forget the pointer here
        qos.0 = std::ptr::null_mut();
        // setting to zero will ensure drop will not deallocate it
        q
    }
}
*/

/// メッセージ履歴設定
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum History {
    /// 指定された数だけ過去のサンプルを保持する
    /// 想定インスタンス数はPolicy::SUPPORT_INSTANCESに依存する
    KeepLast(i32),
    /// すべてのサンプルを保持する
    KeepAll,
}

impl Default for History {
    fn default() -> Self {
        History::KeepLast(1)
    }
}

/// 到達保証設定
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Reliability {
    /// 信頼性あり。最大ブロッキング時間を指定する
    Reliable(Duration),
    /// ベストエフォート
    #[default]
    BestEffort,
}

/// 永続性設定
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Durability {
    /// Readerが起動している間のみデータを保持する
    #[default]
    Volatile,
    /// データをローカルに保存し、後から起動したReaderにも配信する
    TransientLocal {
        /// あとから参加したreaderへ同期する件数
        ///
        /// [`History`]とは別軸で、CycloneDDSでは前者を`DURABILITY_SERVICE`、後者
        /// (マッチ成立後の再送バッファ)を`HISTORY`が決める
        /// ([`DdsQos::set_durability_service`]を参照)。
        ///
        /// Why 非ゼロ有界: 0はDDSとして不正な値であり、無制限(`KEEP_ALL`)にすると
        /// 全readerがackしてもwriter側の履歴が解放されず際限なく増える。
        /// どちらも作れないようにして、送信側が持ち続ける量を必ず有限にする
        sync_depth: NonZeroU16,
    },
}

/// 通信可否に関わる重要なQoS要素のみをまとめた構造体
///
/// QoSは実際には効果のない設定があり設定が煩雑で、
/// インスタンスに紐づく情報が含まれるため比較が難しいため
/// 実用上の比較や設定はこちらを利用することを推奨する
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Policy {
    pub history: History,
    pub reliability: Reliability,
    pub durability: Durability,
}

impl Policy {
    const SUPPORT_INSTANCES: i32 = 4;
    /// iceoryxがpublisherごとに保持できる履歴の上限(`iox_cfg_max_publisher_history()`の既定値)
    ///
    /// `DURABILITY_SERVICE`の深さがこれを超えると、cycloneddsは`dds_writer_supports_shm`で
    /// ゼロコピー経路を無効化する。エラーもログも出ないので、超えたことを利用者へ伝える必要がある
    const IOX_MAX_PUBLISHER_HISTORY: i32 = 16;
    /// # Errors
    /// `history`(KEEP_LASTのdepth)が`1..=u16::MAX`の外、つまり0以下または`u16::MAX`超の場合は
    /// [`DDSError::BadParameter`]を返す。上限は同期件数を載せる[`Durability::TransientLocal`]の
    /// `sync_depth`が`NonZeroU16`であることに由来する
    pub fn create_transient_local(
        history: i32,
        deadline: Option<Duration>,
    ) -> Result<Self, DDSError> {
        validate_history(dds_history_kind::DDS_HISTORY_KEEP_LAST, history)?;
        Ok(Policy {
            history: History::KeepLast(history),
            reliability: Reliability::Reliable(deadline.unwrap_or(Duration::from_millis(100))),
            durability: Durability::TransientLocal {
                sync_depth: Self::sync_depth(history)?,
            },
        })
    }

    /// `history`件をlate joinerへも同期する設定として[`Durability::TransientLocal`]に載せる
    ///
    /// # Errors
    /// `NonZeroU16`に収まらない値は[`DDSError::BadParameter`]を返す
    fn sync_depth(history: i32) -> Result<NonZeroU16, DDSError> {
        u16::try_from(history)
            .ok()
            .and_then(NonZeroU16::new)
            .ok_or(DDSError::BadParameter)
    }

    /// このPolicyに対応する[`DdsQos`]を組み立てる
    ///
    /// # Errors
    /// [`Policy`]はpublicフィールドを持つため`History::KeepLast(0)`のような不正な値も
    /// 構築できる。その場合は[`DDSError::BadParameter`]を返す
    pub fn to_qos(&self) -> Result<DdsQos, DDSError> {
        let mut qos = DdsQos::create()?;
        // History
        match self.history {
            History::KeepLast(depth) => {
                qos.set_history(dds_history_kind::DDS_HISTORY_KEEP_LAST, depth)?;
                // depthが大きいとi32を溢れて負のリソース上限になるため飽和させる
                let max_sample = depth.saturating_mul(Self::SUPPORT_INSTANCES);
                qos.set_resource_limits(max_sample, Self::SUPPORT_INSTANCES, depth);
            }
            History::KeepAll => {
                qos.set_history(dds_history_kind::DDS_HISTORY_KEEP_ALL, 0)?;
            }
        }
        // Reliability
        match self.reliability {
            Reliability::Reliable(max_blocking_time) => {
                qos.set_reliability(
                    dds_reliability_kind::DDS_RELIABILITY_RELIABLE,
                    max_blocking_time,
                );
            }
            Reliability::BestEffort => {
                qos.set_reliability(
                    dds_reliability_kind::DDS_RELIABILITY_BEST_EFFORT,
                    Duration::from_nanos(0),
                );
            }
        }
        // Durability
        match self.durability {
            Durability::Volatile => {
                qos.set_durability(dds_durability_kind::DDS_DURABILITY_VOLATILE);
            }
            Durability::TransientLocal { sync_depth } => {
                qos.set_durability(dds_durability_kind::DDS_DURABILITY_TRANSIENT_LOCAL);
                // あとから参加したreaderへ何件同期するかを決めるのはこのQoSで、`history`は
                // マッチ後の再送バッファにしか効かない。設定しないとDDS既定のKEEP_LAST(1)が
                // 残り、`history`に何を指定しても1件しか届かない
                let depth = i32::from(sync_depth.get());
                qos.set_durability_service(
                    Duration::ZERO,
                    dds_history_kind::DDS_HISTORY_KEEP_LAST,
                    depth,
                    depth.saturating_mul(Self::SUPPORT_INSTANCES),
                    Self::SUPPORT_INSTANCES,
                    depth,
                )?;
                if cfg!(feature = "shm") && depth > Self::IOX_MAX_PUBLISHER_HISTORY {
                    // cyclonedds側は黙ってゼロコピーを落とすだけで何も知らせないため、
                    // `to_qos()`を呼ぶたびに残す。`to_qos()`はreader/topic用のQoS生成にも
                    // 使われる(writer専用ではない)ため、writerに使う場合の影響として書く。
                    // `shm`featureは既定有効なため、CycloneDDS設定側で共有メモリ自体を
                    // 無効化している利用者にも出うるが、その設定はRust側から観測できないため許容する
                    warn!(
                        sync_depth = depth,
                        limit = Self::IOX_MAX_PUBLISHER_HISTORY,
                        "TransientLocalの同期件数がiceoryxの上限を超えるため、\
                         このQoSをwriterに使うと共有メモリのゼロコピー経路が使われない"
                    );
                }
                if self.reliability == Reliability::BestEffort {
                    // TransientLocal で BestEffort は非推奨なので Reliable に変更する
                    qos.set_reliability(
                        dds_reliability_kind::DDS_RELIABILITY_RELIABLE,
                        Duration::from_millis(100),
                    );
                }
            }
        }
        Ok(qos)
    }
}

impl From<&DdsQos> for Policy {
    fn from(qos: &DdsQos) -> Self {
        // 未設定のpolicyはDDSの既定値と同じ意味なので、各型のDefaultに落とす
        let history = match qos.history() {
            Some((dds_history_kind::DDS_HISTORY_KEEP_LAST, depth)) => History::KeepLast(depth),
            Some((dds_history_kind::DDS_HISTORY_KEEP_ALL, _)) => History::KeepAll,
            None => History::default(),
        };
        let reliability = match qos.reliability() {
            Some((dds_reliability_kind::DDS_RELIABILITY_RELIABLE, max_blocking_time)) => {
                Reliability::Reliable(max_blocking_time)
            }
            Some((dds_reliability_kind::DDS_RELIABILITY_BEST_EFFORT, _)) => Reliability::BestEffort,
            None => Reliability::default(),
        };
        let durability = match qos.durability() {
            Some(dds_durability_kind::DDS_DURABILITY_VOLATILE) | None => Durability::Volatile,
            // `Durability`は無制限の同期を表現できないため、KEEP_ALLや範囲外の深さは
            // 表現可能な最大値へ丸める。`Policy`は元から要素を絞った要約なので情報は落ちる
            Some(_) => Durability::TransientLocal {
                sync_depth: match qos.durability_service() {
                    // `depth <= 0`は他ベンダや不正なdiscoveryデータ由来でしか来ないが、
                    // 「無制限」ではなく単なる不正値なのでDDS既定の1へ倒す。
                    // MAXへ丸めると少なすぎる値が65535件保持のwriterに化けて危険側になる
                    Some((dds_history_kind::DDS_HISTORY_KEEP_LAST, depth)) if depth <= 0 => {
                        NonZeroU16::MIN
                    }
                    Some((dds_history_kind::DDS_HISTORY_KEEP_LAST, depth)) => {
                        Policy::sync_depth(depth).unwrap_or(NonZeroU16::MAX)
                    }
                    Some((dds_history_kind::DDS_HISTORY_KEEP_ALL, _)) => NonZeroU16::MAX,
                    // 未設定はDDS既定の`KEEP_LAST(1)`と同義であり、無制限ではない
                    None => NonZeroU16::MIN,
                },
            },
        };
        Policy {
            history,
            reliability,
            durability,
        }
    }
}

impl From<*mut dds_qos_t> for Policy {
    fn from(qos: *mut dds_qos_t) -> Self {
        let q = DdsQos::from_ptr(qos);
        let p = Policy::from(&q);
        q.forget();
        p
    }
}

impl From<*const dds_qos_t> for Policy {
    fn from(qos: *const dds_qos_t) -> Self {
        Self::from(qos as *mut dds_qos_t)
    }
}

#[cfg(test)]
mod dds_qos_tests {
    use super::*;

    /// i64ナノ秒を超えるDurationがラップアラウンドして負値にならないことを確認する
    #[test]
    fn test_to_dds_duration_saturates() {
        assert_eq!(to_dds_duration(Duration::ZERO), 0);
        assert_eq!(
            to_dds_duration(Duration::from_millis(100)),
            100_000_000,
            "通常の値はそのままナノ秒になる"
        );
        // i64ナノ秒の上限は約292年
        assert_eq!(
            to_dds_duration(Duration::from_secs(9_223_372_036)),
            9_223_372_036_000_000_000
        );
        assert_eq!(
            to_dds_duration(Duration::from_secs(9_223_372_037)),
            DDS_INFINITY
        );
        assert_eq!(to_dds_duration(Duration::MAX), DDS_INFINITY);
    }

    /// 負のduration(DDSでは不正値)が巨大な正のDurationに化けないことを確認する
    #[test]
    fn test_from_dds_duration_clamps_negative() {
        assert_eq!(from_dds_duration(0), Duration::ZERO);
        assert_eq!(from_dds_duration(100_000_000), Duration::from_millis(100));
        assert_eq!(from_dds_duration(-1), Duration::ZERO);
        assert_eq!(from_dds_duration(i64::MIN), Duration::ZERO);
    }

    // Why: `dds_qget_*`は未設定policyでは出力先に書かずfalseを返す。戻り値を無視すると
    //      未初期化メモリの読み出しになり、rustified enumでは不正discriminantの生成でUBになる
    // Method: 未設定のQoSで全getterがNoneを返し、設定後は設定値がSomeで返ることを確認する
    #[test]
    fn test_qget_returns_none_when_policy_unset() {
        let mut qos = DdsQos::create().unwrap();
        let unset = (
            qos.durability(),
            qos.history(),
            qos.reliability(),
            qos.lifespan(),
            qos.deadline(),
            qos.liveliness(),
        );
        assert_eq!(unset, (None, None, None, None, None, None));

        qos.set_durability(dds_durability_kind::DDS_DURABILITY_VOLATILE)
            .set_history(dds_history_kind::DDS_HISTORY_KEEP_LAST, 3)
            .expect("KEEP_LAST(3) is valid")
            .set_reliability(
                dds_reliability_kind::DDS_RELIABILITY_RELIABLE,
                Duration::from_millis(100),
            )
            .set_lifespan(Duration::from_millis(200))
            .set_deadline(Duration::from_millis(300))
            .set_liveliness(
                dds_liveliness_kind::DDS_LIVELINESS_AUTOMATIC,
                to_dds_duration(Duration::from_millis(400)),
            );
        let set = (
            qos.durability(),
            qos.history(),
            qos.reliability(),
            qos.lifespan(),
            qos.deadline(),
            qos.liveliness(),
        );
        assert_eq!(
            set,
            (
                Some(dds_durability_kind::DDS_DURABILITY_VOLATILE),
                Some((dds_history_kind::DDS_HISTORY_KEEP_LAST, 3)),
                Some((
                    dds_reliability_kind::DDS_RELIABILITY_RELIABLE,
                    Duration::from_millis(100)
                )),
                Some(Duration::from_millis(200)),
                Some(Duration::from_millis(300)),
                Some((
                    dds_liveliness_kind::DDS_LIVELINESS_AUTOMATIC,
                    Duration::from_millis(400)
                )),
            )
        );
    }

    // Why: 未設定policyでも`Policy`は組み立てられる必要がある。以前は未初期化値をmatchしており
    //      分岐先が不定だった
    // Method: 空のQoSからのPolicyが全項目Defaultになることを確認する
    #[test]
    fn test_policy_from_unset_qos_falls_back_to_default() {
        let qos = DdsQos::create().unwrap();
        assert_eq!(Policy::from(&qos), Policy::default());
    }

    // Why: 不正なdepthはエンティティ生成時まで遅れて`BAD_PARAMETER`になるため
    //      設定した箇所で返す。ただしpanicではなく呼び出し側が回復できるErrにする
    // Method: KEEP_LASTに0以下を渡すとErr(BadParameter)になることを確認する
    #[test]
    fn test_set_history_keep_last_rejects_non_positive_depth() {
        let mut qos = DdsQos::create().unwrap();
        for depth in [0, -1] {
            assert_eq!(
                qos.set_history(dds_history_kind::DDS_HISTORY_KEEP_LAST, depth)
                    .err(),
                Some(DDSError::BadParameter),
                "depth={depth}"
            );
        }
    }

    #[test]
    fn test_set_history_keep_all_ignores_depth() {
        let mut qos = DdsQos::create().unwrap();
        qos.set_history(dds_history_kind::DDS_HISTORY_KEEP_ALL, 0)
            .expect("KEEP_ALLではdepthを見ない");
    }

    // Why: `Policy`はpublicフィールドを持ち`History::KeepLast(0)`も構築できてしまう。
    //      以前はここでpanicしており、docにも`# Panics`が無いため呼び出し側が防げなかった
    // Method: 不正なPolicyのto_qos()がpanicせずErrを返すことを確認する
    #[test]
    fn test_to_qos_rejects_invalid_history_without_panic() {
        let policy = Policy {
            history: History::KeepLast(0),
            ..Default::default()
        };
        assert_eq!(policy.to_qos().err(), Some(DDSError::BadParameter));
    }

    #[test]
    fn test_create_transient_local_rejects_out_of_range_history() {
        // 同期件数は`NonZeroU16`で表すため、0以下とu16を超える値は作れない
        let cases = [
            (0, Some(DDSError::BadParameter)),
            (-1, Some(DDSError::BadParameter)),
            (1, None),
            (i32::from(u16::MAX), None),
            (i32::from(u16::MAX) + 1, Some(DDSError::BadParameter)),
        ];
        let actual = cases
            .iter()
            .map(|&(history, _)| (history, Policy::create_transient_local(history, None).err()))
            .collect::<Vec<_>>();
        assert_eq!(actual, cases.to_vec());
    }

    // Why: `Durability`は無制限の同期を表現できない。KEEP_ALLや`NonZeroU16`に収まらない深さを
    //      そのまま扱うと、writer側の履歴が解放されない設定を`Policy`経由で作れてしまう。
    //      逆に`depth <= 0`は「無制限」ではなく単なる不正値なので、MAXではなく既定の1へ倒す
    // Method: DURABILITY_SERVICEの深さを振り、`Policy::from`が丸め先を使い分けることを確認する
    #[test]
    fn test_policy_from_qos_clamps_unbounded_durability_service() {
        let transient_local_qos = |kind, depth| {
            let mut qos = DdsQos::create().unwrap();
            qos.set_durability(dds_durability_kind::DDS_DURABILITY_TRANSIENT_LOCAL);
            qos.set_durability_service(Duration::ZERO, kind, depth, -1, -1, -1)
                .unwrap();
            qos
        };
        // KEEP_LASTでdepth<=0は`set_durability_service`が拒否するため、このcrateの
        // APIでは作れない。他ベンダ/不正なdiscoveryデータ由来を模してFFIで直接設定する
        let unvalidated_keep_last_qos = |depth| {
            let mut qos = DdsQos::create().unwrap();
            qos.set_durability(dds_durability_kind::DDS_DURABILITY_TRANSIENT_LOCAL);
            unsafe {
                dds_qset_durability_service(
                    qos.0,
                    0,
                    dds_history_kind::DDS_HISTORY_KEEP_LAST,
                    depth,
                    -1,
                    -1,
                    -1,
                );
            }
            qos
        };
        let actual = [
            transient_local_qos(dds_history_kind::DDS_HISTORY_KEEP_LAST, 3),
            transient_local_qos(dds_history_kind::DDS_HISTORY_KEEP_LAST, i32::MAX),
            transient_local_qos(dds_history_kind::DDS_HISTORY_KEEP_ALL, 0),
            unvalidated_keep_last_qos(0),
            unvalidated_keep_last_qos(-1),
        ]
        .map(|qos| Policy::from(&qos).durability);
        assert_eq!(
            actual,
            [
                Durability::TransientLocal {
                    sync_depth: NonZeroU16::new(3).unwrap()
                },
                Durability::TransientLocal {
                    sync_depth: NonZeroU16::MAX
                },
                Durability::TransientLocal {
                    sync_depth: NonZeroU16::MAX
                },
                Durability::TransientLocal {
                    sync_depth: NonZeroU16::MIN
                },
                Durability::TransientLocal {
                    sync_depth: NonZeroU16::MIN
                },
            ]
        );
    }

    // Why: DURABILITY_SERVICE未設定はDDS既定の`KEEP_LAST(1)`と同義で、無制限ではない。
    //      getterを`Option`にした際にKEEP_ALLと同じ扱いへ倒すと同期件数が過大になる
    // Method: durabilityだけ設定したQoSのsync_depthが1になることを確認する
    #[test]
    fn test_policy_from_qos_without_durability_service_syncs_one() {
        let mut qos = DdsQos::create().unwrap();
        qos.set_durability(dds_durability_kind::DDS_DURABILITY_TRANSIENT_LOCAL);
        assert_eq!(qos.durability_service(), None);
        assert_eq!(
            Policy::from(&qos).durability,
            Durability::TransientLocal {
                sync_depth: NonZeroU16::MIN
            }
        );
    }

    #[test]
    fn test_create_qos() {
        if let Ok(_qos) = DdsQos::create() {
        } else {
            panic!("DdsQos::create() should succeed");
        }
    }
    #[test]
    fn test_clone_qos() {
        if let Ok(qos) = DdsQos::create() {
            let _c = qos;
        } else {
            panic!("DdsQos::create() should succeed");
        }
    }

    #[test]
    fn test_merge_qos() {
        if let Ok(mut qos) = DdsQos::create() {
            let c = qos.clone();
            qos.merge(&c);
        } else {
            panic!("DdsQos::create() should succeed");
        }
    }

    #[test]
    fn test_set() {
        if let Ok(mut qos) = DdsQos::create() {
            let _qos = qos
                .set_durability(dds_durability_kind::DDS_DURABILITY_VOLATILE)
                .set_history(dds_history_kind::DDS_HISTORY_KEEP_LAST, 3)
                .expect("KEEP_LAST(3) is valid")
                .set_resource_limits(10, 1, 10)
                .set_presentation(
                    dds_presentation_access_scope_kind::DDS_PRESENTATION_INSTANCE,
                    false,
                    false,
                )
                .set_lifespan(std::time::Duration::from_nanos(100))
                .set_deadline(std::time::Duration::from_nanos(100))
                .set_latency_budget(1000)
                .set_ownership(dds_ownership_kind::DDS_OWNERSHIP_EXCLUSIVE)
                .set_ownership_strength(1000)
                .set_liveliness(dds_liveliness_kind::DDS_LIVELINESS_AUTOMATIC, 10000)
                .set_time_based_filter(1000)
                .set_reliability(
                    dds_reliability_kind::DDS_RELIABILITY_RELIABLE,
                    std::time::Duration::from_nanos(100),
                )
                .set_transport_priority(1000)
                .set_destination_order(
                    dds_destination_order_kind::DDS_DESTINATIONORDER_BY_RECEPTION_TIMESTAMP,
                )
                .set_writer_data_lifecycle(true)
                .set_reader_data_lifecycle(100, 100)
                .set_durability_service(
                    Duration::ZERO,
                    dds_history_kind::DDS_HISTORY_KEEP_LAST,
                    3,
                    3,
                    3,
                    3,
                )
                .expect("KEEP_LAST(3) is valid")
                .set_partition(&std::ffi::CString::new("partition1").unwrap())
                .set_userdata(b"role=logger");
        } else {
            panic!("DdsQos::create() should succeed");
        }
    }

    #[test]
    fn test_userdata_roundtrip() {
        let mut qos = DdsQos::create().unwrap();
        assert_eq!(qos.userdata(), None);

        qos.set_userdata(b"role=logger");
        assert_eq!(qos.userdata(), Some(b"role=logger".to_vec()));

        // 空データの設定もエラーにならず、未設定と区別なく扱える
        qos.set_userdata(b"");
        assert_eq!(qos.userdata(), None);
    }
}
