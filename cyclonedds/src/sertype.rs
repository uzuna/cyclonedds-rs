//! sertypeに関する実装のモジュール
//!
//! sertypeはCycloneDDSで型を扱う構造体で、基本は型名とメッセージの大きさで区別する
//! コアとなるのは `ddsi_sertype_ops` で、Topic毎の比較に使われるのが一つ
//! 受信時のサンプル作成や送信時のシリアライズ実装などの関数を登録する
use std::{ffi::CStr, marker::PhantomData};

use cdr::Infinite;
use cyclonedds_sys::{
    DDS_FREE_ALL_BIT, DDS_FREE_CONTENTS_BIT, dds_free_op_t, ddsi_serdata_ops, ddsi_sertype,
    ddsi_sertype_fini, ddsi_sertype_init, ddsi_sertype_ops, ddsi_sertype_v0,
};
use murmur3::murmur3_32;
use serde::{Serialize, de::DeserializeOwned};
use tracing::{trace, warn};

use crate::{
    Sample, TopicType,
    serdata::{create_serdata_ops_base, create_serdata_ops_serdes},
    util::SGReader,
};

/// CycloneDDSで特定の型を扱うための情報を保持する構造体
#[repr(C)]
pub(crate) struct SerType<T> {
    pub sertype: ddsi_sertype,
    _phantom: PhantomData<T>,
}

impl<T> SerType<T> {
    const DEFAULT_TYPENAME: &'static str = "Untyped";

    /// Untyped向けのopsテーブル
    ///
    /// opsテーブルを型ごとの`'static`にしているのは、opsがsertypeより長生きするため。
    /// [crate::serdata::serdata_to_untyped]が作るuntyped serdataは`type_`をnullにして
    /// sertypeへの参照を持たないが、tkmapに載ってドメイン解体(`ddsi_tkmap_free`)まで
    /// 生存する。この解体時に`serdata->ops->free`が呼ばれるため、sertypeと同じ寿命の
    /// ヒープ領域にopsを置くと解放済み領域を参照してSEGV/SIGBUSする。
    /// CycloneDDS本体のopsテーブルもグローバルなconstである。
    const SERTYPE_OPS_BASE: ddsi_sertype_ops = create_sertype_ops_base::<T>();
    const SERDATA_OPS_BASE: ddsi_serdata_ops = create_serdata_ops_base::<T>();

    /// SerType<T>を型情報なしで生成する
    pub fn untyped(type_name: &str) -> Box<SerType<T>> {
        Box::<SerType<T>>::new(SerType {
            sertype: {
                let ctype = std::ffi::CString::new(type_name).unwrap();
                let mut sertype = std::mem::MaybeUninit::uninit();
                // 型注釈はopsテーブルが'staticへ昇格していることをコンパイル時に保証する。
                // 一時変数のままだとsertypeがダングリングポインタを保持してしまう
                let sertype_ops: &'static ddsi_sertype_ops = &Self::SERTYPE_OPS_BASE;
                let serdata_ops: &'static ddsi_serdata_ops = &Self::SERDATA_OPS_BASE;
                unsafe {
                    ddsi_sertype_init(
                        sertype.as_mut_ptr(),
                        ctype.as_ptr(),
                        sertype_ops,
                        serdata_ops,
                        true,
                    );
                    let mut sertype = sertype.assume_init();
                    // Untypedでは型が不明なのでIOXのRAW扱いは出来ない
                    sertype.set_fixed_size(0);
                    sertype.iox_size = 0;
                    sertype
                }
            },
            _phantom: PhantomData,
        })
    }
}

/// シリアライズ実装を持つ型向けのopsテーブルと生成
impl<T> SerType<T>
where
    T: DeserializeOwned + Serialize + TopicType,
{
    /// `'static`にする理由は [SerType::SERTYPE_OPS_BASE] と同じ
    const SERTYPE_OPS_SER: ddsi_sertype_ops = create_sertype_ops_ser::<T>();
    const SERDATA_OPS_SERDES: ddsi_serdata_ops = create_serdata_ops_serdes::<T>();

    /// SerType<T>を生成する
    ///
    /// TODO: DeserializeOwned + Serializeなしで生成できるようにする
    pub fn new() -> Box<SerType<T>> {
        Box::<SerType<T>>::new(SerType {
            sertype: {
                let mut sertype = std::mem::MaybeUninit::uninit();
                // 型注釈の意図は [SerType::untyped] と同じ
                let sertype_ops: &'static ddsi_sertype_ops = &Self::SERTYPE_OPS_SER;
                let serdata_ops: &'static ddsi_serdata_ops = &Self::SERDATA_OPS_SERDES;
                unsafe {
                    let type_name = T::typename();
                    ddsi_sertype_init(
                        sertype.as_mut_ptr(),
                        type_name.as_ptr(),
                        sertype_ops,
                        serdata_ops,
                        !T::has_key(),
                    );
                    let mut sertype = sertype.assume_init();
                    // 固定型の場合はフラグとサイズを設定する
                    if T::is_fixed_size() {
                        sertype.set_fixed_size(1);
                        sertype.iox_size = std::mem::size_of::<T>() as u32;
                    } else {
                        // 可変長型の場合はフラグを解除して、ioxは都度取得する
                        sertype.set_fixed_size(0);
                    }
                    sertype
                }
            },
            _phantom: PhantomData,
        })
    }
}

impl<T> SerType<T> {
    /// SerType<T>を生ポインタに変換する。CycloneDDSランタイムに管理させる
    pub fn into_sertype(sertype: Box<SerType<T>>) -> *mut ddsi_sertype {
        Box::<SerType<T>>::into_raw(sertype).cast()
    }

    /// CycloneDDSランタイムから変えられた生ポインタをRustで扱う
    pub fn try_from_sertype(sertype: *const ddsi_sertype) -> Option<Box<SerType<T>>> {
        let ptr = sertype as *mut SerType<T>;
        if !ptr.is_null() {
            Some(unsafe { Box::from_raw(ptr) })
        } else {
            None
        }
    }

    // 型名を取得する
    pub fn type_name(&self) -> &str {
        if self.sertype.type_name.is_null() {
            return Self::DEFAULT_TYPENAME;
        }
        unsafe {
            let cstr = CStr::from_ptr(self.sertype.type_name);
            cstr.to_str().unwrap_or(Self::DEFAULT_TYPENAME)
        }
    }

    // settype_ops内で扱いやすくするための変換関数
    pub fn const_ref_from_sertype<'a>(sertype: *const ddsi_sertype) -> &'a Self {
        let ptr = sertype as *const SerType<T>;
        // 基本的にnullになることはないはず
        if ptr.is_null() {
            panic!("sertype is null");
        }
        unsafe { &*ptr }
    }
}

impl<T> Drop for SerType<T> {
    fn drop(&mut self) {
        unsafe {
            let sertype = &mut self.sertype as *mut ddsi_sertype;
            // CycloneDDSのsertypeのリソースを開放。type_nameとか。
            // opsテーブルは型ごとの'staticなので開放しない
            ddsi_sertype_fini(sertype);
        }
    }
}

// sertype_opsの実装。型の大きさが分かれば動作する
//
// `const fn`にしているのは、opsテーブルを型ごとの`'static`な値として持つため。
// 理由は [SerType] のopsテーブル定義を参照。
const fn create_sertype_ops_base<T>() -> ddsi_sertype_ops {
    ddsi_sertype_ops {
        // version情報。0.10.5時点ではv0のみ
        version: Some(ddsi_sertype_v0),
        // 引数は特に使わないのでnull
        arg: std::ptr::null_mut(),
        // refcount=0になったサンプルの開放
        free: Some(sertype_free::<T>),
        // 特定のサンプルを0初期化する関数. `dds_return_loan`でサンプルをクリーンアップするのに使う
        zero_samples: Some(sertype_zero_samples::<T>),
        // サンプル領域を確保する関数。`dds_read`でloanするときに使われる
        realloc_samples: Some(sertype_realloc_samples::<T>),
        // `ddsi_sertype_to_sample` で確保したサンプルを開放する関数
        free_samples: Some(sertype_free_samples::<T>),
        // 型名、キーなし、操作が同じだととわかっているケースにsertypeの等価性を比較する関数
        equal: Some(sertype_equal::<T>),
        // 型定義のhash値を計算する関数。型情報の保持の計算効率化に使っているらしい。 lookup in hopscotch.c.
        //
        // CXX実装では0を返している
        // https://github.com/eclipse-cyclonedds/cyclonedds-cxx/blob/templated-streaming/src/ddscxx/include/org/eclipse/cyclonedds/topic/datatopic.hpp
        hash: Some(sertype_hash::<T>),

        // XTypes向けの型情報。このバインディングでは提供しない
        type_id: None,
        type_map: None,
        type_info: None,
        derive_sertype: None,

        // Unyped実装でも受信でShmを利用するためのダミー関数を設定している
        #[cfg(feature = "shm")]
        get_serialized_size: Some(dummy_sertype_get_serialized_size::<T>),
        #[cfg(feature = "shm")]
        serialize_into: Some(dummy_sertype_serialize_into::<T>),
        #[cfg(not(feature = "shm"))]
        get_serialized_size: None,
        #[cfg(not(feature = "shm"))]
        serialize_into: None,
    }
}

// baseに加えて、fixedでない型をシリアライズするための関数を登録する
const fn create_sertype_ops_ser<T>() -> ddsi_sertype_ops
where
    T: serde::Serialize,
{
    let mut ops = create_sertype_ops_base::<T>();
    // Fixedではない型をIceoryxで送信するための関数を登録する
    ops.get_serialized_size = Some(sertype_get_serialized_size::<T>);
    ops.serialize_into = Some(sertype_serialize_into::<T>);
    ops
}

// refcount=0になったサンプルの開放
//
// CycloneDDSのsertypeの開放と関連するops及び構造体の開放を行う
unsafe extern "C" fn sertype_free<T>(sertype: *mut cyclonedds_sys::ddsi_sertype) {
    let _it = SerType::<T>::try_from_sertype(sertype);
}

// 0初期化する関数. `dds_return_loan`でサンプルをクリーンアップするのに使う
#[tracing::instrument(level = "trace")]
unsafe extern "C" fn sertype_zero_samples<T>(
    sertype: *const ddsi_sertype,
    _ptr: *mut std::ffi::c_void,
    _len: usize,
) {
    let sertype_ref = SerType::<T>::const_ref_from_sertype(sertype);
    trace!(type_name = sertype_ref.type_name());
    // 必要になったら実装する
}

// サンプル保持領域を確保するする関数。`dds_read`でloanするときに使われる
#[tracing::instrument(level = "trace")]
extern "C" fn sertype_realloc_samples<T>(
    ptrs: *mut *mut std::ffi::c_void,
    sertype: *const ddsi_sertype,
    old: *mut std::ffi::c_void,
    old_count: usize,
    new_count: usize,
) {
    assert!(
        old.is_null() || old_count != 0,
        "old is not null but old_count is zero"
    );
    trace!(?ptrs, old = ?old, old_count, new_count);
    let sertype_ref = SerType::<T>::const_ref_from_sertype(sertype);
    let new = if !old.is_null() {
        // oldに残ったデータがあれば詰替えを行う
        let mut old = unsafe {
            Vec::<*mut Sample<T>>::from_raw_parts(old as *mut *mut Sample<T>, old_count, old_count)
        };

        if new_count < old_count {
            // 縮小時は余剰サンプルを明示的に解放し、リークを防ぐ
            for sample in old.drain(new_count..) {
                let _ = unsafe { Box::<Sample<T>>::from_raw(sample) };
            }
        } else if new_count > old_count {
            old.extend(
                std::iter::repeat_with(|| Box::into_raw(Box::default()))
                    .take(new_count - old_count),
            );
        }

        old
    } else {
        // Vec::from_raw_partsはnullpointerを受け付けないため新規作成のみ
        std::iter::repeat_with(|| Box::into_raw(Box::default()))
            .take(new_count)
            .collect()
    };

    let leaked = new.leak();
    let raw = leaked.as_ptr();
    trace!(type_name = sertype_ref.type_name(), old_count, new_count, samples = ?raw);
    unsafe {
        *ptrs = raw as *mut std::ffi::c_void;
    }
}

// `ddsi_sertype_to_sample` で確保したサンプルを開放する関数
#[tracing::instrument(level = "trace")]
extern "C" fn sertype_free_samples<T>(
    sertype: *const ddsi_sertype,
    ptrs: *mut *mut std::ffi::c_void,
    len: usize,
    op: dds_free_op_t,
) {
    let ptrs_v = ptrs as *mut *mut Sample<T>;
    let sertype_ref = SerType::<T>::const_ref_from_sertype(sertype);
    trace!(type_name = sertype_ref.type_name(), len, ?op);
    if ptrs_v.is_null() || len == 0 {
        return;
    }

    if (op & DDS_FREE_ALL_BIT) != 0 {
        // 領域とサンプル自体の両方を解放する
        let samples = unsafe { Vec::<*mut Sample<T>>::from_raw_parts(ptrs_v, len, len) };
        samples.into_iter().for_each(|sample| {
            let _ = unsafe { Box::<Sample<T>>::from_raw(sample) };
        });
    } else {
        // コンテンツのみ解放する
        assert_ne!(op & DDS_FREE_CONTENTS_BIT, 0);
        let samples = unsafe { std::slice::from_raw_parts_mut(ptrs_v, len) };
        for sample in samples {
            let sample_ref = unsafe { &mut **sample };
            sample_ref.free_contents();
        }
    }
}

// 型名、キーなし、操作が同じだとはCycloneDDSが比較している
// 追加的に区別するべき型の違いがある場合はここで比較する
#[tracing::instrument(level = "trace")]
unsafe extern "C" fn sertype_equal<T>(
    acmn: *const ddsi_sertype,
    bcmn: *const ddsi_sertype,
) -> bool {
    let a = SerType::<T>::const_ref_from_sertype(acmn);
    let b = SerType::<T>::const_ref_from_sertype(bcmn);
    trace!(type_name_a = a.type_name(), type_name_b = b.type_name());
    true
}

// 型定義のhash値を計算する関数。型情報の保持周りの計算効率化に使っているらしい
#[tracing::instrument(level = "trace")]
unsafe extern "C" fn sertype_hash<T>(tp: *const ddsi_sertype) -> u32 {
    let sertype = SerType::<T>::const_ref_from_sertype(tp);
    trace!(type_name = sertype.type_name());
    // 型名と型サイズでハッシュ値を計算する
    // SAFETY: type_name は sertype の生存中保持される NUL 終端文字列である。
    let type_name = unsafe { CStr::from_ptr(sertype.sertype.type_name) };
    let type_name_bytes = type_name.to_bytes();
    let type_size = core::mem::size_of::<T>().to_ne_bytes();
    let sg_list = [type_name_bytes, &type_size];
    let mut sg_buffer = SGReader::new(&sg_list);

    let hash = murmur3_32(&mut sg_buffer, 0);
    hash.unwrap_or(0)
}

// ダミー関数
// Untypedの場合は(read|take)cdrしか呼べないので、この関数が呼ばれることはない
#[tracing::instrument(level = "trace")]
unsafe extern "C" fn dummy_sertype_get_serialized_size<T>(
    tp: *const ddsi_sertype,
    sample: *const ::std::os::raw::c_void,
) -> usize {
    let sertype = SerType::<T>::const_ref_from_sertype(tp);
    trace!(type_name = sertype.type_name());
    0
}

// ダミー関数
// Untypedの場合は(read|take)cdrしか呼べないので、この関数が呼ばれることはない
#[tracing::instrument(level = "trace")]
unsafe extern "C" fn dummy_sertype_serialize_into<T>(
    tp: *const ddsi_sertype,
    sample: *const ::std::os::raw::c_void,
    dst_buffer: *mut ::std::os::raw::c_void,
    dst_size: usize,
) -> bool {
    let sertype = SerType::<T>::const_ref_from_sertype(tp);
    trace!(type_name = sertype.type_name(), dst_size);
    true
}

// fixedでない型をシリアライズする特にioxに確保するメモリサイズを教える
#[tracing::instrument(level = "trace")]
unsafe extern "C" fn sertype_get_serialized_size<T>(
    tp: *const ddsi_sertype,
    sample: *const ::std::os::raw::c_void,
) -> usize
where
    T: serde::Serialize,
{
    let sertype = SerType::<T>::const_ref_from_sertype(tp);
    trace!(type_name = sertype.type_name());
    let s = Sample::<T>::const_ref_from_sample(sample as *const Sample<T>);
    cdr::calc_serialized_size(s.get_expected().as_ref()) as usize
}

// fixedでない型をシリアライズする特に呼ばれる
#[tracing::instrument(level = "trace")]
unsafe extern "C" fn sertype_serialize_into<T>(
    tp: *const ddsi_sertype,
    sample: *const ::std::os::raw::c_void,
    dst_buffer: *mut ::std::os::raw::c_void,
    dst_size: usize,
) -> bool
where
    T: serde::Serialize,
{
    let sertype = SerType::<T>::const_ref_from_sertype(tp);
    trace!(type_name = sertype.type_name(), dst_size);

    let s = Sample::<T>::const_ref_from_sample(sample as *const Sample<T>);
    let size = cdr::calc_serialized_size(s.get_expected().as_ref());
    if size as usize > dst_size {
        return false;
    }
    let mut writer = unsafe { std::slice::from_raw_parts_mut(dst_buffer as *mut u8, dst_size) };
    let res = cdr::serialize_into::<_, _, _, cdr::CdrBe>(
        &mut writer,
        s.get_expected().as_ref(),
        Infinite,
    );
    match res {
        Ok(_) => true,
        Err(e) => {
            warn!("Failed to serialize sample: {}", e);
            false
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::{ffi::c_void, marker::PhantomData, sync::Arc};

    use cyclonedds_sys::{
        DDS_FREE_ALL_BIT, DDS_FREE_CONTENTS_BIT, DDSError, DdsEntity,
        IOX_CHUNK_CONTAINS_SERIALIZED_DATA, dds_create_writer, dds_return_loan, dds_write,
        ddsi_sertype, iceoryx_header, iceoryx_header_from_chunk,
    };

    use crate::{
        DdsParticipant, DdsPublisher, DdsTopic, DdsWritable, Entity, Sample, TopicType,
        common::{TestDomain, tests::TestTypeAlloc},
        sertype::SerType,
    };

    // IoxChunkテストのためのWriter
    pub struct Writer<T> {
        entity: DdsEntity,
        _phantom: PhantomData<T>,
    }

    impl<T> Entity for Writer<T> {
        fn entity(&self) -> &DdsEntity {
            &self.entity
        }
    }

    impl<T> Writer<T> {
        pub fn create(entity: &dyn DdsWritable, topic: DdsTopic<T>) -> Result<Self, DDSError>
        where
            T: std::marker::Sized + TopicType,
        {
            unsafe {
                let w = dds_create_writer(
                    entity.entity().entity(),
                    topic.entity().entity(),
                    std::ptr::null(),
                    std::ptr::null_mut(),
                );
                if w < 1 {
                    return Err(DDSError::from(w));
                } else {
                    Ok(Writer {
                        entity: DdsEntity::new(w),
                        _phantom: PhantomData,
                    })
                }
            }
        }

        /// Iceoryxの共有メモリバッファを借用する
        pub fn dds_loan_shared_memory_buffer<'a>(&'a self, size: usize) -> Option<IoxChunk<'a, T>> {
            unsafe {
                let mut p_sample: *mut c_void = std::ptr::null_mut();
                // dds_write実装では shm_create_chunk で確保しているが、公開されていない関数なのでここでは使えない
                // writerに紐付けられたバッファを得る類似関数で代替している
                let res = cyclonedds_sys::dds_loan_shared_memory_buffer(
                    self.entity().entity(),
                    size,
                    &mut p_sample as *mut *mut c_void,
                );
                if res == 0 {
                    Some(IoxChunk::new(self, p_sample))
                } else {
                    None
                }
            }
        }

        fn write_to_entity(entity: &DdsEntity, msg: std::sync::Arc<T>) -> Result<(), DDSError> {
            unsafe {
                let sample = Sample::<T>::from(msg);
                let sample = &sample as *const Sample<T>;
                let sample = sample as *const c_void;
                let ret = dds_write(entity.entity(), sample);
                if ret >= 0 {
                    Ok(())
                } else {
                    Err(DDSError::from(ret))
                }
            }
        }

        pub fn write(&mut self, msg: std::sync::Arc<T>) -> Result<(), DDSError> {
            Self::write_to_entity(&self.entity, msg)
        }
    }

    // Iceoryxについての一時的な参照
    // バッファはserdataに紐付けて開放される
    pub struct IoxChunk<'a, T> {
        w: &'a Writer<T>,
        pub ptr: *mut c_void,
    }

    impl<'a, T> IoxChunk<'a, T> {
        pub fn new(w: &'a Writer<T>, ptr: *mut c_void) -> Self {
            IoxChunk { w, ptr }
        }

        fn header_mut(&self) -> &mut iceoryx_header {
            unsafe { &mut *iceoryx_header_from_chunk(self.ptr) }
        }

        // 書き込みデータ領域を取得する
        pub fn as_slice(&self) -> &[u8] {
            unsafe {
                let header = self.header_mut();
                std::slice::from_raw_parts(self.ptr as *const u8, header.data_size as usize)
            }
        }

        // serdataに結び付けない場合に借りたバッファを開放する
        pub fn return_loan(mut self) {
            unsafe {
                let _ = dds_return_loan(
                    self.w.entity().entity(),
                    &mut self.ptr as *mut *mut c_void,
                    1,
                );
            }
        }
    }

    // settype_opsの関数を呼び出すための補助構造体
    pub struct SerTypeOps<'a, T> {
        sertype: &'a SerType<T>,
    }

    impl<'a, T> SerTypeOps<'a, T> {
        pub fn new(ser_type: &'a SerType<T>) -> Self {
            Self { sertype: ser_type }
        }

        #[inline]
        fn ops(&self) -> &cyclonedds_sys::ddsi_sertype_ops {
            unsafe { &*self.sertype.sertype.ops }
        }

        #[inline]
        unsafe fn sertype_ptr(&self) -> *const ddsi_sertype {
            self.sertype as *const SerType<T> as *const ddsi_sertype
        }

        #[inline]
        unsafe fn sample_ptr(sample: &Sample<T>) -> *const c_void {
            sample as *const Sample<T> as *const c_void
        }

        // ops->get_serialized_sizeを呼び出す
        pub fn get_serialized_size(&self, sample: &Sample<T>) -> usize {
            unsafe {
                self.ops().get_serialized_size.unwrap()(
                    self.sertype_ptr(),
                    Self::sample_ptr(sample),
                )
            }
        }

        // ops->serialize_intoを呼び出す
        pub fn serialize_into(&self, sample: &Sample<T>, buffer: &IoxChunk<'_, T>) -> bool {
            let target_size = self.get_serialized_size(sample);
            unsafe {
                let res = self.ops().serialize_into.unwrap()(
                    self.sertype_ptr(),
                    Self::sample_ptr(sample),
                    buffer.ptr,
                    target_size,
                );
                // shmがシリアライズ済みデータであることを示す
                if res {
                    let header = buffer.header_mut();
                    header.shm_data_state = IOX_CHUNK_CONTAINS_SERIALIZED_DATA;
                }
                res
            }
        }

        // ops->realloc_samplesを呼び出す
        fn realloc_samples(
            &self,
            old_ptr: *mut c_void,
            old_size: usize,
            new_size: usize,
        ) -> (*mut c_void, usize) {
            let mut new_ptr: *mut c_void = std::ptr::null_mut();
            unsafe {
                self.ops().realloc_samples.unwrap()(
                    &mut new_ptr as *mut *mut c_void,
                    self.sertype_ptr(),
                    old_ptr,
                    old_size,
                    new_size,
                );
            }
            (new_ptr, new_size)
        }

        // ops->free_samplesを呼び出す
        fn free_samples(&self, ptr: *mut c_void, len: usize, dds_free_bit: u32) {
            unsafe {
                self.ops().free_samples.unwrap()(
                    self.sertype_ptr(),
                    ptr as *mut *mut c_void,
                    len,
                    dds_free_bit,
                );
            }
        }
    }

    // FixedSizeでない型のシリアライズ関数のテスト
    #[test_log::test]
    #[ignore = "requires iox-roudi to be running"]
    fn test_sertype_ops_serialize() -> anyhow::Result<()> {
        let domain_id = TestDomain::SertypeOpsSerialize.id();
        let _domain = crate::common::tests::create_shm_domain(domain_id)?;
        let p = unsafe { DdsParticipant::create(Some(domain_id), None, None)? };
        let pubb = DdsPublisher::create(&p, None, None)?;
        let topic = DdsTopic::<TestTypeAlloc>::create(&p, "serops_iox", None, None)?;
        let mut w = Writer::create(&pubb, topic)?;

        // iox向けにはシリアライズがすでに走る。スレッド間共有なら自前で行われているはずで
        // ioxを必要とするのは別プロセスであると想定される
        w.write(Arc::new(TestTypeAlloc::default()))?;

        let tp = SerType::<TestTypeAlloc>::new();
        let tpops = SerTypeOps::<TestTypeAlloc>::new(&tp);
        let td = TestTypeAlloc::samples(4);
        for expect in td {
            let expect = Sample::from(expect);

            // IoxChunkへの書き込みシーケンス
            let iox_size = tpops.get_serialized_size(&expect);
            let buffer = w.dds_loan_shared_memory_buffer(iox_size).unwrap();
            assert!(tpops.serialize_into(&expect, &buffer));

            // sertype_opsの関数によってシリアライズされたデータを確認
            let act = cdr::deserialize::<TestTypeAlloc>(buffer.as_slice())?;
            assert_eq!(expect.get_expected().as_ref(), &act);

            // 消費者がいないので開放
            buffer.return_loan();
        }
        Ok(())
    }

    // ops->realloc_samplesとops->free_samplesのテスト
    #[test_log::test]
    fn test_sertype_ops_realloc() -> anyhow::Result<()> {
        // 所定の長さの空のサンプルが確保されていて、SEGVなどが起きないことを確認する
        fn verify_empty_samples<T>(ptr: *mut c_void, len: usize) {
            let samples = unsafe { std::slice::from_raw_parts(ptr as *mut *mut Sample<T>, len) };
            samples.iter().for_each(|&p| {
                let s = unsafe { &*p };
                assert!(s.try_deref().is_none());
            });
        }
        let sizes = [1, 4, 2, 8, 1, 16, 3];

        let tp = SerType::<TestTypeAlloc>::new();
        let tpops = SerTypeOps::<TestTypeAlloc>::new(&tp);

        for i in 1..32 {
            let (new_ptr, new_size) = tpops.realloc_samples(std::ptr::null_mut(), 0, i);
            tpops.free_samples(new_ptr, new_size, DDS_FREE_ALL_BIT);
        }

        let mut old_ptr: *mut c_void = std::ptr::null_mut();
        let mut old_size = 0;
        for &size in &sizes {
            let (new_ptr, new_size) = tpops.realloc_samples(old_ptr, old_size, size);
            old_ptr = new_ptr;
            old_size = new_size;
            verify_empty_samples::<TestTypeAlloc>(old_ptr, old_size);
        }
        tpops.free_samples(old_ptr, old_size, DDS_FREE_CONTENTS_BIT);
        tpops.free_samples(old_ptr, old_size, DDS_FREE_ALL_BIT);

        Ok(())
    }
}
