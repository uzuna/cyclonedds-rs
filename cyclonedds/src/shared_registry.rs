//! ドメインIDごとにプロセス寿命の共有エンティティを保持する登録簿
//!
//! cycloneddsは「あるドメインの参加者refcountが1→0になった瞬間」にドメインの内部状態を
//! 解体するため、これと同じドメインへの生成が並行するとSEGVする。一度作ったエンティティを
//! 意図的にリークさせて保持し続ければrefcountが0に落ちる瞬間そのものが無くなり、
//! このレースを構造的に回避できる。詳細は`docs/domain-lifecycle.md`を参照。

use std::collections::HashMap;
use std::sync::Mutex;

use cyclonedds_sys::{DDSError, DdsDomainId};

pub(crate) struct SharedRegistry<T: 'static> {
    entries: Mutex<HashMap<DdsDomainId, &'static T>>,
}

/// `SharedRegistry::get_or_create`の結果。呼び出し側が「既存を引き当てたのか、
/// 自分が初回生成したのか」を区別できるようにする(引数を無視して既存を返す際の
/// 検知に使う)。
pub(crate) enum Shared<T: 'static> {
    Existing(&'static T),
    Created(&'static T),
}

impl<T: Send + Sync + 'static> Default for SharedRegistry<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + Sync + 'static> SharedRegistry<T> {
    pub(crate) fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// `key`に対応する共有エンティティを返す。無ければ`create`で作りリークして登録する。
    ///
    /// 生成は登録簿のロック内で行うため、複数スレッドが同時に呼んでも生成は1回きりになる。
    /// `resolve_key`は生成したエンティティの「実際の」キーを返す。要求キーが
    /// `DDS_DOMAIN_DEFAULT`のように生成後にしか確定しない場合があり、その場合でも
    /// 実キーのエントリと相互にエイリアスを張って別インスタンスへの分裂を防ぐ。
    pub(crate) fn get_or_create<C, R>(
        &self,
        key: DdsDomainId,
        create: C,
        resolve_key: R,
    ) -> Result<Shared<T>, DDSError>
    where
        C: FnOnce() -> Result<T, DDSError>,
        R: FnOnce(&T) -> Result<DdsDomainId, DDSError>,
    {
        // 中身は&'staticのコピーだけなので、他スレッドのpanicで汚染されても壊れない
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(entity) = entries.get(&key) {
            return Ok(Shared::Existing(*entity));
        }

        let entity = create()?;
        let actual = resolve_key(&entity)?;

        if let Some(existing) = entries.get(&actual).copied() {
            // 同じ実体が既にあるので今作ったものは捨てる。このdropの時点では
            // 同じドメインに`existing`が生存しているためrefcountは0にならない
            drop(entity);
            entries.insert(key, existing);
            return Ok(Shared::Existing(existing));
        }

        let leaked: &'static T = Box::leak(Box::new(entity));
        entries.insert(actual, leaked);
        if key != actual {
            entries.insert(key, leaked);
        }
        Ok(Shared::Created(leaked))
    }
}
