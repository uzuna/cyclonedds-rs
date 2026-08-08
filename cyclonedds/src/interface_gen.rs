//! Rust構造体からDDS IDL / ROS 2 .msg 定義を導出するためのトレイト群。
//!
//! deriveマクロ(`DdsInterface`)はここで定義するトレイトの実装コードを生成するだけで、
//! フィールド型の中身はパースしない。型解決はRustコンパイラに委ねることで、
//! 別モジュール・別クレートの型でもトレイトさえ実装していれば動作し、
//! 未実装ならコンパイルエラーとして検出される。

use std::collections::HashMap;

/// フィールドの型として参照される際の表現を与えるトレイト。
/// プリミティブ・コンテナ型は本体クレートで実装し、ユーザー定義型はderiveで実装する。
///
/// Why on_unimplemented: Arc/Rc/Mutex等の不適格なフィールド型は本トレイトの未実装として
/// 拒否される設計のため、素のE0277の代わりにドメイン向けの案内を出す。
/// 名前ベースのdenylistにしないのは、`Arc`(円弧)のような正当なROS型名を誤拒否しないため。
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a DDS/ROS 2 interface type",
    label = "this type cannot appear as a field of a .msg/IDL definition",
    note = "use primitives (bool, u8..u64, i8..i64, f32, f64, String), Vec<T>, [T; N], or a struct deriving DdsInterface"
)]
pub trait InterfaceRef {
    /// IDLでの型参照。例: `"unsigned long"`, `"sequence<double>"`, `"geometry_msgs::msg::dds_::Point_"`
    /// primitiveならstrでも足りるが、ユーザー定義型では名前空間を埋め込むためStringを返す
    fn idl_ref(mode: NsMode) -> String;

    /// .msgでの型参照。例: "uint32", "float64[]", "geometry_msgs/Point"
    fn msg_ref() -> String;

    /// IDLのフィールド宣言1行分。
    /// Why: 固定長配列はIDLでは `double matrix[16];` のようにサイズがフィールド名側に付き、
    /// 型参照(idl_ref)だけでは表現できないため、フィールド単位でオーバーライド可能にしている。
    fn idl_field(name: &str, mode: NsMode) -> String {
        format!("{} {};", Self::idl_ref(mode), name)
    }

    /// 自身が依存する型定義をレジストリへ登録する（プリミティブ・コンテナは委譲のみ）
    fn collect_defs(_reg: &mut DefRegistry) {}

    /// 自身をROS 2連結メッセージ定義(後述`full_ros2_msg`)の依存先として登録する
    /// （プリミティブ・コンテナは委譲のみ。derive型は自身を登録した上でフィールドへ委譲する）
    fn collect_msg_defs(_reg: &mut MsgDefRegistry) {}
}

/// `Vec<T>`/`[T; N]`の要素型として使える型を表すマーカートレイト。
///
/// Why: ROS 2 `.msg`には`T[][]`や`T[N][M]`のような多段配列・多段シーケンスの記法が存在しない
/// (<https://design.ros2.org/articles/interface_definition.html>)。`Vec<T>`/`[T; N]`自体は
/// このトレイトを実装しないため、`Vec<Vec<T>>`・`Vec<[T; N]>`・`[Vec<T>; N]`・`[[T; N]; M]`
/// のような多段コンテナはコンパイルエラーになる。プリミティブとderive構造体のみ要素になれる。
///
/// ```compile_fail
/// # use cyclonedds_rs::InterfaceRef;
/// fn assert_interface_ref<T: InterfaceRef>() {}
/// assert_interface_ref::<Vec<Vec<f64>>>(); // Vec<f64>はInterfaceElemを実装しないため失敗する
/// ```
///
/// ```compile_fail
/// # use cyclonedds_rs::InterfaceRef;
/// fn assert_interface_ref<T: InterfaceRef>() {}
/// assert_interface_ref::<[[f64; 3]; 4]>(); // [f64;3]はInterfaceElemを実装しないため失敗する
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be used as a Vec<T>/[T; N] element type",
    label = "Vec/array elements must be a primitive or a struct deriving DdsInterface",
    note = "nested containers such as Vec<Vec<T>>, Vec<[T; N]>, [Vec<T>; N], [[T; N]; M] have no valid ROS 2 .msg/IDL representation"
)]
pub trait InterfaceElem: InterfaceRef {}

/// 型定義本体を生成できる型（deriveマクロが実装する）
pub trait InterfaceDef: InterfaceRef {
    /// 所属パッケージ名。例: "my_robot_interfaces"
    fn package() -> &'static str;
    /// 型名。例: "RobotStatus"
    fn name() -> &'static str;
    /// 自身のstruct定義のみ（module節を含む）
    fn idl_def(mode: NsMode) -> String;
    /// 自身の .msg 定義のみ
    fn msg_def() -> String;

    /// 依存型を含めた完全なIDL（依存が先に来るトポロジカル順・重複排除済み）
    fn idl(mode: NsMode) -> String {
        let mut reg = DefRegistry::new(mode);
        Self::collect_defs(&mut reg);
        reg.entries.concat()
    }

    /// 自身の .msg 定義。ROS 2の .msg は1メッセージ1ファイルのため依存先は含めない
    fn ros2_msg() -> String {
        Self::msg_def()
    }

    /// mcap/rosbag2が埋め込みスキーマとして使う「連結メッセージ定義」形式。
    /// 自身の .msg 定義に続けて、依存する各型の定義を
    /// `=`x80の区切り線と `MSG: pkg/Name` ヘッダで区切りながら重複なく列挙する。
    /// 参考: <https://mcap.dev/docs/python/ros2_noenv_example>
    fn full_ros2_msg() -> String {
        let mut reg = MsgDefRegistry::new();
        Self::collect_msg_defs(&mut reg);
        reg.render()
    }
}

/// IDLの名前空間モード。`idl()`/`idl_def()`呼び出し時に引数として指定する。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NsMode {
    #[default]
    Raw,
    /// 中間名前空間を挿入する。例: `WithMiddle("msg")` → `module pkg { module msg { struct T { ... }; }; };`
    /// `::` 区切りで複数階層を指定できる。例: `WithMiddle("msg::dds_")` → `module pkg { module msg { module dds_ { ... };};};`
    WithMiddle(&'static str),
    /// 中間名前空間に加えて型名へsuffixを付与する。`middle`は`WithMiddle`と同じく`::`区切りで複数階層を指定できる。
    /// 例: `WithMiddleSuffix { middle: "msg::dds_", suffix: "_" }` → `module pkg { module msg { module dds_ { struct T_ { ... }; };};};`
    WithMiddleSuffix {
        middle: &'static str,
        suffix: &'static str,
    },
}

impl NsMode {
    /// ROS 2のIDL生成時に使う名前空間モード。ROSのmsg定義からIDLを生成する場合に使う。
    pub const fn ros2idl() -> Self {
        NsMode::WithMiddle("msg")
    }

    /// ROS 2のDDSマッピング(rosidl生成IDL相当)の名前空間モード。
    /// ROS層の型名との衝突を防ぐため、dds_名前空間と型名suffix `_` を付与する。
    pub const fn ros2dds() -> Self {
        NsMode::WithMiddleSuffix {
            middle: "msg::dds_",
            suffix: "_",
        }
    }

    /// 中間名前空間(`::`区切り)。Rawではなし
    fn middle(&self) -> Option<&'static str> {
        match self {
            NsMode::Raw => None,
            NsMode::WithMiddle(mid) => Some(mid),
            NsMode::WithMiddleSuffix { middle, .. } => Some(middle),
        }
    }

    /// module節として開閉する中間セグメントの列挙(空セグメント除去済み)。
    /// `NsMode`はpub APIのため任意の`&'static str`(`WithMiddle("")`や
    /// `WithMiddle("a::::b")`)を渡せてしまい、無検証だと`module  {`のような
    /// 不正なIDLを出力していた。`idl_module_open`/`idl_module_close`の両方が必ずこの
    /// ヘルパを使うことで、開閉の階層数を一致させたまま空セグメントを取り除く
    /// (`WithMiddle("")`は実質`Raw`相当のmodule 1層のみになる)。
    fn middle_segments(&self) -> impl Iterator<Item = &'static str> {
        self.middle()
            .into_iter()
            .flat_map(|mid| mid.split("::"))
            .filter(|seg| !seg.is_empty())
    }

    /// 型名に付与するsuffix。suffix指定のないモードでは空文字
    fn type_suffix(&self) -> &'static str {
        match self {
            NsMode::WithMiddleSuffix { suffix, .. } => suffix,
            _ => "",
        }
    }

    /// IDLでの型宣言名。例: `ros2dds()`で "Point" → "Point_"
    pub fn idl_type_name(&self, name: &str) -> String {
        format!("{}{}", name, self.type_suffix())
    }

    /// IDLでの完全修飾型参照。例: `ros2dds()`で "geometry_msgs::msg::dds_::Point_"
    pub fn idl_type_ref(&self, package: &str, name: &str) -> String {
        match self.middle() {
            None => format!("{}::{}", package, self.idl_type_name(name)),
            Some(mid) => format!("{}::{}::{}", package, mid, self.idl_type_name(name)),
        }
    }

    /// IDLのmodule開き節。例: `WithMiddle("msg::dds_")` → `"module pkg { module msg { module dds_ {\n"`
    pub fn idl_module_open(&self, package: &str) -> String {
        let mut s = format!("module {} {{", package);
        for seg in self.middle_segments() {
            s.push_str(&format!(" module {} {{", seg));
        }
        s.push('\n');
        s
    }

    /// IDLのmodule閉じ節。`idl_module_open`で開いた階層数ぶんの `};` を返す
    pub fn idl_module_close(&self) -> String {
        let depth = 1 + self.middle_segments().count();
        let mut s = "};".repeat(depth);
        s.push('\n');
        s
    }
}

/// 依存型定義の収集器。(package, name) をキーに重複排除し、登録順（依存が先）を保持する。
///
/// `enter`/`finish`はフィールドへの再帰の前後で呼ぶ2段階のAPIになっている。
/// Why: `entries`には依存が先に来るトポロジカル順で積む必要があり、その登録
/// (`finish`)はフィールド再帰の**後**でなければならない。一方で「今たどっている経路上に
/// 同じ型が再度現れた（循環している）」ことは、フィールド再帰へ入る**前**の`enter`時点で
/// 検出しないと無限再帰になる（`struct Node { children: Vec<Node> }`のような自己参照や、
/// 型をまたいだ相互再帰A→B→Aが該当）。そのため「確定済み(`seen`)」と「現在たどっている
/// 経路(`visiting`)」を分けて持つ。
///
/// `seen`は(package, name)から確定済み定義本文へのマップにしている。
/// 以前は`HashSet<(String, String)>`で存在有無しか見ておらず、別々のRust型が同じ
/// (package, name)に解決された場合(re-exportではなく本当に異なる定義を持つ型が
/// 同名を名乗るケース)、内容を見ずに最初に visit した側の定義だけが黙って残り、
/// もう片方のフィールドは同じ型参照に解決されたまま定義が消えていた
/// (スキーマと実データが静かに食い違う)。定義本文まで比較することで、
/// 同一内容の再訪(正当な重複・re-export等)は従来通り無害にスキップしつつ、
/// 内容が食い違う場合だけpanicで検出する。
#[derive(Default)]
pub struct DefRegistry {
    seen: HashMap<(String, String), String>,
    visiting: Vec<(String, String)>,
    entries: Vec<String>,
    mode: NsMode,
}

impl DefRegistry {
    pub fn new(mode: NsMode) -> Self {
        Self {
            seen: HashMap::new(),
            visiting: Vec::new(),
            entries: Vec::new(),
            mode,
        }
    }

    /// `T`のフィールドへ再帰する前に呼ぶ。
    /// - 既に確定済みなら、`T::idl_def`を再計算して登録済み本文と比較する。同一内容なら
    ///   `false`を返す（呼び出し側はフィールド再帰も`finish`もスキップする）。内容が異なれば
    ///   別のRust型が同じ(package, name)に衝突しているとしてpanicする
    /// - 現在たどっている経路上に既に`T`があれば、ROS 2が表現できない循環参照として
    ///   経路を含むメッセージでpanicする
    /// - それ以外（初出）は経路に積んで`true`を返す
    pub fn enter<T: InterfaceDef>(&mut self) -> bool {
        let key = (T::package().to_string(), T::name().to_string());
        if let Some(existing) = self.seen.get(&key) {
            let candidate = T::idl_def(self.mode);
            if existing != &candidate {
                panic!(
                    "conflicting DdsInterface definitions for {}/{}:\n\
                     --- already registered ---\n\
                     {}\
                     --- conflicting ---\n\
                     {}\
                     Two distinct Rust types resolve to the same (package, name); rename one with #[cdds(name = \"...\")].",
                    key.0, key.1, existing, candidate
                );
            }
            return false;
        }
        if self.visiting.contains(&key) {
            panic!(
                "cyclic DdsInterface type definition detected: {}\n\
                 ROS 2 .msg/IDL cannot express recursive or mutually-recursive type definitions.",
                Self::format_cycle(&self.visiting, &key)
            );
        }
        self.visiting.push(key);
        true
    }

    /// `enter`が`true`を返した`T`について、フィールド再帰が終わった後に呼ぶ。
    /// 経路から外し、依存が先に来るトポロジカル順で`entries`へ確定登録する。
    pub fn finish<T: InterfaceDef>(&mut self) {
        let key = (T::package().to_string(), T::name().to_string());
        assert_eq!(
            self.visiting.last(),
            Some(&key),
            "DefRegistry::finish called out of order or without matching enter"
        );
        self.visiting.pop();
        let def = T::idl_def(self.mode);
        self.entries.push(def.clone());
        self.seen.insert(key, def);
    }

    /// 循環経路を`pkg::Name -> pkg::Name -> ... -> pkg::Name`の形で表示する
    fn format_cycle(visiting: &[(String, String)], repeated: &(String, String)) -> String {
        let start = visiting.iter().position(|k| k == repeated).unwrap_or(0);
        let mut path: Vec<String> = visiting[start..]
            .iter()
            .map(|(pkg, name)| format!("{}::{}", pkg, name))
            .collect();
        path.push(format!("{}::{}", repeated.0, repeated.1));
        path.join(" -> ")
    }
}

/// ROS 2連結メッセージ定義の依存先収集器。(package, name) をキーに重複排除し、
/// 最初に登録された1件目（＝呼び出し起点の自身）と、それ以降（＝依存先）を区別してレンダリングする。
///
/// `DefRegistry`と同様、「確定済み(`seen`)」と「現在たどっている経路(`visiting`)」を
/// 分けて持つ。こちらは登場順（自身→出現順の依存先）で`entries`へ積むため`enter`時点で
/// pushする点が`DefRegistry`と異なるが、循環検出のために`enter`/`finish`で経路を
/// 管理する構造は共通にしてある。
#[derive(Default)]
pub struct MsgDefRegistry {
    seen: HashMap<(String, String), String>,
    visiting: Vec<(String, String)>,
    entries: Vec<(String, String)>,
}

impl MsgDefRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// `T`のフィールドへ再帰する前に呼ぶ。
    /// - 既に確定済みなら、`T::msg_def`を再計算して登録済み本文と比較する
    ///   (`DefRegistry::enter`と同じ理由)。同一内容なら`false`を返す（呼び出し側は
    ///   フィールド再帰も`finish`もスキップする）。内容が異なれば別のRust型が同じ
    ///   (package, name)に衝突しているとしてpanicする
    /// - 現在たどっている経路上に既に`T`があれば、循環参照として経路を含むメッセージでpanicする
    /// - それ以外（初出）は経路に積み、自身の定義を出現順で`entries`へ積んで`true`を返す
    pub fn enter<T: InterfaceDef>(&mut self) -> bool {
        let key = (T::package().to_string(), T::name().to_string());
        if let Some(existing) = self.seen.get(&key) {
            let candidate = T::msg_def();
            if existing != &candidate {
                panic!(
                    "conflicting DdsInterface definitions for {}/{}:\n\
                     --- already registered ---\n\
                     {}\
                     --- conflicting ---\n\
                     {}\
                     Two distinct Rust types resolve to the same (package, name); rename one with #[cdds(name = \"...\")].",
                    key.0, key.1, existing, candidate
                );
            }
            return false;
        }
        if self.visiting.contains(&key) {
            panic!(
                "cyclic DdsInterface type definition detected: {}\n\
                 ROS 2 .msg/IDL cannot express recursive or mutually-recursive type definitions.",
                DefRegistry::format_cycle(&self.visiting, &key)
            );
        }
        self.visiting.push(key);
        self.entries.push((T::msg_ref(), T::msg_def()));
        true
    }

    /// `enter`が`true`を返した`T`について、フィールド再帰が終わった後に呼ぶ。経路から外す。
    pub fn finish<T: InterfaceDef>(&mut self) {
        let key = (T::package().to_string(), T::name().to_string());
        assert_eq!(
            self.visiting.last(),
            Some(&key),
            "MsgDefRegistry::finish called out of order or without matching enter"
        );
        self.visiting.pop();
        self.seen.insert(key, T::msg_def());
    }

    /// 1件目はヘッダなし（呼び出し起点自身の定義）、以降は区切り線+`MSG: pkg/Name`ヘッダ付きで連結する
    fn render(&self) -> String {
        const SEPARATOR: &str = "=";
        let mut s = String::new();
        for (i, (msg_ref, msg_def)) in self.entries.iter().enumerate() {
            if i == 0 {
                s.push_str(msg_def);
            } else {
                s.push_str(&SEPARATOR.repeat(80));
                s.push('\n');
                s.push_str(&format!("MSG: {}\n", msg_ref));
                s.push_str(msg_def);
            }
        }
        s
    }
}

// ---- プリミティブ実装 ----

macro_rules! impl_interface_ref_primitive {
    ($($ty:ty => ($idl:expr, $msg:expr)),+ $(,)?) => {
        $(
            impl InterfaceRef for $ty {
                fn idl_ref(_mode: NsMode) -> String { $idl.to_string() }
                fn msg_ref() -> String { $msg.to_string() }
            }
            impl InterfaceElem for $ty {}
        )+
    };
}

impl_interface_ref_primitive! {
    bool => ("boolean", "bool"),
    // octetは符号なし1byte。符号ありの1byteはIDL 4.xのint8を使う
    u8 => ("octet", "uint8"),
    i8 => ("int8", "int8"),
    i16 => ("short", "int16"),
    u16 => ("unsigned short", "uint16"),
    i32 => ("long", "int32"),
    u32 => ("unsigned long", "uint32"),
    i64 => ("long long", "int64"),
    u64 => ("unsigned long long", "uint64"),
    f32 => ("float", "float32"),
    f64 => ("double", "float64"),
    String => ("string", "string"),
}

// ---- Vec<T>（可変長配列） ----
// Why T: InterfaceElem（InterfaceRefではなく）: ROS 2 .msgにはT[][]という多段シーケンスの
// 記法が存在しないため、要素TがVec<U>/[U;M]自身であること（Vec<Vec<T>>等）を型システムで
// 拒否する。Vec<T>自身はInterfaceElemを実装しないため、この制約は再帰的に効く。

impl<T: InterfaceElem> InterfaceRef for Vec<T> {
    fn idl_ref(mode: NsMode) -> String {
        format!("sequence<{}>", T::idl_ref(mode))
    }
    fn msg_ref() -> String {
        format!("{}[]", T::msg_ref())
    }
    fn collect_defs(reg: &mut DefRegistry) {
        T::collect_defs(reg);
    }
    fn collect_msg_defs(reg: &mut MsgDefRegistry) {
        T::collect_msg_defs(reg);
    }
}

/// ---- [T; N]（固定長配列） ----
/// Why T: InterfaceElem: `Vec<T>`と同じ理由。`[[T;N];M]`のような多段配列を型システムで拒否する。
///
/// `[T; 0]`はROS 2 .msg(`uint8[0] a`)・IDL(`octet a[0];`)のどちらでも表現できない
/// (rosidlの固定長配列サイズもIDLの配列境界も正の整数が必須)。Nはmonomorphize時点の
/// コンパイル時定数なので、各メソッド先頭のinline const blockでN>0を検査すれば、
/// `<[u8; 0]>::msg_ref()`等を実際に呼ぶコードがコンパイルされた時点でエラーになる。
///
/// ```compile_fail
/// # use cyclonedds_rs::InterfaceRef;
/// let _ = <[u8; 0]>::msg_ref(); // N=0はpost-monomorphizationエラーになる
/// ```
impl<T: InterfaceElem, const N: usize> InterfaceRef for [T; N] {
    fn idl_ref(mode: NsMode) -> String {
        const {
            assert!(
                N > 0,
                "zero-length arrays cannot be represented in ROS 2 .msg/IDL"
            )
        };
        T::idl_ref(mode)
    }
    fn msg_ref() -> String {
        const {
            assert!(
                N > 0,
                "zero-length arrays cannot be represented in ROS 2 .msg/IDL"
            )
        };
        format!("{}[{}]", T::msg_ref(), N)
    }
    fn idl_field(name: &str, mode: NsMode) -> String {
        const {
            assert!(
                N > 0,
                "zero-length arrays cannot be represented in ROS 2 .msg/IDL"
            )
        };
        format!("{} {}[{}];", T::idl_ref(mode), name, N)
    }
    fn collect_defs(reg: &mut DefRegistry) {
        const {
            assert!(
                N > 0,
                "zero-length arrays cannot be represented in ROS 2 .msg/IDL"
            )
        };
        T::collect_defs(reg);
    }
    fn collect_msg_defs(reg: &mut MsgDefRegistry) {
        const {
            assert!(
                N > 0,
                "zero-length arrays cannot be represented in ROS 2 .msg/IDL"
            )
        };
        T::collect_msg_defs(reg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Why: 型対応表(IDL/.msg)通りの文字列が返ることを保証するため
    // Method: 全プリミティブの実測値と期待値をリスト化してexpect-actual比較する
    #[test]
    fn primitive_type_refs_match_mapping_table() {
        let nsmode = NsMode::Raw;
        let actual = vec![
            (bool::idl_ref(nsmode), bool::msg_ref()),
            (u8::idl_ref(nsmode), u8::msg_ref()),
            (i8::idl_ref(nsmode), i8::msg_ref()),
            (i16::idl_ref(nsmode), i16::msg_ref()),
            (u16::idl_ref(nsmode), u16::msg_ref()),
            (i32::idl_ref(nsmode), i32::msg_ref()),
            (u32::idl_ref(nsmode), u32::msg_ref()),
            (i64::idl_ref(nsmode), i64::msg_ref()),
            (u64::idl_ref(nsmode), u64::msg_ref()),
            (f32::idl_ref(nsmode), f32::msg_ref()),
            (f64::idl_ref(nsmode), f64::msg_ref()),
            (String::idl_ref(nsmode), String::msg_ref()),
        ];
        let expect = vec![
            ("boolean", "bool"),
            ("octet", "uint8"),
            ("int8", "int8"),
            ("short", "int16"),
            ("unsigned short", "uint16"),
            ("long", "int32"),
            ("unsigned long", "uint32"),
            ("long long", "int64"),
            ("unsigned long long", "uint64"),
            ("float", "float32"),
            ("double", "float64"),
            ("string", "string"),
        ]
        .into_iter()
        .map(|(idl, msg)| (idl.to_string(), msg.to_string()))
        .collect::<Vec<_>>();
        assert_eq!(actual, expect);
    }

    // Why: WithMiddleは`::`区切りで複数階層を表せる仕様のため、
    //      module節の開き/閉じが階層数に応じてネストされることを確認する
    #[test]
    fn ns_mode_module_brackets_nest_per_middle_segment() {
        assert_eq!(NsMode::Raw.idl_module_open("pkg"), "module pkg {\n");
        assert_eq!(NsMode::Raw.idl_module_close(), "};\n");
        assert_eq!(
            NsMode::WithMiddle("msg").idl_module_open("pkg"),
            "module pkg { module msg {\n"
        );
        assert_eq!(NsMode::WithMiddle("msg").idl_module_close(), "};};\n");
        assert_eq!(
            NsMode::WithMiddle("msg::dds_").idl_module_open("pkg"),
            "module pkg { module msg { module dds_ {\n"
        );
        assert_eq!(
            NsMode::WithMiddle("msg::dds_").idl_module_close(),
            "};};};\n"
        );
        // WithMiddleSuffixのmiddleもWithMiddleと同様にネストされる
        assert_eq!(
            NsMode::ros2dds().idl_module_open("pkg"),
            "module pkg { module msg { module dds_ {\n"
        );
        assert_eq!(NsMode::ros2dds().idl_module_close(), "};};};\n");
    }

    // Why: `NsMode`はpub APIのため、`WithMiddle("")`(空セグメント)や
    //      `WithMiddle("a::::b")`(セグメント間の空文字列)のような逸脱値を渡せてしまい、
    //      以前は無検証で`module  {`のような不正なIDLを出力していた。空セグメントが
    //      除去され、かつopen/closeの階層数が一致すること(開いた数だけ閉じる)を保証する
    #[test]
    fn ns_mode_with_middle_ignores_empty_segments() {
        // 空文字列は実質Rawと同じ(module 1層のみ)になる
        assert_eq!(
            NsMode::WithMiddle("").idl_module_open("pkg"),
            "module pkg {\n"
        );
        assert_eq!(NsMode::WithMiddle("").idl_module_close(), "};\n");

        // セグメント間の空文字列は無視され、"a"と"b"の2階層になる
        assert_eq!(
            NsMode::WithMiddle("a::::b").idl_module_open("pkg"),
            "module pkg { module a { module b {\n"
        );
        assert_eq!(NsMode::WithMiddle("a::::b").idl_module_close(), "};};};\n");
    }

    // Why: ROS 2のDDSマッピングではROS層との型名衝突を防ぐため型名にsuffix `_` を付ける規約があり、
    //      宣言名(idl_type_name)と参照(idl_type_ref)の両方へ一貫して付与されることを確認する
    #[test]
    fn ns_mode_type_suffix_applies_to_name_and_ref() {
        assert_eq!(NsMode::Raw.idl_type_name("Point"), "Point");
        assert_eq!(NsMode::Raw.idl_type_ref("pkg", "Point"), "pkg::Point");
        assert_eq!(
            NsMode::ros2idl().idl_type_ref("pkg", "Point"),
            "pkg::msg::Point"
        );
        assert_eq!(NsMode::ros2dds().idl_type_name("Point"), "Point_");
        assert_eq!(
            NsMode::ros2dds().idl_type_ref("pkg", "Point"),
            "pkg::msg::dds_::Point_"
        );
    }

    // Why: idl_fieldのデフォルト実装が "型 名前;" の形式になることを確認するため
    // Method: 代表的な2型で実測値と期待値を比較する
    #[test]
    fn idl_field_default_impl_joins_type_and_name() {
        assert_eq!(
            u32::idl_field("id", NsMode::Raw).as_str(),
            "unsigned long id;"
        );
        assert_eq!(
            f64::idl_field("temperature", NsMode::Raw).as_str(),
            "double temperature;"
        );
    }

    // Why: Vec<T>がIDLのsequence<T>、.msgのT[]に変換されることを確認するため
    #[test]
    fn vec_maps_to_idl_sequence_and_msg_array() {
        assert_eq!(Vec::<f32>::idl_ref(NsMode::Raw).as_str(), "sequence<float>");
        assert_eq!(Vec::<f32>::msg_ref().as_str(), "float32[]");
    }

    // Why: [T; N]はIDLではフィールド名側にサイズが付く点がVec<T>と異なるため、
    //      idl_field のオーバーライドを含めて仕様通りか確認する
    #[test]
    fn fixed_array_places_size_on_field_for_idl_and_on_type_for_msg() {
        assert_eq!(<[f64; 16]>::idl_ref(NsMode::Raw).as_str(), "double");
        assert_eq!(<[f64; 16]>::msg_ref().as_str(), "float64[16]");
        assert_eq!(
            <[f64; 16]>::idl_field("matrix", NsMode::Raw).as_str(),
            "double matrix[16];"
        );
    }

    // deriveマクロ未実装のStep1時点でDefRegistryの挙動を検証するための手動実装フィクスチャ。
    // Pointに依存するPoseを介して、依存順序・重複排除・idl()/ros2_msg()の差異を確認する。
    struct Point;
    impl InterfaceRef for Point {
        fn idl_ref(mode: NsMode) -> String {
            mode.idl_type_ref("geo", "Point")
        }
        fn msg_ref() -> String {
            "geo/Point".to_string()
        }
        fn collect_defs(reg: &mut DefRegistry) {
            if reg.enter::<Point>() {
                reg.finish::<Point>();
            }
        }
        fn collect_msg_defs(reg: &mut MsgDefRegistry) {
            if reg.enter::<Point>() {
                reg.finish::<Point>();
            }
        }
    }
    impl InterfaceDef for Point {
        fn package() -> &'static str {
            "geo"
        }
        fn name() -> &'static str {
            "Point"
        }
        fn idl_def(mode: NsMode) -> String {
            match mode {
                NsMode::Raw => "struct Point { double x; double y; };\n".to_string(),
                _ => {
                    format!(
                        "{}struct {} {{ double x; double y; }};\n{}",
                        mode.idl_module_open("geometry_msgs"),
                        mode.idl_type_name("Point"),
                        mode.idl_module_close()
                    )
                }
            }
        }
        fn msg_def() -> String {
            "float64 x\nfloat64 y\n".to_string()
        }
    }

    struct Pose;
    impl InterfaceRef for Pose {
        fn idl_ref(mode: NsMode) -> String {
            mode.idl_type_ref("geo", "Pose")
        }
        fn msg_ref() -> String {
            "geo/Pose".to_string()
        }
        fn collect_defs(reg: &mut DefRegistry) {
            // 自身を先にvisiting経路へ積んでから（循環検出のため）フィールドへ再帰し、
            // 依存先を複数フィールドから2回参照しても重複登録されないことを確認する
            if reg.enter::<Pose>() {
                Point::collect_defs(reg);
                Point::collect_defs(reg);
                reg.finish::<Pose>();
            }
        }
        fn collect_msg_defs(reg: &mut MsgDefRegistry) {
            // 自身を先に登録してから、初出の依存先のみ再帰する（生成コードと同じ順序）
            if reg.enter::<Pose>() {
                Point::collect_msg_defs(reg);
                Point::collect_msg_defs(reg);
                reg.finish::<Pose>();
            }
        }
    }
    impl InterfaceDef for Pose {
        fn package() -> &'static str {
            "geo"
        }
        fn name() -> &'static str {
            "Pose"
        }
        fn idl_def(_mode: NsMode) -> String {
            "struct Pose { Point position; Point orientation; };\n".to_string()
        }
        fn msg_def() -> String {
            "geo/Point position\ngeo/Point orientation\n".to_string()
        }
    }

    // Why: idl()が依存先を重複なく先に並べ、ros2_msg()は自身の定義のみを返す
    //      という仕様(依存関係の解決範囲がIDLと.msgで異なる)を確認するため
    #[test]
    fn idl_orders_dependencies_first_and_dedups_while_ros2_msg_is_self_only() {
        assert_eq!(
            Pose::idl(NsMode::Raw),
            "struct Point { double x; double y; };\n\
             struct Pose { Point position; Point orientation; };\n"
                .to_string()
        );
        assert_eq!(Pose::ros2_msg(), Pose::msg_def());
    }

    // Why: mcap/rosbag2向けの連結メッセージ定義は「自身の定義（ヘッダなし）→
    //      依存先を出現順・重複排除しつつ区切り線+MSGヘッダ付きで列挙」という
    //      仕様通りに組み立てられることを確認するため
    #[test]
    fn full_ros2_msg_lists_self_first_then_deps_with_headers() {
        let expect = "geo/Point position\ngeo/Point orientation\n\
             ================================================================================\n\
             MSG: geo/Point\n\
             float64 x\nfloat64 y\n"
            .to_string();
        assert_eq!(Pose::full_ros2_msg(), expect);
    }

    // 自己参照(`struct Node { children: Vec<Node> }`相当)を模したフィクスチャ。
    // derive生成コードと同じ「enterしてからフィールドへ再帰しfinishする」順序で書く。
    struct Node;
    impl InterfaceRef for Node {
        fn idl_ref(mode: NsMode) -> String {
            mode.idl_type_ref("geo", "Node")
        }
        fn msg_ref() -> String {
            "geo/Node".to_string()
        }
        fn collect_defs(reg: &mut DefRegistry) {
            if reg.enter::<Node>() {
                Node::collect_defs(reg);
                reg.finish::<Node>();
            }
        }
        fn collect_msg_defs(reg: &mut MsgDefRegistry) {
            if reg.enter::<Node>() {
                Node::collect_msg_defs(reg);
                reg.finish::<Node>();
            }
        }
    }
    impl InterfaceDef for Node {
        fn package() -> &'static str {
            "geo"
        }
        fn name() -> &'static str {
            "Node"
        }
        fn idl_def(_mode: NsMode) -> String {
            "struct Node { sequence<Node> children; };\n".to_string()
        }
        fn msg_def() -> String {
            "geo/Node[] children\n".to_string()
        }
    }

    // 相互再帰(A→B→A)を模したフィクスチャ。単純な自己参照だけでなく、
    // 型をまたいだ循環も検出できることを確認するため。
    struct CycleA;
    struct CycleB;
    impl InterfaceRef for CycleA {
        fn idl_ref(mode: NsMode) -> String {
            mode.idl_type_ref("geo", "CycleA")
        }
        fn msg_ref() -> String {
            "geo/CycleA".to_string()
        }
        fn collect_defs(reg: &mut DefRegistry) {
            if reg.enter::<CycleA>() {
                CycleB::collect_defs(reg);
                reg.finish::<CycleA>();
            }
        }
    }
    impl InterfaceDef for CycleA {
        fn package() -> &'static str {
            "geo"
        }
        fn name() -> &'static str {
            "CycleA"
        }
        fn idl_def(_mode: NsMode) -> String {
            "struct CycleA { CycleB b; };\n".to_string()
        }
        fn msg_def() -> String {
            "geo/CycleB b\n".to_string()
        }
    }
    impl InterfaceRef for CycleB {
        fn idl_ref(mode: NsMode) -> String {
            mode.idl_type_ref("geo", "CycleB")
        }
        fn msg_ref() -> String {
            "geo/CycleB".to_string()
        }
        fn collect_defs(reg: &mut DefRegistry) {
            if reg.enter::<CycleB>() {
                CycleA::collect_defs(reg);
                reg.finish::<CycleB>();
            }
        }
    }
    impl InterfaceDef for CycleB {
        fn package() -> &'static str {
            "geo"
        }
        fn name() -> &'static str {
            "CycleB"
        }
        fn idl_def(_mode: NsMode) -> String {
            "struct CycleB { CycleA a; };\n".to_string()
        }
        fn msg_def() -> String {
            "geo/CycleA a\n".to_string()
        }
    }

    // Why: 自己参照型は無限再帰でクラッシュ(stack overflow/abort)するのではなく、
    //      ROS 2 .msg/IDLが表現できない循環参照として明確なpanicで検出されるべきため
    // Method: idl()呼び出しがpanicし、メッセージに循環経路(geo::Node -> geo::Node)が
    //         含まれることを確認する
    #[test]
    #[should_panic(expected = "geo::Node -> geo::Node")]
    fn self_referential_type_panics_with_cycle_path_in_collect_defs() {
        let _ = Node::idl(NsMode::Raw);
    }

    // Why: collect_msg_defs側（full_ros2_msg/ros2_msgの経路）は元々register-firstの順序で
    //      無限再帰こそしないが、代わりに自己参照する不正な.msgを黙って返してしまっていた。
    //      visiting導入後はこちらも同じ理由でpanicするべきことを確認するため
    #[test]
    #[should_panic(expected = "geo::Node -> geo::Node")]
    fn self_referential_type_panics_with_cycle_path_in_collect_msg_defs() {
        let _ = Node::full_ros2_msg();
    }

    // Why: 直接の自己参照だけでなく、型をまたいだ相互再帰(A→B→A)も同じ仕組みで
    //      検出できることを確認するため
    #[test]
    #[should_panic(expected = "geo::CycleA -> geo::CycleB -> geo::CycleA")]
    fn mutually_recursive_types_panic_with_full_cycle_path() {
        let _ = CycleA::idl(NsMode::Raw);
    }

    // Why: collect_defs()のenter/finishの呼び出し順序が正しくない場合、panicすることを確認するため
    //      異なる型でfinishを呼ぶケースと、enterなしでfinishを呼ぶケースの両方を確認する
    #[test]
    #[should_panic(expected = "DefRegistry::finish called out of order or without matching enter")]
    fn def_registry_finish_with_mismatched_type_panics() {
        let mut reg = DefRegistry::new(NsMode::Raw);
        assert!(reg.enter::<Pose>());
        reg.finish::<Point>();

        let mut reg = DefRegistry::new(NsMode::Raw);
        reg.finish::<Point>();
    }

    // Why: MsgDefRegistryもDefRegistryと同じvisitingスタック構造を持つため、
    //      finishでの不変条件チェックが同様に効くことを確認する。
    #[test]
    #[should_panic(
        expected = "MsgDefRegistry::finish called out of order or without matching enter"
    )]
    fn msg_def_registry_finish_with_mismatched_type_panics() {
        let mut reg = MsgDefRegistry::new();
        assert!(reg.enter::<Pose>());
        reg.finish::<Point>();

        let mut reg = MsgDefRegistry::new();
        reg.finish::<Point>();
    }

    // 同じ(package, name) = ("conf", "Dup")に解決される、内容が異なる2つの手動実装フィクスチャ。
    // 実際には別モジュールの別struct 2つが同名でderiveされたケースを模している。
    struct DupA;
    impl InterfaceRef for DupA {
        fn idl_ref(mode: NsMode) -> String {
            mode.idl_type_ref("conf", "Dup")
        }
        fn msg_ref() -> String {
            "conf/Dup".to_string()
        }
    }
    impl InterfaceDef for DupA {
        fn package() -> &'static str {
            "conf"
        }
        fn name() -> &'static str {
            "Dup"
        }
        fn idl_def(_mode: NsMode) -> String {
            "struct Dup { double x; };\n".to_string()
        }
        fn msg_def() -> String {
            "float64 x\n".to_string()
        }
    }

    struct DupB;
    impl InterfaceRef for DupB {
        fn idl_ref(mode: NsMode) -> String {
            mode.idl_type_ref("conf", "Dup")
        }
        fn msg_ref() -> String {
            "conf/Dup".to_string()
        }
    }
    impl InterfaceDef for DupB {
        fn package() -> &'static str {
            "conf"
        }
        fn name() -> &'static str {
            "Dup"
        }
        fn idl_def(_mode: NsMode) -> String {
            "struct Dup { string name;\n  unsigned long retries; };\n".to_string()
        }
        fn msg_def() -> String {
            "string name\nuint32 retries\n".to_string()
        }
    }

    // DupAと同じ(package, name)・同じ内容を持つフィクスチャ。re-export等、同じ定義への
    // 正当な再訪を模している(こちらはpanicせず無害にdedupされるべき)。
    struct DupC;
    impl InterfaceRef for DupC {
        fn idl_ref(mode: NsMode) -> String {
            mode.idl_type_ref("conf", "Dup")
        }
        fn msg_ref() -> String {
            "conf/Dup".to_string()
        }
    }
    impl InterfaceDef for DupC {
        fn package() -> &'static str {
            "conf"
        }
        fn name() -> &'static str {
            "Dup"
        }
        fn idl_def(_mode: NsMode) -> String {
            "struct Dup { double x; };\n".to_string()
        }
        fn msg_def() -> String {
            "float64 x\n".to_string()
        }
    }

    // Why: 以前は(package, name)の重複を内容を見ずにHashSetで判定していたため、
    //      別々のRust型が同じ(package, name)に解決されると、最初にvisitした側の定義だけが
    //      黙って残りもう片方の定義が消えていた(スキーマと実データの静かな不整合)。
    //      内容が食い違う場合はpanicで検出されることを保証する
    #[test]
    #[should_panic(expected = "conflicting DdsInterface definitions for conf/Dup")]
    fn def_registry_panics_on_conflicting_definitions_for_same_key() {
        let mut reg = DefRegistry::new(NsMode::Raw);
        assert!(reg.enter::<DupA>());
        reg.finish::<DupA>();
        reg.enter::<DupB>();
    }

    // Why: MsgDefRegistry側(full_ros2_msg/ros2_msgの経路)も同じ理由でpanicするべき
    #[test]
    #[should_panic(expected = "conflicting DdsInterface definitions for conf/Dup")]
    fn msg_def_registry_panics_on_conflicting_definitions_for_same_key() {
        let mut reg = MsgDefRegistry::new();
        assert!(reg.enter::<DupA>());
        reg.finish::<DupA>();
        reg.enter::<DupB>();
    }

    // Why: 内容まで完全に一致する重複(同一型の再訪・re-export等)は従来通り
    //      無害にスキップされ、panicにもentriesの重複追加にもならないことを保証する
    #[test]
    fn def_registry_same_content_duplicate_is_deduped_without_panic() {
        let mut reg = DefRegistry::new(NsMode::Raw);
        assert!(reg.enter::<DupA>());
        reg.finish::<DupA>();
        assert!(!reg.enter::<DupC>());
        assert_eq!(reg.entries.len(), 1);
    }

    #[test]
    fn msg_def_registry_same_content_duplicate_is_deduped_without_panic() {
        let mut reg = MsgDefRegistry::new();
        assert!(reg.enter::<DupA>());
        reg.finish::<DupA>();
        assert!(!reg.enter::<DupC>());
        assert_eq!(reg.entries.len(), 1);
    }
}
