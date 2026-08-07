//! deriveマクロの生成コードが呼び出し側の型シャドーイングに耐えることを確認する敵対的テスト
//!
//! proc-macroの出力トークンは呼び出し側モジュールの名前解決に従う(call-site hygiene)。
//! ROS型名がpreludeの型名と一致する場合(例: std_msgs/String由来の`pub struct String`)、
//! 生成コードが`String`等を非修飾で参照していると同一モジュール内の全deriveが
//! E0053/E0308/E0599でビルド不可となる。ここでは実在するstd_msgs/Stringを模した衝突環境と、
//! prelude型を網羅的にシャドーした環境の両方でderiveが機能することを確認する。

#![allow(dead_code)]

use cyclonedds_rs::*;

/// std_msgs/String相当の生成コードを模したモジュール。
/// この`String`定義自体がpreludeの`std::string::String`をモジュール内でシャドーする。
mod std_msgs {
    use cyclonedds_rs::*;

    #[derive(Default, Deserialize, Serialize, PartialEq, Clone, Topic, DdsInterface)]
    #[cdds(package = "std_msgs")]
    pub struct String {
        // Why 完全修飾: このモジュールでは`String`がROS型に解決されるため、
        // ROSプリミティブstringのフィールドは絶対パスで書く必要がある(生成側の規約)
        #[topic_key]
        pub data: ::std::string::String,
    }

    /// シャドーされた`String`をROS型(std_msgs/String)として参照する依存側メッセージ
    #[derive(DdsInterface)]
    #[cdds(package = "test_msgs")]
    pub struct LogEntry {
        pub msg: String,
        pub count: u32,
    }
}

/// prelude型・stdモジュール名を同名のダミーで網羅的にシャドーしたモジュール。
/// 生成コードに非修飾参照が残っていればここでのderiveはコンパイルに失敗する。
mod prelude_shadow {
    use cyclonedds_rs::*;

    pub struct Vec;
    pub struct Option;
    pub struct Result;
    pub struct From;
    pub struct Default;
    pub mod std {}

    #[derive(
        ::std::default::Default, Deserialize, Serialize, PartialEq, Clone, Topic, DdsInterface,
    )]
    #[cdds(package = "adversarial_msgs")]
    pub struct Sensor {
        #[topic_key]
        pub id: u32,
        pub values: ::std::vec::Vec<f32>,
    }
}

/// name override属性の確認用。Rust側は衝突回避のため`StringMsg`へ改名し、
/// ワイヤ上の型名は`#[cdds(name = "String")]`で維持する。
mod renamed {
    use cyclonedds_rs::*;

    #[derive(DdsInterface)]
    #[cdds(package = "std_msgs", name = "String")]
    pub struct StringMsg {
        pub data: String,
    }

    #[derive(DdsInterface)]
    #[cdds(package = "test_msgs")]
    pub struct RenamedDependent {
        pub msg: StringMsg,
    }
}

// Why: `String`がシャドーされたモジュール内でも、同居する全deriveが.msg/IDLを
//      正しく出力できること(生成コードの完全修飾)を保証するため
// Method: 自身(String)と依存側(LogEntry)の各出力を期待文字列と全文比較する
#[test]
fn shadowed_string_module_outputs_expected_definitions() {
    assert_eq!(std_msgs::String::ros2_msg(), "string data\n");
    assert_eq!(
        <std_msgs::String as InterfaceRef>::msg_ref(),
        "std_msgs/String"
    );

    let expect_idl = "\
module std_msgs { module msg {
struct String {
  @key string data;
};
};};
";
    assert_eq!(std_msgs::String::idl(NsMode::ros2idl()), expect_idl);

    let expect_full = "\
std_msgs/String msg
uint32 count
================================================================================
MSG: std_msgs/String
string data
";
    assert_eq!(std_msgs::LogEntry::full_ros2_msg(), expect_full);
}

// Why: Topic deriveでも生成コードがシャドーに耐え、完全修飾された
//      `::std::string::String`キーがプリミティブ・可変長として分類されることを保証するため
// Method: key_cdr()を同レイアウトのミラー構造体のcdr直列化結果と比較し、
//         可変長キー判定(force_md5_keyhash)も確認する
#[test]
fn shadowed_string_topic_key_is_variable_length_primitive() {
    #[derive(serde::Serialize)]
    struct KeyMirror {
        data: String,
    }

    let sample = std_msgs::String {
        data: "hello".to_string(),
    };
    let expect = cdr::serialize::<_, _, cdr::CdrBe>(
        &KeyMirror {
            data: "hello".to_string(),
        },
        cdr::Infinite,
    )
    .unwrap();
    assert_eq!(sample.key_cdr(), expect);
    assert!(std_msgs::String::has_key());
    assert!(std_msgs::String::force_md5_keyhash());
}

// Why: Vec/Option/Result/From/Defaultとmod stdを同時にシャドーしても
//      両deriveがコンパイル・動作すること(=非修飾参照が残っていないこと)を保証するため
// Method: .msg出力とキー直列化(固定長キー)を期待値と比較する
#[test]
fn prelude_shadow_module_still_compiles_and_works() {
    assert_eq!(
        prelude_shadow::Sensor::ros2_msg(),
        "uint32 id\nfloat32[] values\n"
    );

    #[derive(serde::Serialize)]
    struct KeyMirror {
        id: u32,
    }
    let sample = prelude_shadow::Sensor {
        id: 7,
        values: vec![1.0],
    };
    let expect = cdr::serialize::<_, _, cdr::CdrBe>(&KeyMirror { id: 7 }, cdr::Infinite).unwrap();
    assert_eq!(sample.key_cdr(), expect);
    assert!(!prelude_shadow::Sensor::force_md5_keyhash());
}

/// 別モジュールのpub structをキーにするフィクスチャ。
/// 以前はKeyHolder_structが常に非pubで生成されていたため、他モジュールの構造体を
/// `#[topic_key]`にすると生成コードが参照する`PointKeyHolder_`がE0603
/// (privateなstructの外部参照)になっていた。元structの可視性を引き継ぐことで解消する。
mod geometry {
    use cyclonedds_rs::*;

    #[derive(Default, Deserialize, Serialize, PartialEq, Clone, Topic)]
    pub struct Point {
        #[topic_key]
        pub id: u32,
    }
}

mod cross_mod_key {
    use cyclonedds_rs::*;

    #[derive(Default, Deserialize, Serialize, PartialEq, Clone, Topic)]
    pub struct CrossModKey {
        #[topic_key]
        pub p: super::geometry::Point,
    }
}

// Why: 別モジュールのpub structをキーにするコンパイルが通り、実際にkey_cdr()まで
//      呼べることを保証するため(コンパイルが通ること自体が回帰検知の主目的)
#[test]
fn cross_module_struct_key_compiles_and_computes_key_cdr() {
    assert!(cross_mod_key::CrossModKey::has_key());
    let sample = cross_mod_key::CrossModKey {
        p: geometry::Point { id: 42 },
    };
    assert!(!sample.key_cdr().is_empty());
}

// Why: name override属性でRust名とワイヤ名を分離でき、依存側からの参照名も
//      上書き後の型名になることを保証するため
// Method: msg_ref/name/idl/full_ros2_msgの各出力を期待値と全文比較する
#[test]
fn name_override_replaces_wire_name_everywhere() {
    assert_eq!(
        <renamed::StringMsg as InterfaceRef>::msg_ref(),
        "std_msgs/String"
    );
    assert_eq!(<renamed::StringMsg as InterfaceDef>::name(), "String");
    assert_eq!(renamed::StringMsg::ros2_msg(), "string data\n");

    let expect_idl = "\
module std_msgs { module msg {
struct String {
  string data;
};
};};
";
    assert_eq!(renamed::StringMsg::idl(NsMode::ros2idl()), expect_idl);

    let expect_full = "\
std_msgs/String msg
================================================================================
MSG: std_msgs/String
string data
";
    assert_eq!(renamed::RenamedDependent::full_ros2_msg(), expect_full);
}
