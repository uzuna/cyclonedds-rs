//! DdsInterfaceのderiveを通して、ネストされた構造体からIDL/.msgが正しく合成されることを確認する統合テスト
//!
//! NOTE: IDLはまだ使う予定がないがDDSの表現上有利な点もあるので実装。
//! 細かい使用感は追って調整する。
//!
//! OMG IDL仕様: https://www.omg.org/spec/IDL/4.0
//! ROS 2のIDL仕様: https://design.ros2.org/articles/idl_interface_definition.html
//! ROS 2でのIDL例: https://github.com/ros2/rosidl/blob/rolling/rosidl_adapter/test/data/msg/Test.expected.idl
//!
//! ROS 2の.msg仕様: https://design.ros2.org/articles/interface_definition.html
//! MCAPのMSG埋め込み仕様: https://docs.ros.org/en/foxy/Concepts/About-ROS-Interfaces.html#message-description-specification

#![allow(dead_code)]

use cdds_derive::DdsInterface;
use cyclonedds_rs::*;

#[derive(DdsInterface)]
#[cdds(package = "geometry_msgs")]
struct Point {
    x: f64,
    y: f64,
}

#[derive(DdsInterface)]
#[cdds(package = "my_robot_interfaces")]
struct Pose {
    position: Point,
    orientation: Point,
}

#[derive(DdsInterface)]
#[cdds(package = "my_robot_interfaces")]
struct RobotStatus {
    status_code: u8,
    battery_level: f64,
    position: Point,
    history: Vec<f32>,
    matrix: [f64; 16],
}

/// shape_msgs/SolidPrimitive等に実在する予約語フィールドを模したフィクスチャ
#[derive(DdsInterface)]
#[cdds(package = "test_msgs")]
struct KeywordFields {
    r#type: u8,
    r#loop: u32,
    r#ref: String,
    normal_name: f64,
}

/// 実在するROS 2パッケージ名(map)がIDL/C/C++キーワードとも衝突するケースのフィクスチャ。
/// パッケージ名は`_`で終われず(REP 144)リネーム以外の回避手段が無いため、
/// 拒否ではなくIDL4のエスケープ識別子(先頭`_`)で解決する
#[derive(DdsInterface)]
#[cdds(package = "map")]
struct MapPose {
    x: f64,
}

// Why: パッケージ名の予約語衝突をcompile_errorで拒否すると、実在するROS 2パッケージ名
//      (map/object/port等)がリネーム以外の回避手段を持てなくなる。IDLのmodule識別子/型参照
//      にのみエスケープ済み名前を使い、ROS 2 .msgパス(msg_ref)やpackage()が返す
//      実際のパッケージ名は変えないことを確認する
// Method: idl()のmodule宣言・msg_ref()・package()を期待値と直接比較する
#[test]
fn reserved_keyword_package_name_is_escaped_in_idl_but_not_elsewhere() {
    let expect_idl = "\
module _map {
struct MapPose {
  double x;
};
};
";
    assert_eq!(MapPose::idl(NsMode::Raw), expect_idl);
    assert_eq!(MapPose::msg_ref(), "map/MapPose");
    assert_eq!(MapPose::package(), "map");
}

// Why: 同じ依存型(Point)を複数フィールドから参照しても、IDL上の型定義は重複せず
//      依存(Point_)が参照側(Pose_)より前に1回だけ出力されることを確認するため
// Method: Pose::idl()の全体を期待するIDL文字列と直接比較する
#[test]
fn nested_struct_idl_dedups_and_orders_dependency_first() {
    let expect = "\
module geometry_msgs {
struct Point {
  double x;
  double y;
};
};
module my_robot_interfaces {
struct Pose {
  geometry_msgs::Point position;
  geometry_msgs::Point orientation;
};
};
";
    assert_eq!(Pose::idl(NsMode::Raw), expect);
}

// Why: ROS 2の.msgは1メッセージ1ファイルの規約のため、依存型(Point)の定義自体は含めず
//      型参照のみが並ぶことを確認するため
#[test]
fn nested_struct_ros2_msg_only_contains_self_definition() {
    let expect = "geometry_msgs/Point position\ngeometry_msgs/Point orientation\n";
    assert_eq!(Pose::ros2_msg(), expect);
}

// Why: プリミティブ/ネスト構造体/可変長配列(Vec)/固定長配列([T;N])が1つの構造体に混在しても
//      それぞれ型対応表通りに変換されることを確認するため
// Method: RobotStatus::idl()/ros2_msg()を期待する完全な文字列と比較する
#[test]
fn mixed_field_kinds_are_mapped_per_type_table() {
    let expect_idl = "\
module geometry_msgs {
struct Point {
  double x;
  double y;
};
};
module my_robot_interfaces {
struct RobotStatus {
  octet status_code;
  double battery_level;
  geometry_msgs::Point position;
  sequence<float> history;
  double matrix[16];
};
};
";
    assert_eq!(RobotStatus::idl(NsMode::Raw), expect_idl);
    let expect_idl = "\
module geometry_msgs { module msg {
struct Point {
  double x;
  double y;
};
};};
module my_robot_interfaces { module msg {
struct RobotStatus {
  octet status_code;
  double battery_level;
  geometry_msgs::msg::Point position;
  sequence<float> history;
  double matrix[16];
};
};};
";
    assert_eq!(RobotStatus::idl(NsMode::ros2idl()), expect_idl);

    let expect_ros2_idl = "\
module geometry_msgs { module msg { module dds_ {
struct Point {
  double x;
  double y;
};
};};};
module my_robot_interfaces { module msg { module dds_ {
struct RobotStatus {
  octet status_code;
  double battery_level;
  geometry_msgs::msg::dds_::Point position;
  sequence<float> history;
  double matrix[16];
};
};};};
";
    assert_eq!(
        RobotStatus::idl(NsMode::WithMiddle("msg::dds_")),
        expect_ros2_idl
    );

    // ros2dds(): dds_名前空間に加え、ROS層との型名衝突を防ぐsuffix `_` が
    // 宣言名と参照の両方へ付与される
    let expect_ros2_dds_idl = "\
module geometry_msgs { module msg { module dds_ {
struct Point_ {
  double x;
  double y;
};
};};};
module my_robot_interfaces { module msg { module dds_ {
struct RobotStatus_ {
  octet status_code;
  double battery_level;
  geometry_msgs::msg::dds_::Point_ position;
  sequence<float> history;
  double matrix[16];
};
};};};
";
    assert_eq!(RobotStatus::idl(NsMode::ros2dds()), expect_ros2_dds_idl);

    let expect_msg = "\
uint8 status_code
float64 battery_level
geometry_msgs/Point position
float32[] history
float64[16] matrix
";
    assert_eq!(RobotStatus::ros2_msg(), expect_msg);
}

// Why: mcap/rosbag2がスキーマ埋め込みに使う連結メッセージ定義
//      (https://mcap.dev/docs/python/ros2_noenv_example) は、自身の.msg定義（ヘッダなし）に続けて
//      依存先を出現順・重複排除しつつ区切り線+`MSG: pkg/Name`ヘッダ付きで列挙する形式のため、
//      2フィールドから同じPointを参照しても定義が1回だけ出ることを確認する
// Method: Pose::full_ros2_msg()を期待する連結文字列と直接比較する
#[test]
fn full_ros2_msg_concatenates_self_and_deduped_dependency_definitions() {
    let expect = "\
geometry_msgs/Point position
geometry_msgs/Point orientation
================================================================================
MSG: geometry_msgs/Point
float64 x
float64 y
";
    assert_eq!(Pose::full_ros2_msg(), expect);
}

// Why: Rust予約語フィールドはr#付きでしか定義できないが、.msg/IDLには予約語制約がなく
//      r#を除去した素の名前が正しい出力のため。r#が残るとros2msg-schema側で
//      フィールド名"r"+コメントとして静かに誤読される
// Method: 予約語フィールドを含む構造体のros2_msg()/idl()を期待文字列と直接比較する
#[test]
fn raw_identifier_fields_are_unrawed_in_msg_and_idl() {
    let expect_msg = "\
uint8 type
uint32 loop
string ref
float64 normal_name
";
    assert_eq!(KeywordFields::ros2_msg(), expect_msg);

    let expect_idl = "\
module test_msgs {
struct KeywordFields {
  octet type;
  unsigned long loop;
  string ref;
  double normal_name;
};
};
";
    assert_eq!(KeywordFields::idl(NsMode::Raw), expect_idl);
}

/// 自己参照する構造体。`struct Node { children: Vec<Node> }`はROS 2 .msg/IDLが
/// 表現できない循環参照だが、Rust自体としては合法な型のためderiveは通ってしまう。
#[derive(DdsInterface)]
#[cdds(package = "test_msgs")]
struct Node {
    value: i32,
    children: Vec<Node>,
}

// Why: 以前はcollect_defsがフィールド再帰を自身の登録より先に行っていたため、
//      自己参照型でNode::idl()を呼ぶと無限再帰でスタックオーバーフロー・プロセスabort
//      していた（`panic`ではなく`abort`のためcatch_unwindでも捕捉不可能だった）。
//      derive経由でも無限再帰ではなく明確なpanicになることを確認する
// Method: #[should_panic]でpanicすることとメッセージに循環経路が含まれることを確認する
#[test]
#[should_panic(expected = "test_msgs::Node -> test_msgs::Node")]
fn self_referential_derive_struct_panics_instead_of_overflowing_stack() {
    let _ = Node::idl(NsMode::Raw);
}

// Why: full_ros2_msg/ros2_msg側（MsgDefRegistry経由）は元々register-firstの順序で
//      クラッシュこそしなかったが、代わりに自己参照する不正な.msgを黙って返していた。
//      visiting導入後はこちらも同じ循環としてpanicするべきことを確認する
#[test]
#[should_panic(expected = "test_msgs::Node -> test_msgs::Node")]
fn self_referential_derive_struct_panics_in_full_ros2_msg_too() {
    let _ = Node::full_ros2_msg();
}

/// 相互再帰(A→B→A)を模した構造体ペア。単純な自己参照だけでなく、
/// 型をまたいだ循環も検出できることを確認するため。
#[derive(DdsInterface)]
#[cdds(package = "test_msgs")]
struct CycleA {
    b: Vec<CycleB>,
}

#[derive(DdsInterface)]
#[cdds(package = "test_msgs")]
struct CycleB {
    a: Vec<CycleA>,
}

#[test]
#[should_panic(expected = "test_msgs::CycleA -> test_msgs::CycleB -> test_msgs::CycleA")]
fn mutually_recursive_derive_structs_panic_with_full_cycle_path() {
    let _ = CycleA::idl(NsMode::Raw);
}

/// `Topic`を併用しない`DdsInterface`単独の構造体でも`#[topic_key]`が書けることを兼ねて確認するフィクスチャ
#[derive(DdsInterface)]
#[cdds(package = "test_msgs")]
struct KeyedPoint {
    #[topic_key]
    id: u32,
    x: f64,
    y: f64,
}

// Why: Topic deriveのkeyhash計算に使う#[topic_key]を、DdsInterfaceのidl_def()も
//      唯一の情報源として共有し、キーフィールドにだけ`@key `が前置されることを確認する
// Method: NsMode::Raw/ros2ddsをリスト化し、KeyedPoint::idl(mode)を期待文字列と比較する
#[test]
fn topic_key_field_gets_key_annotation_in_idl() {
    let cases = [
        (
            NsMode::Raw,
            "\
module test_msgs {
struct KeyedPoint {
  @key unsigned long id;
  double x;
  double y;
};
};
",
        ),
        (
            NsMode::ros2dds(),
            "\
module test_msgs { module msg { module dds_ {
struct KeyedPoint_ {
  @key unsigned long id;
  double x;
  double y;
};
};};};
",
        ),
    ];
    for (mode, expect) in cases {
        assert_eq!(KeyedPoint::idl(mode), expect);
    }
}

/// ネスト構造体キー: `KeyedOuter.inner`が`#[topic_key]`のとき、参照側フィールドに`@key`が付き、
/// `KeyedInner`自身の定義は自身のキー指定(`id`)に従う。
#[derive(DdsInterface)]
#[cdds(package = "test_msgs")]
struct KeyedInner {
    #[topic_key]
    id: u32,
    value: f64,
}

#[derive(DdsInterface)]
#[cdds(package = "test_msgs")]
struct KeyedOuter {
    #[topic_key]
    inner: KeyedInner,
    payload: f64,
}

#[test]
fn nested_struct_key_annotates_reference_field_only() {
    let expect = "\
module test_msgs {
struct KeyedInner {
  @key unsigned long id;
  double value;
};
};
module test_msgs {
struct KeyedOuter {
  @key test_msgs::KeyedInner inner;
  double payload;
};
};
";
    assert_eq!(KeyedOuter::idl(NsMode::Raw), expect);
}

/// `#[topic_key_enum]`も`#[topic_key]`と同様に`@key`が付くことを確認するフィクスチャ
#[derive(DdsInterface)]
#[cdds(package = "test_msgs")]
struct KeyedEnumField {
    #[topic_key_enum]
    kind: u8,
    payload: f64,
}

#[test]
fn topic_key_enum_field_gets_key_annotation_in_idl() {
    let expect = "\
module test_msgs {
struct KeyedEnumField {
  @key octet kind;
  double payload;
};
};
";
    assert_eq!(KeyedEnumField::idl(NsMode::Raw), expect);
}

/// 別モジュールの同名(package, name)だが内容の異なる2つのstruct。
/// 以前はdedupが(package, name)しか見ておらず、両方をフィールドに持つ型の
/// idl()/full_ros2_msg()を呼ぶと最初にvisitした側の定義だけが残り、もう片方の
/// フィールドは同じ型参照に解決されたまま定義が消えていた(スキーマと実データの不整合)。
mod dup_v1 {
    use cyclonedds_rs::*;
    #[derive(DdsInterface)]
    #[cdds(package = "app_msgs")]
    pub struct Config {
        pub threshold: f64,
    }
}
mod dup_v2 {
    use cyclonedds_rs::*;
    #[derive(DdsInterface)]
    #[cdds(package = "app_msgs")]
    pub struct Config {
        pub name: String,
        pub retries: u32,
    }
}

#[derive(DdsInterface)]
#[cdds(package = "test_msgs")]
struct UsesConflictingConfigs {
    a: dup_v1::Config,
    b: dup_v2::Config,
}

// Why: derive経由でも、別モジュールの別structが同じ(package, name)に解決される
//      衝突がidl()呼び出し時にpanicで検出されることを保証する
#[test]
#[should_panic(expected = "conflicting DdsInterface definitions for app_msgs/Config")]
fn conflicting_same_name_derive_structs_panic_via_idl() {
    let _ = UsesConflictingConfigs::idl(NsMode::Raw);
}

// Why: full_ros2_msg()(MsgDefRegistry経由)でも同様にpanicすることを保証する
#[test]
#[should_panic(expected = "conflicting DdsInterface definitions for app_msgs/Config")]
fn conflicting_same_name_derive_structs_panic_via_full_ros2_msg() {
    let _ = UsesConflictingConfigs::full_ros2_msg();
}
