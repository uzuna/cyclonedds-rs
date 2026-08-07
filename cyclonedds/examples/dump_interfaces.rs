//! DdsInterfaceを実装した型からIDL/.msg定義をダンプする確認用サンプル
//!
//! `cargo run --example dump_interfaces` で実行し、目視で内容を確認する

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
struct RobotStatus {
    status_code: u8,
    battery_level: f64,
    position: Point,
    history: Vec<f32>,
    matrix: [f64; 16],
}

fn main() {
    println!("=== RobotStatus.idl ===");
    println!("{}", RobotStatus::idl(NsMode::Raw));
    println!("=== RobotStatus.msg ===");
    println!("{}", RobotStatus::ros2_msg());
    println!("=== RobotStatus full (mcap/rosbag2向け連結メッセージ定義) ===");
    println!("{}", RobotStatus::full_ros2_msg());
}
