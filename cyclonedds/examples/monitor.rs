//! cycloneddsの参加者、データなどをモニタリングする

use cdds_derive::Topic;
use cyclonedds_rs::{
    dds_builtin::{BuiltinDataReader, BuiltinSamples, Publications},
    *,
};

#[derive(Debug, Clone, PartialEq, Topic, Serialize, Deserialize)]
pub struct ExTopic1 {
    id: u32,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Topic, Serialize, Deserialize)]
#[cdds(fixed_size)]
pub struct ExTopic2 {
    a: i32,
    b: [u8; 32],
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let p = DdsParticipant::get_or_create(None)?;
    let pr: BuiltinDataReader<Publications> =
        BuiltinDataReader::<Publications>::create_async(p, None)?;

    loop {
        let mut s = BuiltinSamples::<Publications>::new(10);
        let count = pr.take_async(&mut s).await?;
        println!("Found {} publications", count);
        for p in s.iter() {
            match p.is_alive() {
                true => {
                    println!(
                        "Add Publication: {}: {}({})",
                        p.guid(),
                        p.name().unwrap().to_string_lossy(),
                        p.type_name().unwrap().to_string_lossy()
                    );
                }
                false => println!("Remove Publication {}", p.guid()),
            }
        }
    }
}
