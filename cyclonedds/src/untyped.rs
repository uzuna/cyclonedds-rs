//! デシリアライズをせず、エンコード済みの情報をそのまま扱う
//!
//! rosbagのようなデータのレコーディングや、ドメイン間の転送などを想定している
//!
//! 参考情報
//! - [GitHub Issue(cycloneddsで型が不明なtopicへの読み書きについて)](https://github.com/eclipse-cyclonedds/cyclonedds/issues/2099)
//! - [GitHub Issue(CycloneとROS 2間の通信)](https://github.com/eclipse-cyclonedds/cyclonedds/issues/1412)

/// ライブラリで利用するUntypedを明示する型
///
/// Untyped利用にトレイト境界はないため、実装にあわせて別の型を使ってわかりやすくすることを推奨します
#[derive(Clone, Copy, Debug)]
pub struct Untyped;

#[cfg(test)]
mod tests {
    use std::{process::Command, sync::Arc, time::Duration};

    use cdds_derive::Topic;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::common::TestDomain;
    use crate::*;

    const READ_TIMEOUT: Duration = Duration::from_secs(5);

    #[derive(Serialize, Deserialize, Topic, Debug, PartialEq, Clone)]
    struct TestTypedTopic {
        a: u32,
        b: u16,
        c: String,
        d: Vec<u8>,
    }

    impl Default for TestTypedTopic {
        fn default() -> Self {
            Self {
                a: 1234,
                b: 5678,
                c: "Hello".to_string(),
                d: vec![1, 2, 3, 4, 5],
            }
        }
    }

    struct Pub<T> {
        wr: DdsWriter<T>,
        _pb: DdsPublisher,
    }

    impl<T> Pub<T> {
        fn new(participant: &DdsParticipant, topic: &DdsTopic<T>) -> anyhow::Result<Self> {
            let pb = DdsPublisher::create(participant, None, None)?;
            let wr = DdsWriter::create(&pb, topic.clone(), None, None)?;
            Ok(Self { wr, _pb: pb })
        }
    }

    struct Sub<T> {
        re: DdsReader<T>,
        _sb: DdsSubscriber,
    }

    impl<T> Sub<T> {
        fn new(participant: &DdsParticipant, topic: DdsTopic<T>) -> anyhow::Result<Self> {
            let sb = DdsSubscriber::create(participant, None, None)?;
            let re = DdsReader::<T>::create(&sb, topic, None, None)?;
            Ok(Self { re, _sb: sb })
        }

        fn new_async(participant: &DdsParticipant, topic: DdsTopic<T>) -> anyhow::Result<Self> {
            let sb = DdsSubscriber::create(participant, None, None)?;
            let re = DdsReader::<T>::create_async(&sb, topic, None)?;
            Ok(Self { re, _sb: sb })
        }
    }

    struct PubSub<T1, T2> {
        pa: DdsParticipant,
        _t: DdsTopic<T1>,
        qos: Option<DdsQos>,
        pb: Pub<T1>,
        sb: Option<Sub<T2>>,
    }

    impl PubSub<TestTypedTopic, Untyped> {
        fn new(domain_id: u32, qos: Option<DdsQos>) -> anyhow::Result<Self> {
            let pa = unsafe { DdsParticipant::create(Some(domain_id), qos.clone(), None)? };
            let t = TestTypedTopic::create_topic(&pa, None, qos.clone(), None)?;
            let pb = Pub::new(&pa, &t)?;
            Ok(Self {
                pa,
                _t: t,
                pb,
                sb: None,
                qos,
            })
        }

        fn add_reader(&mut self) -> anyhow::Result<()> {
            let t = DdsTopic::create_untyped(
                &self.pa,
                &TestTypedTopic::topic_name(None),
                TestTypedTopic::typename().to_string_lossy().as_ref(),
                self.qos.clone(),
                None,
            )?;
            self.sb = Some(Sub::new(&self.pa, t)?);
            Ok(())
        }

        fn add_reader_async(&mut self) -> anyhow::Result<()> {
            let topic = DdsTopic::create_untyped(
                &self.pa,
                &TestTypedTopic::topic_name(None),
                TestTypedTopic::typename().to_string_lossy().as_ref(),
                self.qos.clone(),
                None,
            )?;
            self.sb = Some(Sub::new_async(&self.pa, topic)?);
            Ok(())
        }

        fn write(&mut self, data: Arc<TestTypedTopic>) -> anyhow::Result<()> {
            self.pb.wr.write(data)?;
            Ok(())
        }

        fn reader(&self) -> &DdsReader<Untyped> {
            &self.sb.as_ref().unwrap().re
        }
    }

    #[test]
    fn test_untyped_ops_survive_domain_teardown() -> anyhow::Result<()> {
        let status = Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("untyped::tests::test_untyped_ops_survive_domain_teardown_child")
            .arg("--nocapture")
            .env("CYCLONEDDS_RS_OPS_LIFETIME_CHILD", "1")
            .status()?;

        assert!(status.success(), "子プロセスが異常終了しました: {status}");
        Ok(())
    }

    #[test]
    fn test_untyped_ops_survive_domain_teardown_child() -> anyhow::Result<()> {
        if std::env::var_os("CYCLONEDDS_RS_OPS_LIFETIME_CHILD").is_none() {
            return Ok(());
        }

        let domain_id = TestDomain::UntypedOpsTeardown.id();
        let domain = crate::common::tests::create_loopback_domain(domain_id)?;
        let mut pubsub = PubSub::<TestTypedTopic, Untyped>::new(domain_id, None)?;
        pubsub.add_reader()?;
        pubsub.write(Arc::new(TestTypedTopic::default()))?;

        std::thread::sleep(Duration::from_millis(300));
        let mut samples = SampleBuffer::new(10);
        assert_eq!(pubsub.reader().takecdr_now(&mut samples)?, 1);

        drop(samples);
        drop(pubsub);
        drop(domain);
        Ok(())
    }

    /// DdsWriterで書いたデータが読める
    #[test_log::test]
    fn test_untyped_read_sync() -> anyhow::Result<()> {
        let domain_id = TestDomain::UntypedReadSync.id();
        let _domain = crate::common::tests::create_loopback_domain(domain_id)?;
        let mut pubsub = PubSub::<TestTypedTopic, Untyped>::new(domain_id, None)?;
        pubsub.add_reader()?;

        let data = TestTypedTopic::default();
        let expected = cdr::serialize::<_, _, cdr::CdrBe>(&data, cdr::Infinite)?;
        pubsub.write(Arc::new(data))?;

        // Wait for data to be delivered
        std::thread::sleep(std::time::Duration::from_millis(300));

        // 読み出し
        let mut buf = SampleBuffer::new(10);
        let size = pubsub.reader().takecdr_now(&mut buf)?;
        assert_eq!(size, 1);

        for sample in buf.iter_sample() {
            let received_bytes = sample.cdr().unwrap();
            assert_eq!(expected, received_bytes);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_untyped_read_async() -> anyhow::Result<()> {
        let domain_id = TestDomain::UntypedReadAsync.id();
        let _domain = crate::common::tests::create_loopback_domain(domain_id)?;
        let mut pubsub = PubSub::<TestTypedTopic, Untyped>::new(domain_id, None)?;
        pubsub.add_reader_async()?;

        let data = TestTypedTopic::default();
        let expected = cdr::serialize::<_, _, cdr::CdrBe>(&data, cdr::Infinite)?;
        pubsub.write(Arc::new(data))?;

        std::thread::sleep(Duration::from_millis(300));

        let mut buf = SampleBuffer::new(10);
        let size = tokio::time::timeout(READ_TIMEOUT, pubsub.reader().takecdr_async(&mut buf))
            .await
            .expect("timeout waiting for the sample")?;
        assert_eq!(size, 1);
        assert_eq!(buf.iter_sample().count(), 1);
        for sample in buf.iter_sample() {
            assert_eq!(expected, sample.cdr().unwrap());
        }
        Ok(())
    }

    // 既知のサンプルの転送テスト
    // Memo: 同じプロセスだと転送ルートがローカルとなって、期待するネットワーク通信のテストにならないかも?
    #[test_log::test]
    fn test_untyped_write_sample() -> anyhow::Result<()> {
        let source_domain_id = TestDomain::UntypedWriteSampleSrc.id();
        let destination_domain_id = TestDomain::UntypedWriteSampleDest.id();
        let _source_domain = crate::common::tests::create_loopback_domain(source_domain_id)?;
        let _destination_domain =
            crate::common::tests::create_loopback_domain(destination_domain_id)?;
        let src_parti = unsafe { DdsParticipant::create(Some(source_domain_id), None, None)? };
        // 扱い型のあるトピック
        let src_topic = TestTypedTopic::create_topic(&src_parti, None, None, None)?;
        let src_pbl = DdsPublisher::create(&src_parti, None, None)?;
        let src_sub = DdsSubscriber::create(&src_parti, None, None)?;

        let mut src_writer = DdsWriter::create(&src_pbl, src_topic.clone(), None, None)?;

        // Untypedで読み出すためのトピック
        let steel_topic = DdsTopic::<Untyped>::create_untyped(
            &src_parti,
            &TestTypedTopic::topic_name(None),
            TestTypedTopic::typename().to_string_lossy().as_ref(),
            None,
            None,
        )?;
        let src_reader = DdsReader::<Untyped>::create(&src_sub, steel_topic.clone(), None, None)?;

        // 転送先であるsrcと別のドメイン
        let dest_parti =
            unsafe { DdsParticipant::create(Some(destination_domain_id), None, None)? };
        let dest_topic = DdsTopic::<Untyped>::create_untyped(
            &dest_parti,
            &TestTypedTopic::topic_name(None),
            TestTypedTopic::typename().to_string_lossy().as_ref(),
            None,
            None,
        )?;
        let dest_pbl = DdsPublisher::create(&dest_parti, None, None)?;
        let dest_sub = DdsSubscriber::create(&dest_parti, None, None)?;
        let mut dest_writer = DdsWriter::create(&dest_pbl, dest_topic.clone(), None, None)?;
        let dest_reader = DdsReader::create(&dest_sub, dest_topic.clone(), None, None)?;

        let data = TestTypedTopic::default();
        let expected = cdr::serialize::<_, _, cdr::CdrBe>(&data, cdr::Infinite)?;
        src_writer.write(Arc::new(data))?;

        // Wait for data to be delivered
        std::thread::sleep(std::time::Duration::from_millis(300));

        // 読み出し
        let mut buf = SampleBuffer::<Untyped>::new(10);
        let size = src_reader.takecdr_now(&mut buf)?;
        assert_eq!(size, 1);

        for sample in buf.iter_sample() {
            let received_bytes = sample.cdr().unwrap();
            assert_eq!(expected, received_bytes);
            dest_writer.forward(sample)?;
        }

        let mut buf = SampleBuffer::<Untyped>::new(10);
        let size = dest_reader.takecdr_now(&mut buf)?;
        assert_eq!(size, 1);
        for sample in buf.iter_sample() {
            let received_bytes = sample.cdr().unwrap();
            assert_eq!(expected, received_bytes);
        }
        Ok(())
    }

    // 揮発性データ
    #[test_log::test]
    fn test_untyped_volatile() -> anyhow::Result<()> {
        let domain_id = TestDomain::UntypedVolatile.id();
        let _domain = crate::common::tests::create_loopback_domain(domain_id)?;
        let mut qos = DdsQos::create()?;
        qos.set_deadline(Duration::from_millis(100))
            .set_reliability(
                dds_reliability_kind::DDS_RELIABILITY_RELIABLE,
                Duration::from_millis(50),
            )
            .set_durability(dds_durability_kind::DDS_DURABILITY_VOLATILE);
        let mut pubsub = PubSub::<TestTypedTopic, Untyped>::new(domain_id, Some(qos))?;

        // Reader不在で書き込んだデータは読めない
        let data = TestTypedTopic::default();
        let expected = cdr::serialize::<_, _, cdr::CdrBe>(&data, cdr::Infinite)?;
        pubsub.write(Arc::new(data))?;
        std::thread::sleep(std::time::Duration::from_millis(300));

        pubsub.add_reader()?;
        let mut buf = SampleBuffer::new(10);
        assert_eq!(pubsub.reader().takecdr_now(&mut buf), Err(DDSError::NoData));

        // Reader存在で書き込んだデータは読める
        pubsub.write(Arc::new(TestTypedTopic::default()))?;
        std::thread::sleep(std::time::Duration::from_millis(60));
        let size = pubsub.reader().takecdr_now(&mut buf)?;
        assert_eq!(size, 1, "One sample should be available"); // 最新のデータは読める
        for sample in buf.iter_sample() {
            let received_bytes = sample.cdr().unwrap();
            assert_eq!(expected, received_bytes);
        }
        Ok(())
    }

    // 永続データのテスト
    // 永続なデータにするには少なくとも3つの設定変更が必要
    // 1. DurabilityをTRANSIENT_LOCAL以上に設定。Historyに記録されるようになる
    // 2. HistoryをKEEP_ALLに設定。記録するためのバッファを確保する
    // 3. ReliabilityをRELIABLEに設定。データの欠落を許容しない
    #[test_log::test]
    fn test_untyped_transient_local() -> anyhow::Result<()> {
        let domain_id = TestDomain::UntypedTransientLocal.id();
        let _domain = crate::common::tests::create_loopback_domain(domain_id)?;
        let mut qos = DdsQos::create()?;
        qos.set_deadline(Duration::from_secs(60 * 60))
            .set_durability(dds_durability_kind::DDS_DURABILITY_TRANSIENT_LOCAL)
            .set_reliability(
                dds_reliability_kind::DDS_RELIABILITY_RELIABLE,
                Duration::from_millis(100),
            )
            .set_history(dds_history_kind::DDS_HISTORY_KEEP_ALL, 1)?;
        let mut pubsub = PubSub::<TestTypedTopic, Untyped>::new(domain_id, Some(qos))?;

        // Reader不在で書き込んだデータでも読める
        let data = TestTypedTopic::default();
        let expected = cdr::serialize::<_, _, cdr::CdrBe>(&data, cdr::Infinite)?;
        pubsub.write(Arc::new(data))?;
        std::thread::sleep(std::time::Duration::from_millis(100));

        pubsub.add_reader()?;
        std::thread::sleep(std::time::Duration::from_millis(10));
        let mut buf = SampleBuffer::new(10);
        let size = pubsub.reader().takecdr_now(&mut buf)?;
        assert_eq!(size, 1, "One sample should be available"); // 過去のデータも読める
        for sample in buf.iter_sample() {
            let received_bytes = sample.cdr().unwrap();
            assert_eq!(expected, received_bytes);
        }
        Ok(())
    }
}
