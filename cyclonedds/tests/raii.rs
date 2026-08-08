use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use cdds_derive::Topic;
use cyclonedds_rs::dds_builtin::{
    BuiltinDataReader, BuiltinSamples, Participants, Publications, Subscriptions,
};
use cyclonedds_rs::*;

// 並列実行によるリソースカウントのブレを防ぐため、シリアル実行を推奨
#[cfg(test)]
use serial_test::serial;

// 他の影響を避けるためにloopbackのみを使う設定。各テストの冒頭で`DdsDomain::get_or_create`へ
// 渡し、そのドメインにのみ適用する。
//
// Why not 環境変数(`CYCLONEDDS_URI`): 設定はプロセス全体に効くため`set_var`が必要になるが、
// cycloneddsやtokioのバックグラウンドスレッドが並行して`getenv`を呼び得るので、
// `#[serial]`でテスト同士を直列化しても`set_var`のデータ競合は防げない。
// 明示生成した共有ドメインはプロセス終了までdropされないため、参加者をdropするこれらの
// テストでも暗黙のドメイン破棄(`docs/domain-lifecycle.md`のSEGV)を踏まない利点もある。
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

const DOMAIN_TEST_PARTICIPANT_ID: u32 = 12; // テスト用の固定ドメインID
const DOMAIN_TEST_READER_ID: u32 = 13;
const DOMAIN_TEST_WRITER_ID: u32 = 14;
const DOMAIN_TEST_PUBLISHER_ID: u32 = 15;
const DOMAIN_TEST_TOPIC_ID: u32 = 16;
// builtin readerのリソース反映待ちのタイムアウト。
// 「イベントが来なければテストを失敗させる」ための上限であって期待待ち時間ではないので、
// 遅いCIでフレーキーにならないよう十分な余裕を持たせる(正常時は数msで抜ける)
const RESOURCE_READ_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Topic, Serialize, Deserialize)]
struct RaiiEndpointTopic {
    id: u32,
    value: String,
}

// Why: 実装したインスタンスが適切に開放されるかを確認するためのテスト
#[tokio::test]
#[serial]
async fn test_dds_resource_leak_lifecycle() -> anyhow::Result<()> {
    DdsDomain::get_or_create(DOMAIN_TEST_PARTICIPANT_ID, Some(CYCLONE_LOOPBACK_CONFIG))?;

    // SAFETY: 参加者のRAII(生成/解放)そのものを検証するテストなので共有参加者は使えない。
    // 3テストとも`#[serial]`で相互排他され、ドメインIDもテストごとに固有なので、
    // 同一ドメインへの生成/破棄が並行することはない
    let participant =
        unsafe { DdsParticipant::create(Some(DOMAIN_TEST_PARTICIPANT_ID), None, None) }?;
    let self_guid = participant.guid();

    let reader_partic = BuiltinDataReader::<Participants>::create_async(&participant, None)?;

    // 初期状態は自分自身が存在することを確認
    let mut samples = BuiltinSamples::<Participants>::new(20);
    let count = reader_partic.take_async(&mut samples).await?;
    assert_eq!(count, 1);
    assert!(samples.iter().find(|p| p.guid() == self_guid).is_some());

    // participantを追加して自分自身以外が指定数追加されることを確認
    const TARGET_PARTICIPANT_COUNT: usize = 10;
    {
        let _pars = (0..TARGET_PARTICIPANT_COUNT)
            .map(|_| unsafe {
                DdsParticipant::create(Some(DOMAIN_TEST_PARTICIPANT_ID), None, None).unwrap()
            })
            .collect::<Vec<_>>();
        let count = reader_partic.take_async(&mut samples).await?;
        assert_eq!(count, TARGET_PARTICIPANT_COUNT);
        for s in samples.iter() {
            assert_ne!(s.guid(), self_guid, "自分自身のguidが見つかるべきではない");
            assert!(s.is_alive(), "participantが生存していることを確認");
        }
    }

    // スコープを抜けたらRAIIでparticipantが解放されることを確認
    let count = reader_partic.take_async(&mut samples).await?;
    assert_eq!(count, TARGET_PARTICIPANT_COUNT);
    for s in samples.iter() {
        assert_ne!(s.guid(), self_guid, "自分自身のguidが見つかるべきではない");
        assert!(!s.is_alive(), "participantが解放されていることを確認");
    }
    Ok(())
}

// Why: DdsReaderがスコープ離脱時に適切に開放されることを確認するためのテスト
#[tokio::test]
#[serial]
async fn test_ddsreader_raii_lifecycle() -> anyhow::Result<()> {
    DdsDomain::get_or_create(DOMAIN_TEST_READER_ID, Some(CYCLONE_LOOPBACK_CONFIG))?;

    // SAFETY: 同上(`#[serial]`かつ固有ドメイン)
    let participant = unsafe { DdsParticipant::create(Some(DOMAIN_TEST_READER_ID), None, None) }?;
    let participant_guid = participant.guid();
    let reader_sub = BuiltinDataReader::<Subscriptions>::create_async(&participant, None)?;
    let topic_name = RaiiEndpointTopic::topic_name(None);
    let mut samples = BuiltinSamples::<Subscriptions>::new(20);

    // 先に作成済みのbuiltin readerなどをドレインして以降の生成イベントだけを観測する
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let _ = reader_sub.take_now(&mut samples);
    samples.clear();

    let subscriber = DdsSubscriber::create(&participant, None, None)?;
    let topic = RaiiEndpointTopic::create_topic(&participant, None, None, None)?;
    let reader = DdsReader::create_async(&subscriber, topic, None)?;

    // DataReader/Writerの疎通には一定の遅延が発生するのでループで待つ。
    // タイムアウトはループの「外」に置くこと。内側に置くと、`tokio::time::Timeout`は
    // 内側のfutureを先にpollしReadyならdeadlineを見ない実装のため、条件に一致しない
    // サンプルが途切れなく届く状況では期限を過ぎてもホットループになる
    let endpoint_guid = tokio::time::timeout(RESOURCE_READ_TIMEOUT, async {
        loop {
            reader_sub.take_async(&mut samples).await?;

            if let Some(sample) = samples.iter().find(|sample| {
                sample.participant_guid() == participant_guid
                    && sample.is_alive()
                    && sample.name().and_then(|name| name.to_str().ok())
                        == Some(topic_name.as_str())
            }) {
                assert_eq!(
                    sample.type_name(),
                    Some(RaiiEndpointTopic::typename().as_c_str()),
                    "Subscriptionsが設定通りの型名を持つことを確認"
                );
                break Ok::<_, anyhow::Error>(sample.guid());
            }

            samples.clear();
        }
    })
    .await
    .expect("timeout waiting for the Subscriptions creation event")?;

    drop(reader);
    drop(subscriber);

    tokio::time::timeout(RESOURCE_READ_TIMEOUT, async {
        loop {
            reader_sub.take_async(&mut samples).await?;

            if let Some(sample) = samples.iter().find(|sample| sample.guid() == endpoint_guid) {
                assert_eq!(sample.participant_guid(), participant_guid);
                assert!(
                    !sample.is_alive(),
                    "Subscriptionsが解放済みであることを確認"
                );
                break Ok::<(), anyhow::Error>(());
            }

            samples.clear();
        }
    })
    .await
    .expect("timeout waiting for the Subscriptions dispose event")?;

    Ok(())
}

// Why: reader/writerが生成に使ったトピックのハンドルを保持し続けることを確認する。
//      保持しないと、利用者がトピックを手放した時点でトピックに付けたリスナーの
//      `Callbacks`がRust側で解放される一方、cycloneddsはtopicのrefcでreader/writerを
//      数えるため遅延削除でエンティティ自体は残り、生き残ったエンティティの
//      `m_listener`が解放済みメモリを指すUAFになる
// Method: ドロップ時にフラグを立てるガードをトピックのリスナーへキャプチャさせ、
//         reader/writer作成後に利用者側のトピックハンドルを手放してもフラグが
//         立たない(=`Callbacks`が生きている)ことを見る
#[tokio::test]
#[serial]
async fn test_ddstopic_outlives_user_handle() -> anyhow::Result<()> {
    DdsDomain::get_or_create(DOMAIN_TEST_TOPIC_ID, Some(CYCLONE_LOOPBACK_CONFIG))?;

    // SAFETY: 同上(`#[serial]`かつ固有ドメイン)
    let participant = unsafe { DdsParticipant::create(Some(DOMAIN_TEST_TOPIC_ID), None, None) }?;
    let subscriber = DdsSubscriber::create(&participant, None, None)?;
    let publisher = DdsPublisher::create(&participant, None, None)?;

    // クロージャに保持させ、Callbacksが解放されるタイミングをDropで観測する
    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let callbacks_freed = Arc::new(AtomicBool::new(false));
    let guard = DropFlag(callbacks_freed.clone());
    let listener = DdsListenerBuilder::new()
        .on_inconsistent_topic(move |_entity, _status| {
            // 呼ばれることは想定していない。クロージャにguardを持たせるためだけ
            let _ = &guard;
        })
        .build();

    let (reader, writer) = {
        let topic = RaiiEndpointTopic::create_topic(
            &participant,
            Some("outlives_user_handle"),
            None,
            Some(listener),
        )?;
        let reader = DdsReader::create_async(&subscriber, topic.clone(), None)?;
        let writer = DdsWriter::create(&publisher, topic, None, None)?;
        (reader, writer)
    };

    assert!(
        !callbacks_freed.load(Ordering::SeqCst),
        "reader/writerがトピックを保持していればリスナーのCallbacksは解放されないはず"
    );

    drop(reader);
    drop(writer);

    Ok(())
}

// Why: writerが生成元のpublisherを保持することを確認する。保持しないと、publisherの
//      ハンドルを手放した時点でC側の親エンティティが消え、子のwriterごと削除される
// Method: publisherのハンドルを手放した後に書き込み、readerが受け取れること
//         (=writerのエンティティが生きていること)を見る
#[tokio::test]
#[serial]
async fn test_ddswriter_keeps_publisher_alive() -> anyhow::Result<()> {
    DdsDomain::get_or_create(DOMAIN_TEST_PUBLISHER_ID, Some(CYCLONE_LOOPBACK_CONFIG))?;

    // SAFETY: 同上(`#[serial]`かつ固有ドメイン)
    let participant =
        unsafe { DdsParticipant::create(Some(DOMAIN_TEST_PUBLISHER_ID), None, None) }?;
    let subscriber = DdsSubscriber::create(&participant, None, None)?;
    let topic = RaiiEndpointTopic::create_topic(&participant, None, None, None)?;
    let reader = DdsReader::create_async(&subscriber, topic.clone(), None)?;

    let mut writer = {
        let publisher = DdsPublisher::create(&participant, None, None)?;
        DdsWriter::create(&publisher, topic, None, None)?
    };

    let sent = RaiiEndpointTopic {
        id: 7,
        value: "writer outlives publisher handle".to_string(),
    };
    writer.write(std::sync::Arc::new(sent.clone()))?;

    let mut samples = SampleBuffer::new(1);
    let count = tokio::time::timeout(RESOURCE_READ_TIMEOUT, reader.take_async(&mut samples))
        .await
        .expect("timeout waiting for the sample")?;
    assert_eq!(count, 1);
    assert_eq!(
        samples.iter().next().cloned(),
        Some(sent),
        "publisherのハンドルを手放した後でも書き込めることを確認"
    );

    Ok(())
}

// Why: DdsWriterがスコープ離脱時に適切に開放されることを確認するためのテスト
#[tokio::test]
#[serial]
async fn test_ddswriter_raii_lifecycle() -> anyhow::Result<()> {
    DdsDomain::get_or_create(DOMAIN_TEST_WRITER_ID, Some(CYCLONE_LOOPBACK_CONFIG))?;

    // SAFETY: 同上(`#[serial]`かつ固有ドメイン)
    let participant = unsafe { DdsParticipant::create(Some(DOMAIN_TEST_WRITER_ID), None, None) }?;
    let participant_guid = participant.guid();
    let reader_pub = BuiltinDataReader::<Publications>::create_async(&participant, None)?;
    let topic_name = RaiiEndpointTopic::topic_name(None);
    let mut samples = BuiltinSamples::<Publications>::new(20);

    // 先に作成済みのbuiltin readerなどをドレインして以降の生成イベントだけを観測する
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let _ = reader_pub.take_now(&mut samples);
    samples.clear();

    let publisher = DdsPublisher::create(&participant, None, None)?;
    let topic = RaiiEndpointTopic::create_topic(&participant, None, None, None)?;
    let writer = DdsWriter::create(&publisher, topic, None, None)?;

    // タイムアウトをループの外に置く理由は`test_ddsreader_raii_lifecycle`のコメント参照
    let endpoint_guid = tokio::time::timeout(RESOURCE_READ_TIMEOUT, async {
        loop {
            reader_pub.take_async(&mut samples).await?;

            if let Some(sample) = samples.iter().find(|sample| {
                sample.participant_guid() == participant_guid
                    && sample.is_alive()
                    && sample.name().and_then(|name| name.to_str().ok())
                        == Some(topic_name.as_str())
            }) {
                assert_eq!(
                    sample.type_name(),
                    Some(RaiiEndpointTopic::typename().as_c_str()),
                    "Publicationsが設定通りの型名を持つことを確認"
                );
                break Ok::<_, anyhow::Error>(sample.guid());
            }

            samples.clear();
        }
    })
    .await
    .expect("timeout waiting for the Publications creation event")?;

    drop(writer);
    drop(publisher);

    tokio::time::timeout(RESOURCE_READ_TIMEOUT, async {
        loop {
            reader_pub.take_async(&mut samples).await?;

            if let Some(sample) = samples.iter().find(|sample| sample.guid() == endpoint_guid) {
                assert_eq!(sample.participant_guid(), participant_guid);
                assert!(!sample.is_alive(), "Publicationsが解放済みであることを確認");
                break Ok::<(), anyhow::Error>(());
            }

            samples.clear();
        }
    })
    .await
    .expect("timeout waiting for the Publications dispose event")?;

    Ok(())
}
