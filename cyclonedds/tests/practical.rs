//! 実運用で踏んだケースを、有界時間で完了する統合テストとして固定する。
//!
//! 待ち方のノウハウは [docs/receiving-samples.md](../../docs/receiving-samples.md) に文章として
//! まとめてある。ここはその実行可能な形。

mod common;

use std::time::Duration;

use cdds_derive::Topic;
use common::{arrival_timeout, silence_window};
use cyclonedds_rs::*;
use serial_test::serial;

// 他ホストの通信と混ざらないよう、loopbackのみを使う(`lo`限定で排除できるのは他ホストの
// トラフィックのみで、同一ホスト上で並走する別ジョブとは同じドメインIDならマッチしうる)。
// 環境変数`CYCLONEDDS_URI`はプロセス全体に効き`set_var`のデータ競合を招くため使わない。
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

// Why 単一ドメイン: 共有participantを使い続ける限りドメインの参加者refcountが1→0に落ちず、
// `docs/domain-lifecycle.md`のレース(ドメイン解体中の再生成でSEGV)の経路に入らない。
// ケース間の分離はトピック名で行う
const DOMAIN_ID: u32 = common::PRACTICAL_DOMAIN_ID;

#[derive(Debug, Clone, PartialEq, Topic, Serialize, Deserialize)]
struct Seq {
    id: u32,
}

/// 受信結果を表す1行。期待値と実測値を同じ型で作り、表ごと比較する。
#[derive(Debug, PartialEq)]
struct Received {
    case: &'static str,
    /// 受信できたidの列。件数・順序・重複をまとめて表す
    ids: Vec<u32>,
}

/// `want`件が揃うまで有界時間で取り続ける。
///
/// Why ループが要る: `take_async`は**その時点で到着している分だけ**返して完了するため、
/// 1回の呼び出しで`want`件揃う保証はない。1回で判定すると件数不足として観測される。
async fn take_up_to(reader: &DdsReader<Seq>, want: usize) -> Vec<u32> {
    let mut ids = Vec::new();
    let mut buf = SampleBuffer::<Seq>::new(want.max(1));
    let deadline = tokio::time::Instant::now() + arrival_timeout();

    while ids.len() < want {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, reader.take_async(&mut buf)).await {
            Ok(Ok(_)) => ids.extend(buf.iter().map(|s| s.id)),
            // 期待件数に届かないまま打ち切る。差分は呼び出し側の表比較で露見する
            Ok(Err(_)) | Err(_) => break,
        }
    }
    ids
}

/// [`take_up_to`]に加えて、静穏窓のあいだ余分が届かないことを確かめる。
///
/// 期待件数の「不足」だけでなく「超過」も見たい表で使う。
async fn take_until(reader: &DdsReader<Seq>, want: usize) -> Vec<u32> {
    let mut ids = take_up_to(reader, want).await;
    let mut buf = SampleBuffer::<Seq>::new(want.max(1));
    match tokio::time::timeout(silence_window(), reader.take_async(&mut buf)).await {
        // 届いていれば`ids`に載って表比較で落ちる
        Ok(Ok(_)) => ids.extend(buf.iter().map(|s| s.id)),
        // 「余分が届かなかった」と言えるのは静穏窓が時間切れになった場合だけ。読み出し自体の
        // 失敗を同じ扱いにすると、readerが壊れていても超過なしとしてpassしてしまう
        Ok(Err(e)) => panic!("静穏窓の観測に失敗した: {e:?}"),
        Err(_) => {}
    }
    ids
}

fn writer_reader(
    participant: &'static DdsParticipant,
    topic_name: &str,
    writer_policy: &Policy,
    reader_policy: &Policy,
) -> anyhow::Result<(DdsWriter<Seq>, DdsReader<Seq>)> {
    let publisher = DdsPublisher::create(participant, None, None)?;
    let subscriber = DdsSubscriber::create(participant, None, None)?;
    let topic = Seq::create_topic(participant, Some(topic_name), None, None)?;
    let writer = WriterBuilder::new()
        .with_qos(writer_policy.to_qos()?)
        .create(&publisher, topic.clone())?;
    let reader = ReaderBuilder::new()
        .as_async()
        .with_qos(reader_policy.to_qos()?)
        .create(&subscriber, topic)?;
    Ok((writer, reader))
}

/// writerとreaderのマッチ成立を有界時間で待つ。
///
/// Why 必要: マッチ前のwriteは相手に届かない。固定sleepで代用するとランナーの速度で
/// 結果が変わるため、`publication_matched_status`が1になるまで待つ。
async fn wait_matched(writer: &DdsWriter<Seq>) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + arrival_timeout();
    while tokio::time::Instant::now() < deadline {
        if writer.publication_matched_status()?.current_count > 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    anyhow::bail!(
        "writerとreaderのマッチが{:?}以内に成立しなかった",
        arrival_timeout()
    )
}

/// Why: TransientLocalのlate joinerが履歴を何件受け取れるか、writeのタイミングで
/// 何が変わるかを1つの表に固定する。
/// Method: Durability × writeタイミング × History depth を振り、受信idの列を実測値として比較する。
#[tokio::test]
#[serial]
async fn test_received_sequence_matrix() -> anyhow::Result<()> {
    DdsDomain::get_or_create(DOMAIN_ID, Some(CYCLONE_LOOPBACK_CONFIG))?;
    let participant = DdsParticipant::get_or_create(Some(DOMAIN_ID))?;

    let mut actual = Vec::new();

    // late joiner向け。writerを作って3件書いたあとにreaderを作る
    for (case, depth, write_count) in [
        ("transient_local_depth3_write3", 3, 3),
        ("transient_local_depth1_write3", 1, 3),
        ("transient_local_depth3_write5", 3, 5),
    ] {
        let policy = Policy::create_transient_local(depth, None)?;
        let publisher = DdsPublisher::create(participant, None, None)?;
        let topic = Seq::create_topic(participant, Some(case), None, None)?;
        let mut writer = WriterBuilder::new()
            .with_qos(policy.to_qos()?)
            .create(&publisher, topic.clone())?;
        for id in 1..=write_count {
            writer.write(std::sync::Arc::new(Seq { id }))?;
        }

        let subscriber = DdsSubscriber::create(participant, None, None)?;
        let reader = ReaderBuilder::new()
            .as_async()
            .with_qos(policy.to_qos()?)
            .create(&subscriber, topic)?;
        let ids = take_until(&reader, depth as usize).await;
        actual.push(Received { case, ids });
    }

    // Volatileのlate joinerには履歴が届かない
    {
        let case = "volatile_late_joiner";
        let policy = Policy {
            history: History::KeepLast(3),
            reliability: Reliability::Reliable(Duration::from_millis(100)),
            durability: Durability::Volatile,
        };
        let publisher = DdsPublisher::create(participant, None, None)?;
        let topic = Seq::create_topic(participant, Some(case), None, None)?;
        let mut writer = WriterBuilder::new()
            .with_qos(policy.to_qos()?)
            .create(&publisher, topic.clone())?;
        for id in 1..=3 {
            writer.write(std::sync::Arc::new(Seq { id }))?;
        }
        let subscriber = DdsSubscriber::create(participant, None, None)?;
        let reader = ReaderBuilder::new()
            .as_async()
            .with_qos(policy.to_qos()?)
            .create(&subscriber, topic)?;
        // マッチ前だと「届かない」のが履歴同期の欠如なのかマッチ未成立なのか
        // 区別できないため、マッチ成立を確認してから静穏窓を見る
        wait_matched(&writer).await?;
        actual.push(Received {
            case,
            ids: take_until(&reader, 0).await,
        });
    }

    // `Policy`を経由せず`HISTORY`だけを設定した場合。CycloneDDSでは同期件数を決めるのは
    // `DURABILITY_SERVICE`なので、`HISTORY`をいくら深くしてもlate joinerには既定の1件しか届かない
    {
        let case = "history_only_without_durability_service";
        let publisher = DdsPublisher::create(participant, None, None)?;
        let topic = Seq::create_topic(participant, Some(case), None, None)?;
        let raw_qos = || -> anyhow::Result<DdsQos> {
            let mut qos = DdsQos::create()?;
            qos.set_durability(dds_durability_kind::DDS_DURABILITY_TRANSIENT_LOCAL);
            qos.set_reliability(
                dds_reliability_kind::DDS_RELIABILITY_RELIABLE,
                Duration::from_millis(100),
            );
            qos.set_history(dds_history_kind::DDS_HISTORY_KEEP_LAST, 3)?;
            Ok(qos)
        };
        let mut writer = WriterBuilder::new()
            .with_qos(raw_qos()?)
            .create(&publisher, topic.clone())?;
        for id in 1..=3 {
            writer.write(std::sync::Arc::new(Seq { id }))?;
        }
        let subscriber = DdsSubscriber::create(participant, None, None)?;
        let reader = ReaderBuilder::new()
            .as_async()
            .with_qos(raw_qos()?)
            .create(&subscriber, topic)?;
        actual.push(Received {
            case,
            ids: take_until(&reader, 3).await,
        });
    }

    // マッチ成立後の連続write。欠落・重複・順序違反がないこと
    {
        let case = "reliable_after_match_100";
        let policy = Policy {
            history: History::KeepAll,
            reliability: Reliability::Reliable(Duration::from_secs(1)),
            durability: Durability::Volatile,
        };
        let (mut writer, reader) = writer_reader(participant, case, &policy, &policy)?;
        wait_matched(&writer).await?;
        for id in 1..=100 {
            writer.write(std::sync::Arc::new(Seq { id }))?;
        }
        actual.push(Received {
            case,
            ids: take_until(&reader, 100).await,
        });
    }

    let expected = vec![
        Received {
            case: "transient_local_depth3_write3",
            ids: vec![1, 2, 3],
        },
        Received {
            case: "transient_local_depth1_write3",
            ids: vec![3],
        },
        Received {
            case: "transient_local_depth3_write5",
            ids: vec![3, 4, 5],
        },
        Received {
            case: "volatile_late_joiner",
            ids: vec![],
        },
        Received {
            case: "history_only_without_durability_service",
            ids: vec![3],
        },
        Received {
            case: "reliable_after_match_100",
            ids: (1..=100).collect(),
        },
    ];
    assert_eq!(actual, expected);
    Ok(())
}

/// `dds_requested_incompatible_qos_status_t::last_policy_id` の値。
///
/// Why 自前定義: cyclonedds-sysのbindingsに`DDS_*_QOS_POLICY_ID`のenumが生成されていないため、
/// ヘッダ(`dds/ddsc/dds_public_qosdefs.h`)の宣言順をそのまま写して読める形にする。
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum PolicyId {
    Durability,
    Deadline,
    Ownership,
    Liveliness,
    Reliability,
    DestinationOrder,
    Other(u32),
}

impl From<u32> for PolicyId {
    fn from(raw: u32) -> Self {
        match raw {
            2 => PolicyId::Durability,
            4 => PolicyId::Deadline,
            6 => PolicyId::Ownership,
            8 => PolicyId::Liveliness,
            11 => PolicyId::Reliability,
            12 => PolicyId::DestinationOrder,
            other => PolicyId::Other(other),
        }
    }
}

/// QoSの対パターン1件の観測結果。
///
/// マッチしたか否かだけでは「なぜ繋がらないか」が分からないため、
/// 通知されたpolicy idと実際の受信可否まで並べる。
#[derive(Debug, PartialEq)]
struct Observed {
    matched: bool,
    /// readerに通知された非互換QoSの種別。互換なら`None`
    incompatible: Option<PolicyId>,
    received: bool,
}

/// 軸ごとの表の1行
#[derive(Debug, PartialEq)]
struct QosOutcome {
    case: &'static str,
    matched: bool,
    incompatible: Option<PolicyId>,
    received: bool,
}

/// writerとreaderの組合せの表の1行
#[derive(Debug, PartialEq)]
struct PairOutcome {
    writer: &'static str,
    reader: &'static str,
    matched: bool,
    incompatible: Option<PolicyId>,
    received: bool,
}

/// writerとreaderを対で作り、マッチ結果と1件の送受信可否を観測する。
async fn observe_qos_pair(
    participant: &'static DdsParticipant,
    case: &str,
    writer_qos: DdsQos,
    reader_qos: DdsQos,
) -> anyhow::Result<Observed> {
    let publisher = DdsPublisher::create(participant, None, None)?;
    let subscriber = DdsSubscriber::create(participant, None, None)?;
    let topic = Seq::create_topic(participant, Some(case), None, None)?;

    let incompatible = std::sync::Arc::new(std::sync::Mutex::new(None::<PolicyId>));
    let listener = DdsListenerBuilder::new().on_requested_incompatible_qos({
        let incompatible = incompatible.clone();
        move |_, status| {
            *incompatible.lock().unwrap() = Some(PolicyId::from(status.last_policy_id));
        }
    });

    let mut writer = WriterBuilder::new()
        .with_qos(writer_qos)
        .create(&publisher, topic.clone())?;
    let reader = ReaderBuilder::new()
        .as_async()
        .with_qos(reader_qos)
        .with_listener_builder(listener)
        .create(&subscriber, topic)?;

    // マッチ成立か非互換通知のどちらかが来るまで待つ。どちらも来ないまま上限に達したら
    // 「マッチせず理由も分からない」として観測値に出す
    let deadline = tokio::time::Instant::now() + arrival_timeout();
    let matched = loop {
        if writer.publication_matched_status()?.current_count > 0 {
            break true;
        }
        if incompatible.lock().unwrap().is_some() {
            break false;
        }
        if tokio::time::Instant::now() >= deadline {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };

    writer.write(std::sync::Arc::new(Seq { id: 1 }))?;
    let mut buf = SampleBuffer::<Seq>::new(1);
    // matched時は「1件届くまでの上限」としてarrival_timeout()を、非matched時は「これ以上
    // 来ないことの確認」としてsilence_window()を使う。逆にすると非互換行の待ち時間が
    // 16行の表全体で跳ね上がる
    let receive_timeout = if matched {
        arrival_timeout()
    } else {
        silence_window()
    };
    let received = matches!(
        tokio::time::timeout(receive_timeout, reader.take_async(&mut buf)).await,
        Ok(Ok(n)) if n > 0
    );

    let incompatible = *incompatible.lock().unwrap();
    Ok(Observed {
        matched,
        incompatible,
        received,
    })
}

/// 軸ごとの表の1行を観測する
async fn probe_qos_pair(
    participant: &'static DdsParticipant,
    case: &'static str,
    writer_qos: DdsQos,
    reader_qos: DdsQos,
) -> anyhow::Result<QosOutcome> {
    let observed = observe_qos_pair(participant, case, writer_qos, reader_qos).await?;
    Ok(QosOutcome {
        case,
        matched: observed.matched,
        incompatible: observed.incompatible,
        received: observed.received,
    })
}

fn qos_with(f: impl FnOnce(&mut DdsQos)) -> anyhow::Result<DdsQos> {
    let mut qos = DdsQos::create()?;
    // 既定値のままだとBestEffort/Volatileで、軸によっては差が出ないため互換な基準を敷く
    qos.set_reliability(
        dds_reliability_kind::DDS_RELIABILITY_RELIABLE,
        Duration::from_millis(100),
    );
    qos.set_history(dds_history_kind::DDS_HISTORY_KEEP_LAST, 8)?;
    f(&mut qos);
    Ok(qos)
}

/// Why: QoSミスマッチで届く/届かないの挙動を把握できていないため、軸ごとの挙動を表で残す。
/// Method: RxO規則を持つ6軸を1軸ずつ非互換側へ振り、マッチ・通知policy id・受信可否を比較する。
#[tokio::test]
#[serial]
async fn test_qos_axis_matrix() -> anyhow::Result<()> {
    DdsDomain::get_or_create(DOMAIN_ID, Some(CYCLONE_LOOPBACK_CONFIG))?;
    let participant = DdsParticipant::get_or_create(Some(DOMAIN_ID))?;

    let mut actual = Vec::new();

    // 基準: 全軸が互換
    actual.push(
        probe_qos_pair(
            participant,
            "axis_all_compatible",
            qos_with(|_| {})?,
            qos_with(|_| {})?,
        )
        .await?,
    );

    // Reliability: readerがReliableを要求し、writerがBestEffortしか提供しない
    actual.push(
        probe_qos_pair(
            participant,
            "axis_reliability",
            qos_with(|q| {
                q.set_reliability(
                    dds_reliability_kind::DDS_RELIABILITY_BEST_EFFORT,
                    Duration::ZERO,
                );
            })?,
            qos_with(|_| {})?,
        )
        .await?,
    );

    // Durability: readerがTransientLocalを要求し、writerがVolatileしか提供しない
    actual.push(
        probe_qos_pair(
            participant,
            "axis_durability",
            qos_with(|q| {
                q.set_durability(dds_durability_kind::DDS_DURABILITY_VOLATILE);
            })?,
            qos_with(|q| {
                q.set_durability(dds_durability_kind::DDS_DURABILITY_TRANSIENT_LOCAL);
            })?,
        )
        .await?,
    );

    // Deadline: readerが要求する周期より、writerが約束する周期の方が長い
    actual.push(
        probe_qos_pair(
            participant,
            "axis_deadline",
            qos_with(|q| {
                q.set_deadline(Duration::from_secs(1));
            })?,
            qos_with(|q| {
                q.set_deadline(Duration::from_millis(100));
            })?,
        )
        .await?,
    );

    // Liveliness: readerがMANUAL_BY_TOPICを要求し、writerはAUTOMATICしか提供しない
    actual.push(
        probe_qos_pair(
            participant,
            "axis_liveliness",
            qos_with(|q| {
                q.set_liveliness(dds_liveliness_kind::DDS_LIVELINESS_AUTOMATIC, i64::MAX);
            })?,
            qos_with(|q| {
                q.set_liveliness(
                    dds_liveliness_kind::DDS_LIVELINESS_MANUAL_BY_TOPIC,
                    i64::MAX,
                );
            })?,
        )
        .await?,
    );

    // Ownership: 種別が一致しなければ非互換(順序ではなく一致が条件)
    actual.push(
        probe_qos_pair(
            participant,
            "axis_ownership",
            qos_with(|q| {
                q.set_ownership(dds_ownership_kind::DDS_OWNERSHIP_SHARED);
            })?,
            qos_with(|q| {
                q.set_ownership(dds_ownership_kind::DDS_OWNERSHIP_EXCLUSIVE);
            })?,
        )
        .await?,
    );

    // DestinationOrder: readerが送信時刻順を要求し、writerは受信時刻順しか提供しない
    actual.push(
        probe_qos_pair(
            participant,
            "axis_destination_order",
            qos_with(|q| {
                q.set_destination_order(
                    dds_destination_order_kind::DDS_DESTINATIONORDER_BY_RECEPTION_TIMESTAMP,
                );
            })?,
            qos_with(|q| {
                q.set_destination_order(
                    dds_destination_order_kind::DDS_DESTINATIONORDER_BY_SOURCE_TIMESTAMP,
                );
            })?,
        )
        .await?,
    );

    let expected = vec![
        QosOutcome {
            case: "axis_all_compatible",
            matched: true,
            incompatible: None,
            received: true,
        },
        QosOutcome {
            case: "axis_reliability",
            matched: false,
            incompatible: Some(PolicyId::Reliability),
            received: false,
        },
        QosOutcome {
            case: "axis_durability",
            matched: false,
            incompatible: Some(PolicyId::Durability),
            received: false,
        },
        QosOutcome {
            case: "axis_deadline",
            matched: false,
            incompatible: Some(PolicyId::Deadline),
            received: false,
        },
        QosOutcome {
            case: "axis_liveliness",
            matched: false,
            incompatible: Some(PolicyId::Liveliness),
            received: false,
        },
        QosOutcome {
            case: "axis_ownership",
            matched: false,
            incompatible: Some(PolicyId::Ownership),
            received: false,
        },
        QosOutcome {
            case: "axis_destination_order",
            matched: false,
            incompatible: Some(PolicyId::DestinationOrder),
            received: false,
        },
    ];
    assert_eq!(actual, expected);
    Ok(())
}

/// Why: 実務で最も踏むReliabilityとDurabilityの相互作用を、全組合せで残す。
/// Method: writer側4通り × reader側4通りを振り、マッチ・通知policy id・受信可否を比較する。
#[tokio::test]
#[serial]
async fn test_reliability_durability_matrix() -> anyhow::Result<()> {
    DdsDomain::get_or_create(DOMAIN_ID, Some(CYCLONE_LOOPBACK_CONFIG))?;
    let participant = DdsParticipant::get_or_create(Some(DOMAIN_ID))?;

    const COMBOS: [(&str, dds_reliability_kind, dds_durability_kind); 4] = [
        (
            "be_vol",
            dds_reliability_kind::DDS_RELIABILITY_BEST_EFFORT,
            dds_durability_kind::DDS_DURABILITY_VOLATILE,
        ),
        (
            "be_tl",
            dds_reliability_kind::DDS_RELIABILITY_BEST_EFFORT,
            dds_durability_kind::DDS_DURABILITY_TRANSIENT_LOCAL,
        ),
        (
            "rel_vol",
            dds_reliability_kind::DDS_RELIABILITY_RELIABLE,
            dds_durability_kind::DDS_DURABILITY_VOLATILE,
        ),
        (
            "rel_tl",
            dds_reliability_kind::DDS_RELIABILITY_RELIABLE,
            dds_durability_kind::DDS_DURABILITY_TRANSIENT_LOCAL,
        ),
    ];

    let mut actual = Vec::new();
    for (writer, w_rel, w_dur) in COMBOS {
        for (reader, r_rel, r_dur) in COMBOS {
            let observed = observe_qos_pair(
                participant,
                &format!("w_{writer}__r_{reader}"),
                qos_with(|q| {
                    q.set_reliability(w_rel, Duration::from_millis(100));
                    q.set_durability(w_dur);
                })?,
                qos_with(|q| {
                    q.set_reliability(r_rel, Duration::from_millis(100));
                    q.set_durability(r_dur);
                })?,
            )
            .await?;
            actual.push(PairOutcome {
                writer,
                reader,
                matched: observed.matched,
                incompatible: observed.incompatible,
                received: observed.received,
            });
        }
    }

    // 両方が非互換な組(例: w_be_vol × r_rel_tl)では、通知されるpolicy idはReliability側になる
    let expected = vec![
        PairOutcome {
            writer: "be_vol",
            reader: "be_vol",
            matched: true,
            incompatible: None,
            received: true,
        },
        PairOutcome {
            writer: "be_vol",
            reader: "be_tl",
            matched: false,
            incompatible: Some(PolicyId::Durability),
            received: false,
        },
        PairOutcome {
            writer: "be_vol",
            reader: "rel_vol",
            matched: false,
            incompatible: Some(PolicyId::Reliability),
            received: false,
        },
        PairOutcome {
            writer: "be_vol",
            reader: "rel_tl",
            matched: false,
            incompatible: Some(PolicyId::Reliability),
            received: false,
        },
        PairOutcome {
            writer: "be_tl",
            reader: "be_vol",
            matched: true,
            incompatible: None,
            received: true,
        },
        PairOutcome {
            writer: "be_tl",
            reader: "be_tl",
            matched: true,
            incompatible: None,
            received: true,
        },
        PairOutcome {
            writer: "be_tl",
            reader: "rel_vol",
            matched: false,
            incompatible: Some(PolicyId::Reliability),
            received: false,
        },
        PairOutcome {
            writer: "be_tl",
            reader: "rel_tl",
            matched: false,
            incompatible: Some(PolicyId::Reliability),
            received: false,
        },
        PairOutcome {
            writer: "rel_vol",
            reader: "be_vol",
            matched: true,
            incompatible: None,
            received: true,
        },
        PairOutcome {
            writer: "rel_vol",
            reader: "be_tl",
            matched: false,
            incompatible: Some(PolicyId::Durability),
            received: false,
        },
        PairOutcome {
            writer: "rel_vol",
            reader: "rel_vol",
            matched: true,
            incompatible: None,
            received: true,
        },
        PairOutcome {
            writer: "rel_vol",
            reader: "rel_tl",
            matched: false,
            incompatible: Some(PolicyId::Durability),
            received: false,
        },
        PairOutcome {
            writer: "rel_tl",
            reader: "be_vol",
            matched: true,
            incompatible: None,
            received: true,
        },
        PairOutcome {
            writer: "rel_tl",
            reader: "be_tl",
            matched: true,
            incompatible: None,
            received: true,
        },
        PairOutcome {
            writer: "rel_tl",
            reader: "rel_vol",
            matched: true,
            incompatible: None,
            received: true,
        },
        PairOutcome {
            writer: "rel_tl",
            reader: "rel_tl",
            matched: true,
            incompatible: None,
            received: true,
        },
    ];
    assert_eq!(actual, expected);
    Ok(())
}

/// Why: 同一型で多数のトピックを同時に扱うとハングした事象への回帰テスト。
/// Method: 同一型・異なるtopic名の30組を生成し、match・送受信・破棄まで通して受信idを比較する。
#[tokio::test]
#[serial]
async fn test_many_topics_same_type() -> anyhow::Result<()> {
    // 30組はCIに常設する安全側の数。手元では1000組まで通ることを確認済み
    const PAIRS: u32 = 30;
    // トピック数以外を原因から外すため、他ケースのdiscoveryが載らない専用ドメインを使う
    const DOMAIN_ID: u32 = common::PRACTICAL_MANY_TOPICS_DOMAIN_ID;

    DdsDomain::get_or_create(DOMAIN_ID, Some(CYCLONE_LOOPBACK_CONFIG))?;
    let participant = DdsParticipant::get_or_create(Some(DOMAIN_ID))?;
    let publisher = DdsPublisher::create(participant, None, None)?;
    let subscriber = DdsSubscriber::create(participant, None, None)?;

    let mut pairs = Vec::new();
    for i in 0..PAIRS {
        let topic = Seq::create_topic(participant, Some(&format!("many_{i}")), None, None)?;
        let writer = WriterBuilder::new().create(&publisher, topic.clone())?;
        // リスナー登録経路も同時に踏ませる。トピックごとに別リスナーを作る必要がある
        let reader = ReaderBuilder::new()
            .as_async()
            .with_listener_builder(DdsListenerBuilder::new().on_data_available(|_| {}))
            .create(&subscriber, topic)?;
        pairs.push((writer, reader));
    }

    for (i, (writer, _)) in pairs.iter_mut().enumerate() {
        writer.write(std::sync::Arc::new(Seq { id: i as u32 }))?;
    }

    let mut actual = Vec::new();
    for (i, (_, reader)) in pairs.iter().enumerate() {
        actual.push((i as u32, take_up_to(reader, 1).await));
    }

    // 生成順に破棄する。ハングしていればここまで到達しない
    drop(pairs);

    let expected = (0..PAIRS).map(|i| (i, vec![i])).collect::<Vec<_>>();
    assert_eq!(actual, expected);
    Ok(())
}

/// Why: TransientLocalの履歴件数が実際に指定どおり届く上限を確かめる。
/// Method: KeepLast(N)のNを振り、depth+2件書いたlate joinerが直近N件を順序どおり受けるか比較する。
#[tokio::test]
#[serial]
async fn test_transient_local_depth_sweep() -> anyhow::Result<()> {
    // 実運用で使う範囲としてN<100を押さえる。手元では5000まで期待どおりに届くことを確認済みで、
    // CIに常設するのは実行時間に見合う上端の99までとする
    const DEPTHS: [i32; 6] = [1, 2, 3, 10, 50, 99];

    DdsDomain::get_or_create(DOMAIN_ID, Some(CYCLONE_LOOPBACK_CONFIG))?;
    let participant = DdsParticipant::get_or_create(Some(DOMAIN_ID))?;

    let mut actual = Vec::new();
    for depth in DEPTHS {
        // depthより多く書いて、直近depth件へ切り詰められることまで見る
        let write_count = depth as u32 + 2;
        let policy = Policy::create_transient_local(depth, None)?;
        let publisher = DdsPublisher::create(participant, None, None)?;
        let topic = Seq::create_topic(participant, Some(&format!("depth_{depth}")), None, None)?;
        let mut writer = WriterBuilder::new()
            .with_qos(policy.to_qos()?)
            .create(&publisher, topic.clone())?;
        for id in 1..=write_count {
            writer.write(std::sync::Arc::new(Seq { id }))?;
        }

        let subscriber = DdsSubscriber::create(participant, None, None)?;
        let reader = ReaderBuilder::new()
            .as_async()
            .with_qos(policy.to_qos()?)
            .create(&subscriber, topic)?;
        actual.push((depth, take_until(&reader, depth as usize).await));
    }

    let expected = DEPTHS
        .iter()
        .map(|&depth| {
            let write_count = depth as u32 + 2;
            (
                depth,
                (write_count - depth as u32 + 1..=write_count).collect(),
            )
        })
        .collect::<Vec<(i32, Vec<u32>)>>();
    assert_eq!(actual, expected);
    Ok(())
}
