use assignment_2_solution::{
    run_register_process, serialize_register_command, ClientCommandHeader, ClientRegisterCommand,
    ClientRegisterCommandContent, Configuration, PublicConfiguration, RegisterCommand, SectorVec,
    MAGIC_NUMBER,
};
use assignment_2_test_utils::system::*;
use hmac::Mac;
use ntest::timeout;
use std::convert::TryInto;
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
#[timeout(4000)]
async fn single_process_system_completes_operations() {
    // given
    // use log::info;

    let hmac_client_key = [5; 32];
    let tcp_port = 30_287;
    let storage_dir = tempdir().unwrap();
    let request_identifier = 1778;

    let config = Configuration {
        public: PublicConfiguration {
            tcp_locations: vec![("127.0.0.1".to_string(), tcp_port)],
            self_rank: 1,
            n_sectors: 20,
            storage_dir: storage_dir.into_path(),
        },
        hmac_system_key: [1; 64],
        hmac_client_key,
    };

    tokio::spawn(run_register_process(config));

    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut stream = TcpStream::connect(("127.0.0.1", tcp_port))
        .await
        .expect("Could not connect to TCP port");
    let write_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx: 12,
        },
        content: ClientRegisterCommandContent::Write {
            data: SectorVec(vec![3; 4096]),
        },
    });

    // when
    send_cmd(&write_cmd, &mut stream, &hmac_client_key).await;

    // then
    const EXPECTED_RESPONSES_SIZE: usize = 48;
    let mut buf = [0_u8; EXPECTED_RESPONSES_SIZE];
    stream
        .read_exact(&mut buf)
        .await
        .expect("Less data then expected");

    // asserts for write response
    assert_eq!(&buf[0..4], MAGIC_NUMBER.as_ref());
    assert_eq!(buf[7], 0x42);
    assert_eq!(
        u64::from_be_bytes(buf[8..16].try_into().unwrap()),
        request_identifier
    );
    assert!(hmac_tag_is_ok(&hmac_client_key, &buf));
}

#[tokio::test]
// #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[serial_test::serial]
#[timeout(30000)]
async fn concurrent_operations_on_the_same_sector() {
    // given
    use log::info;
    println!("starting test");

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }
    init();

    let port_range_start = 21518;
    let n_clients = 16;
    let config = TestProcessesConfig::new(1, port_range_start);
    config.start().await;
    let mut streams = Vec::new();
    for _ in 0..n_clients {
        streams.push(config.connect(0).await);
    }
    // when
    for (i, stream) in streams.iter_mut().enumerate() {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: i.try_into().unwrap(),
                        sector_idx: 0,
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(vec![if i % 2 == 0 { 1 } else { 254 }; 4096]),
                    },
                }),
                stream,
            )
            .await;
    }

    for stream in &mut streams {
        // println!("reading response");
        // info!("logging");
        config.read_response(stream).await.unwrap();
    }

    config
        .send_cmd(
            &RegisterCommand::Client(ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier: n_clients,
                    sector_idx: 0,
                },
                content: ClientRegisterCommandContent::Read,
            }),
            &mut streams[0],
        )
        .await;
    println!("sending command");
    let response = config.read_response(&mut streams[0]).await.unwrap();
    println!("read response");

    match response.content {
        RegisterResponseContent::Read(SectorVec(sector)) => {
            assert!(sector == vec![1; 4096] || sector == vec![254; 4096]);
        }
        _ => panic!("Expected read response"),
    }
}

#[tokio::test]
// #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[serial_test::serial]
#[timeout(40000)]  // 40000
async fn large_number_of_operations_execute_successfully() {
    // given
    use log;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }
    init();

    let port_range_start = 21625;
    let commands_total = 32;
    let config = TestProcessesConfig::new(3, port_range_start);
    // let config = TestProcessesConfig::new(70, port_range_start);
    config.start().await;
    let mut stream = config.connect(2).await;

    for cmd_idx in 0..commands_total {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: cmd_idx,
                        sector_idx: cmd_idx,
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(vec![cmd_idx as u8; 4096]),
                    },
                }),
                &mut stream,
            )
            .await;
    }

    for _ in 0..commands_total {
        config.read_response(&mut stream).await.unwrap();
    }

    // when
    for cmd_idx in 0..commands_total {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: cmd_idx + 256,
                        sector_idx: cmd_idx,
                    },
                    content: ClientRegisterCommandContent::Read,
                }),
                &mut stream,
            )
            .await;
    }

    // then
    for _ in 0..commands_total {
        let response = config.read_response(&mut stream).await.unwrap();
        match response.content {
            RegisterResponseContent::Read(SectorVec(sector)) => {
                assert_eq!(
                    sector,
                    vec![(response.header.request_identifier - 256) as u8; 4096]
                )
            }
            _ => panic!("Expected read response"),
        }
    }
}

async fn send_cmd(register_cmd: &RegisterCommand, stream: &mut TcpStream, hmac_client_key: &[u8]) {
    let mut data = Vec::new();
    serialize_register_command(register_cmd, &mut data, hmac_client_key)
        .await
        .unwrap();

    stream.write_all(&data).await.unwrap();
}

fn hmac_tag_is_ok(key: &[u8], data: &[u8]) -> bool {
    let boundary = data.len() - HMAC_TAG_SIZE;
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(&data[..boundary]);
    mac.verify_slice(&data[boundary..]).is_ok()
}
