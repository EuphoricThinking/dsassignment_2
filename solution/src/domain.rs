use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

pub static MAGIC_NUMBER: [u8; 4] = [0x61, 0x74, 0x64, 0x64];

pub static READ_CLIENT_REQ: u8 = 0x01;
pub static WRITE_CLIENT_REQ: u8 = 0x02;

pub static READ_PROC: u8 = 0x03;
pub static VALUE: u8 = 0x04;
pub static WRITE_PROC: u8 = 0x05;
pub static ACK: u8 = 0x06;

pub static EXTERNAL_UPPER_HALF: u8 = 0x00;
pub static PROCESS_RESPONSE_ADD: u8 = 0x40;
pub static PROCESS_CUSTOM_MSG: u8 = 0x80;

pub static LOWER_HALVES: [u8; 6] = [READ_CLIENT_REQ, WRITE_CLIENT_REQ, READ_PROC, VALUE, WRITE_PROC, ACK];
pub static UPPER_HALVES: [u8; 3] = [EXTERNAL_UPPER_HALF, PROCESS_RESPONSE_ADD, PROCESS_CUSTOM_MSG];

pub static TIMESTAMP_WR_RANK_SEP: & 'static str = ".";

pub static CONTENT_SIZE: usize = 4096;

pub static READ_RESPONSE_STATUS_CODE: u8 = 0x41;
pub static WRITE_RESPONSE_STATUS_CODE: u8 = 0x42;

pub static RETRANSMISSION_DELAY: u64 = 500;

#[derive (Debug)] // TODO added
pub struct Configuration {
    /// Hmac key to verify and sign internal requests.
    pub hmac_system_key: [u8; 64],
    /// Hmac key to verify client requests.
    pub hmac_client_key: [u8; 32],
    /// Part of configuration which is safe to share with external world.
    pub public: PublicConfiguration,
}

#[derive(Debug)]
pub struct PublicConfiguration {
    /// Storage for durable data.
    pub storage_dir: PathBuf,
    /// Host and port, indexed by identifiers, of every process, including itself
    /// (subtract 1 from self_rank to obtain index in this array).
    /// You can assume that `tcp_locations.len() < 255`.
    pub tcp_locations: Vec<(String, u16)>,
    /// Identifier of this process. Identifiers start at 1.
    pub self_rank: u8,
    /// The number of sectors. The range of supported sectors is <0, `n_sectors`).
    pub n_sectors: u64,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct SectorVec(pub Vec<u8>);

pub type SectorIdx = u64;

#[derive (Debug, Clone)]
pub enum SuicideOrMsg {
    Suicide(SectorIdx),
    RCMessage(RegisterCommand),
}

#[derive(Debug, Clone)]
pub enum RegisterCommand {
    Client(ClientRegisterCommand),
    System(SystemRegisterCommand),
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
/// Repr u8 macro marks this enum as translatable to a single byte. So `Ok` is 0x0,
/// and consecutive values are consecutive numbers. Use (status_code as u8) syntax.
pub enum StatusCode {
    /// Command completed successfully
    Ok,
    /// Invalid HMAC signature
    AuthFailure,
    /// Sector index is out of range <0, Configuration.n_sectors)
    InvalidSectorIndex,
}

#[derive(Debug, Clone)]
pub struct ClientRegisterCommand {
    pub header: ClientCommandHeader,
    pub content: ClientRegisterCommandContent,
}

#[derive(Debug, Clone)]
pub struct SystemRegisterCommand {
    pub header: SystemCommandHeader,
    pub content: SystemRegisterCommandContent,
}

#[derive(Debug, Clone)]
pub enum SystemRegisterCommandContent {
    ReadProc,
    Value {
        timestamp: u64,
        write_rank: u8,
        sector_data: SectorVec,
    },
    WriteProc {
        timestamp: u64,
        write_rank: u8,
        data_to_write: SectorVec,
    },
    Ack,
}


#[repr(u8)]
#[derive (Debug, Clone, PartialEq, Eq)]
pub enum SystemCommandType {
    ReadProc,
    Value,
    WriteProc,
    Ack,
    Other,
    ClientWrite, // for debug
    ClientRead, // for debug
}

#[derive(Debug, Clone)]
pub enum ClientRegisterCommandContent {
    Read,
    Write { data: SectorVec },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ClientCommandHeader {
    pub request_identifier: u64,
    pub sector_idx: SectorIdx,
}

#[derive(Debug, Clone, Copy)]
pub struct SystemCommandHeader {
    pub process_identifier: u8,
    pub msg_ident: Uuid,
    pub sector_idx: SectorIdx,
}

#[derive(Debug, Clone)]
pub struct OperationSuccess {
    pub request_identifier: u64,
    pub op_return: OperationReturn,
}

#[derive(Debug, Clone)]
pub enum OperationReturn {
    Read(ReadReturn),
    Write,
}

#[derive(Debug, Clone)]
pub struct ReadReturn {
    pub read_data: SectorVec,
}
