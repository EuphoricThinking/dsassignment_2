mod domain;

pub use crate::domain::*;
pub use atomic_register_public::*;
use bincode::ErrorKind;
pub use register_client_public::*;
pub use sectors_manager_public::*;
use tokio::task::JoinHandle;
pub use transfer_public::*;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use tokio::net::{TcpListener, TcpStream};
use core::net::SocketAddr;
use tokio::sync::mpsc::{self, unbounded_channel, UnboundedReceiver, UnboundedSender};
use std::sync::Arc;

use uuid::Uuid;

type channel_map<T> = HashMap<SectorIdx, (UnboundedSender<T>, UnboundedReceiver<T>)>;
type ConnectionMap = HashMap<Uuid, JoinHandle<()>>;

struct ConnectionData {
    host: String,
    addr: u16,
    is_active: bool,
}

struct ConnectionHandlerConfig {
    /// Hmac key to verify and sign internal requests.
    hmac_system_key: [u8; 64],
    /// Hmac key to verify client requests.
    hmac_client_key: [u8; 32],
    n_sectors: u64,
    msg_sender: UnboundedSender<(RegisterCommand, SectorIdx)>,
}
// use hmac::{Hmac, Mac};
// use sha2::Sha256;
// use hmac::digest::KeyInit;

// type HmacSha256 = Hmac<Sha256>;

// use hmac::Mac;
// use hmac::digest::KeyInit;

fn get_own_number_in_tcp_ports(self_rank: u8, tcp_locations: &Vec<(String, u16)>) -> (String, u16) {
    let self_idx = self_rank.saturating_sub(1);
    let port_data = tcp_locations[self_idx as usize].clone();

    port_data
}

fn get_sector_idx_from_filename(sector_path: &PathBuf) -> u64 {
    if let Some(fname) = sector_path.file_name() {
        if let Some(str_name) = fname.to_str() {
            let idx: u64 = str_name.parse().unwrap();

            return idx;
        }
    }

    return 0;
}

async fn get_sectors_written_after_recovery(path: &PathBuf) -> HashSet<SectorIdx> {
    let iterator_dir = tokio::fs::read_dir(path).await;
    let mut written_sectors: HashSet<SectorIdx> = HashSet::new();

    if let Ok(mut root_iterator) = iterator_dir {
        while let Ok(Some(sector_entry)) = root_iterator.next_entry().await {
            let sector_path = sector_entry.path();
            let sector_iter = tokio::fs::read_dir(&sector_path).await;
            if let Ok(mut sector_dir) = sector_iter {
                while let Ok(Some(inside_sector)) = sector_dir.next_entry().await {
                    if inside_sector.path().is_file() {
                        let idx = get_sector_idx_from_filename(&sector_path);
                        written_sectors.insert(idx);
                    }
                }
            }
        }
    }

    written_sectors
}

fn get_sector_idx_from_command(rg_command: &RegisterCommand) -> SectorIdx {
    match rg_command {
        RegisterCommand::Client(ClientRegisterCommand{header, ..}) => {
            return header.sector_idx;
        },
        RegisterCommand::System(SystemRegisterCommand{header, ..}) => {
            return header.sector_idx;
        }
    }
}

fn is_sector_idx_valid(sector_idx: SectorIdx, n_sectors: u64) -> bool {
    // let sector_idx = get_sector_idx_from_command(rg_command);

    return sector_idx < n_sectors;
}

async fn process_connection(mut socket: TcpStream, addr: SocketAddr, sender: UnboundedSender<Uuid>, handle_id: Uuid,  config: Arc<ConnectionHandlerConfig>) {
    let deserialize_result = deserialize_register_command(&mut socket, &config.hmac_system_key, &config.hmac_client_key).await;

    match deserialize_result {
        Err(ref err) if err.kind() == std::io::ErrorKind::InvalidInput => {
            // TODO send message to client, serialized with error msg
        },
        Err(ref err) if err.kind() == std::io::ErrorKind::Other => {

        },
        Err(_) => {
            // error in connection - try to send and send as inactive
        },
        Ok((command, is_hmac_valid)) => {
            let sector_idx = get_sector_idx_from_command(&command);

            if !is_sector_idx_valid(sector_idx, config.n_sectors) {
                // send information about the error
            }
            else if !is_hmac_valid {
                // send information about error
            }
            else {
                // correct message
            }
        }
    }
}

async fn handle_connections(listener: TcpListener, config: Arc<ConnectionHandlerConfig>) {
    let mut connections: ConnectionMap = HashMap::new();
    // for deletion of erroneous connections
    let (connection_sender, mut connection_receiver) = unbounded_channel::<Uuid>();

    loop {
        tokio::select! {
            client_connection = listener.accept() => {

            // there might be up to 16 clients, but there might be more processes willing to connect
                if let Ok((socket, addr)) = client_connection {
                    let handle_id = uuid::Uuid::new_v4();
                    let spawned_handle = tokio::spawn(process_connection(socket, addr, connection_sender.clone(), handle_id, config.clone()));
                    connections.insert(handle_id, spawned_handle);
                }
            }

            error_ocurred = connection_receiver.recv() => {
                match error_ocurred {
                    None => {},
                    Some(client_id) => {
                        // cleanup of connections which returned error
                        connections.remove(&client_id);
                    }
                }
            }
        }
    }
}

pub async fn run_register_process(config: Configuration) {
    let (host, port) = get_own_number_in_tcp_ports(config.public.self_rank, &config.public.tcp_locations);
    let address = format!("{}:{}", host, port);
    let listener = TcpListener::bind(address).await.unwrap();

    // tasks managing registers
    let mut register_handlers: HashMap<SectorIdx, JoinHandle<()>> = HashMap::new();
    /*
    channels granting permission to work by a register; if register detects empty queues and expresses a wish to be dropped, the process checks if the reigster is really not needed; if it is needed, it gives a coin, allowing work; 
     */
    let mut active_coin_channels: channel_map<bool> = HashMap::new();
    // let mut suicidal_channels: channel_map<SectorIdx> = HashMap::new();
    // let (mut internal_send_channel, mut internal_recv_channel) = unbounded_channel::<SuicideOrMsg>();
    let mut register_msg_queues: channel_map<RegisterCommand> = HashMap::new();
    // registers inform the process that they are probably not needed
    let (suicidal_sender, mut suicidal_receiver) = unbounded_channel::<SectorIdx>();
    // messages from clients, other processes and internal
    let (rcommands_sender, mut rcommands_receiver) = unbounded_channel::<(RegisterCommand, SectorIdx)>();

    let root_path = config.public.storage_dir.clone();
    let sector_manager = build_sectors_manager(config.public.storage_dir).await;
    // let sectors_written_after_recovery = sector_manager.get_
    let sectors_written_after_recovery = get_sectors_written_after_recovery(&root_path);
    // let connection_tasks: HashMap<(String, u16), JoinHandle<()>> = HashMap::new();

    let config_for_connections = Arc::new(ConnectionHandlerConfig{hmac_system_key: config.hmac_system_key, hmac_client_key: config.hmac_client_key, n_sectors: config.public.n_sectors, msg_sender: rcommands_sender.clone()});
    tokio::spawn(handle_connections(listener, config_for_connections.clone()));

    
    
    unimplemented!()
}

pub mod atomic_register_public {
    use crate::{
        Broadcast, ClientRegisterCommand, ClientRegisterCommandContent, OperationSuccess, ReadReturn, RegisterClient, SectorIdx, SectorVec, SectorsManager, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent, CONTENT_SIZE
    };
    use crate::register_client_public::Send as RegisterSend;
    use std::collections::{HashSet, HashMap};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use uuid::Uuid;

    #[derive(Clone)]
    struct SectorData {
        timestamp: u64,
        write_rank: u8,
        value: SectorVec,
    }


    struct RegisterPerSector {
        callback: Option<Box<
        dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
            + Send
            + Sync,>>,
        my_process_ident: u8,
        sector_idx: SectorIdx,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,

        // elements required by the algorithm
        timestamp: u64,
        writing_rank: u8,
        value: Option<SectorVec>,
        readlist: HashMap<u8, SectorData>,
        acklist: HashSet<u8>,
        reading: bool,
        writing: bool,
        // value to be written
        writeval: Option<SectorVec>,
        // value read from the sector
        readval: Option<SectorVec>,
        operation_id: Uuid,
        write_phase: bool, 
        request_id: u64,

        // last_issued_command: Option<SystemRegisterCommand>,
    }

    impl RegisterPerSector {
        async fn retrieve(&mut self) -> SectorData {
            let (timestamp, write_rank) = self.sectors_manager.read_metadata(self.sector_idx).await;
            let value = self.sectors_manager.read_data(self.sector_idx).await;

            SectorData{timestamp, write_rank, value}
            // unimplemented!()
        }

        async fn recovery(&mut self) {
            let SectorData{timestamp, write_rank, value} = self.retrieve().await;

            self.timestamp = timestamp;
            self.writing_rank = write_rank;
            self.value = Some(value);

            // unimplemented!()
        }

        async fn send_broadcast_readproc_command(&mut self) {
            let command_content = SystemRegisterCommandContent::ReadProc;
            let command_header = SystemCommandHeader{
                process_identifier: self.my_process_ident,
                msg_ident: self.operation_id,
                sector_idx: self.sector_idx,
            };

            let system_command = SystemRegisterCommand{
                header: command_header,
                content: command_content,
            };

            let msg = Broadcast{
                cmd: Arc::new(system_command)
            };

            self.register_client.broadcast(msg).await;
        }

        fn get_command_header(&self, operation_id: Uuid) -> SystemCommandHeader {
            SystemCommandHeader{
                process_identifier: self.my_process_ident,
                msg_ident: operation_id,
                sector_idx: self.sector_idx,
            }
        }

        async fn store(&mut self, timestamp: u64, write_rank: u8, value: SectorVec) {
            self.sectors_manager.write(self.sector_idx, &(value, timestamp, write_rank)).await;
        }

        async fn store_and_save(&mut self, timestamp: u64, write_rank: u8, value: &SectorVec) {
            self.store(timestamp, write_rank, value.clone()).await;

            self.timestamp = timestamp;
            self.writing_rank = write_rank;
            self.value = Some(value.clone());
        }

        async fn broadcast_write_proc(&mut self, timestamp: u64, write_rank: u8, value: SectorVec ){
            let reply_header = self.get_command_header(self.operation_id);

            let reply_content = SystemRegisterCommandContent::WriteProc { timestamp: timestamp, write_rank: write_rank, data_to_write: value };

            let reply_command = SystemRegisterCommand{
                header: reply_header,
                content: reply_content,
            };

            let msg = Broadcast{
                cmd: Arc::new(reply_command),
            };

            self.register_client.broadcast(msg).await;
            // TODO implement store
            // let reply_content
        }

        fn is_quorum_and_reading_or_writing(&self, container_len: usize) -> bool {
            (container_len > (self.processes_count / 2).into()) && (self.reading || self.writing)
        }

        async fn get_value(&self) -> SectorVec {
            match &self.value {
                None => {
                    // there should have been value after recovery
                    return self.sectors_manager.read_data(self.sector_idx).await;
                },
                Some(val) => {return val.clone();},
            }
        }

        fn unpack_readval_writeval(&self, value: &Option<SectorVec>) -> SectorVec {
            match value {
                None => {
                    let new_vector: Vec<u8> = vec![0; CONTENT_SIZE];
                    return SectorVec(new_vector);
                }
                Some(val) => {return val.clone();},
            }
        }

        fn get_max_value_readlist(&self) -> SectorData {
            let (_, val) = self.readlist.iter().max_by(|(_, v1), (_, v2)| {
                v1.timestamp.cmp(&v2.timestamp).then(v1.write_rank.cmp(&v2.write_rank))
            }).unwrap();

            return val.clone();

            // unimplemented!()
        }
    }
    
    #[async_trait::async_trait]
    impl AtomicRegister for RegisterPerSector {
        async fn client_command(
            &mut self,
            cmd: ClientRegisterCommand,
            success_callback: Box<
                dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        ) {
            let ClientRegisterCommand{header, content} = cmd;

            self.callback = Some(success_callback);

            self.request_id = header.request_identifier;
            self.operation_id = Uuid::new_v4();
            self.acklist = HashSet::new();
            self.readlist = HashMap::new();

            match content {
                ClientRegisterCommandContent::Read => {
                    // self.operation_id = Uuid::new_v4();
                    // self.readlist = HashMap::new();
                    // self.acklist = HashSet::new();
                    self.reading = true;

                    // self.register_client.broadcast(msg)
                    self.send_broadcast_readproc_command().await;
                    
                },
                ClientRegisterCommandContent::Write{data} => {
                    // self.operation_id = Uuid::new_v4();
                    self.writeval = Some(data);
                    // self.acklist = HashSet::new();
                    // self.readlist = HashMap::new();
                    self.writing = true;

                    self.send_broadcast_readproc_command().await;
                }
            }
            // unimplemented!()
        }

        async fn system_command(&mut self, cmd: SystemRegisterCommand) {
            let SystemRegisterCommand{header, content} = cmd;

            // let SystemCommandHeader{process_identifier, msg_ident, sector_idx} = header;

            // let receiver_id = process_identifier;

            if header.sector_idx == self.sector_idx {

                match content {
                    SystemRegisterCommandContent::ReadProc => {
                        let reply_content = SystemRegisterCommandContent::Value { timestamp: self.timestamp, write_rank: self.writing_rank, sector_data: self.get_value().await };

                        let reply_header = SystemCommandHeader{
                            process_identifier: self.my_process_ident,
                            msg_ident: header.msg_ident,
                            sector_idx: self.sector_idx,
                        };

                        let reply = SystemRegisterCommand{
                            header: reply_header, //header,
                            content: reply_content,
                        };

                        let msg = RegisterSend {
                            cmd: Arc::new(reply),
                            target: header.process_identifier,
                        };

                        self.register_client.send(msg).await;
                    },
                    SystemRegisterCommandContent::Value { timestamp, write_rank, sector_data } => {
                        if (header.msg_ident == self.operation_id) && !self.write_phase {
                            self.readlist.insert(header.process_identifier, SectorData { timestamp, write_rank, value: sector_data});

                            if self.is_quorum_and_reading_or_writing(self.readlist.len()) {
                                self.readlist.insert(self.my_process_ident, SectorData { timestamp: self.timestamp, write_rank: self.writing_rank, value: self.get_value().await });
                                let max_val = self.get_max_value_readlist();
                                self.readlist = HashMap::new();
                                self.acklist = HashSet::new();
                                self.write_phase = true;

                                let SectorData { timestamp: max_ts, write_rank: rr, value: read_value } = max_val;

                                self.readval = Some(read_value.clone());

                                if self.reading {
                                    self.broadcast_write_proc(max_ts, rr, read_value).await;
                                }
                                else {
                                    let new_val = self.unpack_readval_writeval(&self.writeval);
                                    let (new_ts, new_wr) = (max_ts + 1, self.my_process_ident);
                                    self.store_and_save(new_ts, new_wr, &new_val).await;

                                    self.broadcast_write_proc(new_ts, new_wr, new_val).await;
                                }
                        }
                    }

                    },
                    SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write } => {
                        if (self.timestamp, self.writing_rank) > (timestamp, write_rank) {
                            self.store_and_save(timestamp, write_rank, &data_to_write).await;
                        }

                        let reply_command = SystemRegisterCommand{
                            header: self.get_command_header(header.msg_ident),
                            content: SystemRegisterCommandContent::Ack,
                        };

                        let msg = RegisterSend{
                            cmd: Arc::new(reply_command),
                            target: header.process_identifier,
                        };

                        self.register_client.send(msg).await;
                    },
                    SystemRegisterCommandContent::Ack => {
                        if ((header.msg_ident) == self.operation_id) && self.write_phase {
                            // self.acklist.insert(self.my_process_ident);
                            self.acklist.insert(header.process_identifier);
                            if self.is_quorum_and_reading_or_writing(self.acklist.len()){
                                self.acklist = HashSet::new();
                                self.write_phase = false;
                                if self.reading {
                                    self.reading = false;
                                    let read_return = ReadReturn{
                                        read_data: self.unpack_readval_writeval(&self.readval),
                                    };

                                    let request_result = OperationSuccess{
                                        request_identifier: self.request_id,
                                        op_return: crate::OperationReturn::Read(read_return),
                                        };

                                    if let Some(callback) = self.callback.take() {
                                        callback(request_result).await;
                                    }
                                }
                                else {
                                    self.writing = false;
                                    let request_result = OperationSuccess{
                                        request_identifier: self.request_id,
                                        op_return: crate::OperationReturn::Write,
                                    };

                                    if let Some(callback) = self.callback.take() {
                                        callback(request_result).await;
                                    }
                                }
                            }
                        }
                    },
                }
            }
            // unimplemented!()
        }


    }
    #[async_trait::async_trait]
    pub trait AtomicRegister: Send + Sync {
        /// Handle a client command. After the command is completed, we expect
        /// callback to be called. Note that completion of client command happens after
        /// delivery of multiple system commands to the register, as the algorithm specifies.
        ///
        /// This function corresponds to the handlers of Read and Write events in the
        /// (N,N)-AtomicRegister algorithm.
        async fn client_command(
            &mut self,
            cmd: ClientRegisterCommand,
            success_callback: Box<
                dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        );

        /// Handle a system command.
        ///
        /// This function corresponds to the handlers of READ_PROC, VALUE, WRITE_PROC
        /// and ACK messages in the (N,N)-AtomicRegister algorithm.
        async fn system_command(&mut self, cmd: SystemRegisterCommand);
    }

    /// Idents are numbered starting at 1 (up to the number of processes in the system).
    /// Communication with other processes of the system is to be done by register_client.
    /// And sectors must be stored in the sectors_manager instance.
    ///
    /// This function corresponds to the handlers of Init and Recovery events in the
    /// (N,N)-AtomicRegister algorithm.
    pub async fn build_atomic_register(
        self_ident: u8,
        sector_idx: SectorIdx,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    ) -> Box<dyn AtomicRegister> {

        let mut new_register = RegisterPerSector {
            callback: None,

            my_process_ident: self_ident,
            sector_idx: sector_idx,
            register_client: register_client,
            sectors_manager: sectors_manager,
            processes_count: processes_count,

            timestamp: 0,
            writing_rank: 0,
            value: None,
            readlist: HashMap::new(),
            acklist: HashSet::new(),
            reading: false,
            writing: false,
            writeval: None,
            readval: None,
            operation_id: Uuid::nil(),
            write_phase: false,
            request_id: 0,
            // last_issued_command: None,
        };

        new_register.recovery().await;

        return Box::new(new_register);
        // unimplemented!()
    }
}

pub mod sectors_manager_public {
    use crate::{SectorIdx, SectorVec, CONTENT_SIZE};
    use std::collections::{HashMap, HashSet};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use hmac::digest::generic_array::{ArrayLength, GenericArray};
    use sha2::{Sha256, Digest};
    use tokio::fs::{DirEntry, ReadDir, File};
    use tokio::io::AsyncWriteExt;
    use tokio::sync::Mutex;
    use uuid::timestamp;
    use std::io::Error;
    use std::ffi::OsStr;
    use crate::get_sector_idx_from_filename;

    
    struct ProcessSectorManager {
        root_dir: PathBuf,
        // RWLock to be implemented
        // written_sectors: Arc<Mutex<HashSet<u64>>>,
        hasher: Sha256,
        sectors_written_after_recovery: Option<HashSet<u64>>,
    }

    impl ProcessSectorManager {
        // timestamp_writerank separated by "_"
        fn create_filename(&self, timestamp: &u64, write_rank: &u8) -> String {
            return timestamp.to_string() + "_" + &write_rank.to_string();
        }

        fn get_sector_dir(&self, idx: SectorIdx) -> PathBuf {
            return self.root_dir.join(idx.to_string());
        }

        fn create_tmp_path_name(&self, tmp_dir_path: &PathBuf, metadata_filename: &String) -> PathBuf {
            return tmp_dir_path.join(metadata_filename);
        }

        // checking an error from create etc. won't work,
        // since the file would be truncated if already existed
        async fn sector_file_dir_exists(&self, dirname: &PathBuf) -> bool {
            // let checked = dirname.try_exists();
            // match checked {
            //     Err(_) => false,
            //     Ok(if_exists) => if_exists,
            // }
            let metadata = tokio::fs::metadata(dirname).await;

            match metadata {
                Err(_) => false,
                Ok(_) => true,
            }

        }

        fn get_checksum(&self, value: &[u8]) -> Vec<u8> {
            let mut hasher = self.hasher.clone();
            hasher.update(value);
            let checksum = hasher.finalize();

            return checksum.to_vec();
        }

        fn get_file_content_with_checksum(&self, value: &Vec<u8>, checksum: Vec<u8>) -> Vec<u8> {
            let mut content = value.clone();
            content.extend(checksum);

            return content;
        }

        async fn get_current_dst_file_path(&self, dir_path: &PathBuf) -> Option<PathBuf> {
            let entries_res = tokio::fs::read_dir(dir_path).await;
            if let Ok(mut entry_reader) = entries_res {
                while let Ok(Some(dir_entry)) = entry_reader.next_entry().await {
                    // if let Some(dir_entry) = entry {
                        let entry_path = dir_entry.path();
                        if entry_path.is_file() {
                            return Some(entry_path);
                        }
                    // }
                }
            }

            None
        }

        async fn sync_dir(&self, dirname: &PathBuf) -> () {
            tokio::fs::File::open(&dirname).await.unwrap().sync_data().await.unwrap();
        }

        fn create_tmp_dir_name_in_sector(&self, sector_path: &PathBuf) -> PathBuf {
            return sector_path.join("tmp");
        }

        fn create_new_dst_file_path(&self, sector_path: &PathBuf, metadata_filename: &String) -> PathBuf {
            return sector_path.join(metadata_filename);
        }

        async fn remove_file(&self, filepath: &PathBuf, parent_dir_path: &PathBuf) -> () {
            // remove file
            tokio::fs::remove_file(filepath).await.unwrap();
    
            // sync parent dir
            tokio::fs::File::open(&parent_dir_path).await.unwrap().sync_data().await.unwrap();
        }
        
        async fn write_to_file_sync_file_and_dir(&self, file: &mut File, parent_dir_path: &PathBuf, content: &Vec<u8> ) {
            file.write_all(&content).await.unwrap();
            file.sync_data().await.unwrap();
            self.sync_dir(parent_dir_path).await;
        }

        async fn get_tmp_content_or_none_if_err(&self, tmp_path: &PathBuf) -> Option<Vec<u8>> {
            // check checksum
            let content_res = tokio::fs::read(&tmp_path).await;
            match content_res {
                Err(_) => return None,
                Ok(content) => {
                    if content.len() < 32 {
                        // incorrect tmp, too short
                        return None;
                    }
                    else {
                        // TODO use custom function?
                        let checksum = &content[content.len() - 32..];
                        let file_content = &content[..content.len()-32];
    
                        // let mut hasher = self.hasher.clone();
                        // hasher.update(&file_content);
                        // let calculated_checksum = hasher.finalize();
                        let calculated_checksum = self.get_checksum(file_content);
    
                        if calculated_checksum.as_slice() == checksum {
                            // correct checksum
                            return Some(file_content.to_vec());
                        }
                        else {
                            return None;
                        }
                    }
                }
            }
        }

        async fn recovery(&mut self) {
            /*
            iterate over sector_dirs
            if there is no tmp dir - add tmp dir
            - if there is tmp dir and tmp file ->
                - check if tmp correct
                    - correct:
                        remove dst - if crash happens, we still have a correct tmp
                        write new dst
                        remove tmp
                    - incorrect
                        remove tmp
            
             */
            let iterator_dir = tokio::fs::read_dir(&self.root_dir).await;
            if let Ok(mut root_iterator)  = iterator_dir{
                // iteratate over sectors
                while let Ok(Some(sector_entry)) = root_iterator.next_entry().await {
                    // if let Some(sector_entry) = sector_dir {
                        let sector_path = sector_entry.path();
                        let tmp_dir_path = self.create_tmp_dir_name_in_sector(&sector_path);
                    
                        // if tmp directory does not exist: create one
                        let tmp_dir_exists = self.sector_file_dir_exists(&tmp_dir_path).await;

                        if !tmp_dir_exists {
                            tokio::fs::create_dir(&tmp_dir_path).await.unwrap();
                            self.sync_dir(&sector_path).await;
                            // there should be no other files and dirs to check
                        }
                        else {
                            // there is tmp folder
                
                            // if tmp has been created, there is a possibility that dst have been created: either we have to recover tmp or check if there is any
                            let dst_path_res = self.get_current_dst_file_path(&sector_path).await;
                            
                            // check if there is a file in tmp folder
                            let tmp_iter_res = tokio::fs::read_dir(&tmp_dir_path).await;
                            if let Ok(mut tmp_iterator) = tmp_iter_res {
                                // there should might one tmp file
                                if let Ok(tmp_file_result) = tmp_iterator.next_entry().await {
                                    if let Some(tmp_file) = tmp_file_result {
                                        let tmp_file_path = tmp_file.path();
                                        // there is tmp file

                                        // check if tmp is correct
                                        // if correct:
                                        // remove dst if exists, write dst, remove tmp
                                        // else:
                                        // remove tmp, proceed
                                        let tmp_content = self.get_tmp_content_or_none_if_err(&tmp_file_path).await;

                                        match tmp_content {
                                            None => {
                                                // checksum is incorrect - delete the file and proceed
                                                self.remove_file(&tmp_file_path, &tmp_dir_path).await;
                                            },
                                            Some(content) => {
                                                // tmp file is correct
                                            //    let dst_path_res = self.get_current_dst_file_path(&sector_path).await;

                                               if let Some(dst_path) = &dst_path_res {
                                                // remove previous dst
                                                self.remove_file(&dst_path, &sector_path).await;
                                               }

                                               let metadata_filename = tmp_file.file_name();

                                               let new_dst_path = sector_path.join(metadata_filename);
                                               let new_dst_file_res = File::create(new_dst_path).await;

                                               if let Ok(mut new_dst_file) = new_dst_file_res {
                                                self.write_to_file_sync_file_and_dir(&mut new_dst_file, &sector_path, &content).await;

                                                    self.remove_file(&tmp_file_path, &tmp_dir_path).await;


                                                    // a new file has been written - 
                                                    if let Some(_) = &dst_path_res {
                                                        let sector_idx: u64 = get_sector_idx_from_filename(&sector_path);
                        
                                                        self.sectors_written_after_recovery.get_or_insert_with(HashSet::new).insert(sector_idx);
                                                    }
                                               }

                                            }
                                        }
                                    }
                                    else {
                                        // there isn't a tmp file, but maybe there is a correct dst then
                                        if let Some(_) = &dst_path_res {
                                            let sector_idx: u64 = get_sector_idx_from_filename(&sector_path);
            
                                            self.sectors_written_after_recovery.get_or_insert_with(HashSet::new).insert(sector_idx);
                                        }
                                    }
                                }
                                // otherwise:
                                // there might be dst file - should be correct

                            // }
                        }
                    }
                    

                }
            }
            // unimplemented!()
        }



        fn get_timestamp_write_rank_from_path(&self, path: PathBuf) -> (u64, u8) {

            
                    // let timestamp_opt = filename.file_stem();//parse().unwrap();
                    // let write_rank_opt = filename.extension(); //.parse().unwrap();

                    // if let Some(timestamp) = timestamp_opt {
                    //     if let Some(write_rank) = write_rank_opt {
                    //         let time: u8 = timestamp.to_str().parse().unwrap();
                    //     }
                    // }
                    let filename = path.file_name();
                    if let Some(fname) = filename {
                        let fname_str = fname.to_str();

                        if let Some(timestamp_rank) = fname_str {
                            let split_pair: Vec<&str> = timestamp_rank.split("_").collect();

                            let timestamp: u64 = split_pair[0].parse().unwrap();
                            let write_rank: u8 = split_pair[1].parse().unwrap();

                            return (timestamp, write_rank);
                        }

                    }

                    // (0, 0)
                    // let fname = path.file_name().to_str();
                    (0, 0)
                    // (timestamp, write_rank)
                }

            fn get_empty_zeroed_vec(&self) -> SectorVec {
                let empty_vec: Vec<u8> = vec![0; CONTENT_SIZE];

                return SectorVec(empty_vec);
            }

            pub fn get_already_written_sectorsafter_recovery(&mut self) -> HashSet<u64>{
                // return self.sectors_written_after_recovery.clone();
                let sectors_res = self.sectors_written_after_recovery.take();
                match sectors_res {
                    None => HashSet::new(),
                    Some(sectors) => sectors,
                }
            }
            
            // let split_pair: Vec<&str> = filename.split("_").collect();
            // let timestamp: u64 = split_pair[0].parse().unwrap();
            // let write_rank: u8 = split_pair[1].parse().unwrap();
            



        // fn handle_filename_retrieval_get_metadata(&self, filename: Option<&OsStr>) -> (u64, u8) {
        //     match filename {
        //         None => return (0, 0),
        //         Some(fname) => return self.get_timestamp_write_rank_from_filename(fname),
        //     }
        // }

        // async fn get_entry(&self, entry_reader: &mut ReadDir) -> Option<DirEntry> {
            
        //             let fst_entry = entry_reader.next_entry().await;
        //             match fst_entry {
        //                 Err(_) => return None,
        //                 Ok(None) => return None,
        //                 Ok(Some(entry1)) => {
        //                     return Some(entry1);
        //                 }
                    
        //     }
        //     // return None;
        // }
    }

    #[async_trait::async_trait]
    impl SectorsManager for ProcessSectorManager {
        async fn read_data(&self, idx: SectorIdx) -> SectorVec {
            let sector_path = self.get_sector_dir(idx);
            let dst_path_res = self.get_current_dst_file_path(&sector_path).await;
            match dst_path_res {
                None => return self.get_empty_zeroed_vec(),
                Some(dst_path) => {
                    let content_or_err = tokio::fs::read(dst_path).await;
                    match content_or_err {
                        Err(_) => {
                            // File probably not found
                            // file not written yet
                            // let empty_vec: Vec<u8> = vec![0; CONTENT_SIZE];

                            // return SectorVec(empty_vec);
                            return self.get_empty_zeroed_vec();
                        }
                        Ok(content) => {
                            return SectorVec(content);
                        }
                    }
                }
            }

            // unimplemented!()
        }

        async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
            // Every atomic register has its own subdirectory for the sector,
            // therefore we can just access the right subdirectory,
            // once knwoing the pattern

            // onlu &self - this is not a monitor

            // if subdir does not exist - not written yet
            // let sector_path = self.root_dir.join(idx.to_string());
            let sector_path = self.get_sector_dir(idx);
            let dst_file_path = self.get_current_dst_file_path(&sector_path).await;
            match dst_file_path {
                None => (0, 0),
                Some(path) => return self.get_timestamp_write_rank_from_path(path),
            }
        }

            // After the recovery - there should be destination file and tmp directory

            // unimplemented!()


        /*
            Structure:
            /root
                /sector_dir
                    - dst_file
                    /tmp_dir
                        - tmp_file

            metadata as filenames
             */

        /*
        After recovery ther should have been left in a sector: tmp dir, eventually dst file
         */
        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
            let sector_path = self.get_sector_dir(idx);
            let tmp_dir_per_sector_path = self.create_tmp_dir_name_in_sector(&sector_path);

            // if there is already a file
            // write first tmp
            // then remove dst
            // recovery - check if tmp exist
            let sector_exists = self.sector_file_dir_exists(&sector_path).await;

            // create a new subdir and fsync if does not exist
            if !sector_exists {
                // I am the only register responsible for the the sector creation,
                // therefore I can create the dir and then fsync 
                tokio::fs::create_dir(&sector_path).await.unwrap();
                // sync root dir
                // tokio::fs::File::open(&self.root_dir).await.unwrap().sync_data().await.unwrap();
                self.sync_dir(&self.root_dir).await;

                // create tmp dir
                tokio::fs::create_dir(&tmp_dir_per_sector_path).await.unwrap();
                // sync sector dir
                self.sync_dir(&sector_path).await;
            }

            // if the sector exists - there should be dst file and a tmp folder
            // from recovery
            let (SectorVec(value), timestamp, write_rank) = sector;
            let checksum = self.get_checksum(&value);
            let content_with_checksum = self.get_file_content_with_checksum(value, checksum);

            let metadata_filename = self.create_filename(timestamp, write_rank);
            // tmp file in tmp dir, with metadata as filename
            let tmp_file_path = self.create_tmp_path_name(&tmp_dir_per_sector_path, &metadata_filename);
            let tmp_file_res = File::create(&tmp_file_path).await;

            // write data with checksum to tmp in tmp dir
            // fsync file
            // fsync tmp dir
            if let Ok(mut tmp_file) = tmp_file_res {

                self.write_to_file_sync_file_and_dir(&mut tmp_file, &tmp_dir_per_sector_path, &content_with_checksum).await;

                // tmp_file.write_all(&content_with_checksum).await.unwrap();
                // tmp_file.sync_data().await.unwrap();
                // self.sync_dir(&tmp_dir_per_sector_path).await;

                // tmp file should be fully written at this moment
                // even if the crash happens during writing of the dst file,
                // the content of the most recent write might be recovered
                // from tmp
                let old_dst_path = self.get_current_dst_file_path(&sector_path).await;

                if let Some(old_path) = old_dst_path {
                    // If there have been dst file
                    /*
                    Structure:
                    /sector_dir
                        - dst_file
                    */
                    self.remove_file(&old_path, &sector_path).await;
                }

                // Write data without checksum to the destination
                // sync dst_file
                // sync dst_dir (sector)
                let dst_path = self.create_new_dst_file_path(&sector_path, &metadata_filename);
                let dst_file_res = File::create(dst_path).await;
                if let Ok(mut dst_file) = dst_file_res {
                    self.write_to_file_sync_file_and_dir(&mut dst_file, &sector_path, value).await;

                    // dst_file.write_all(value).await.unwrap();
                    // dst_file.sync_data().await.unwrap();
                    // self.sync_dir(&sector_path).await;

                    // remove tmp_file
                    self.remove_file(&tmp_file_path, &tmp_dir_per_sector_path).await;
                }
            }
            // unimplemented!()
        }
    }

    #[async_trait::async_trait]
    pub trait SectorsManager: Send + Sync {
        /// Returns 4096 bytes of sector data by index.
        async fn read_data(&self, idx: SectorIdx) -> SectorVec;

        /// Returns timestamp and write rank of the process which has saved this data.
        /// Timestamps and ranks are relevant for atomic register algorithm, and are described
        /// there.
        async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8);

        /// Writes a new data, along with timestamp and write rank to some sector.
        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8));
    }

    /// Path parameter points to a directory to which this method has exclusive access.
    pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
        let mut sector_manager = ProcessSectorManager{
            root_dir: path,
            hasher: Sha256::new(),
            sectors_written_after_recovery: Some(HashSet::new()),
        };
        sector_manager.recovery().await;

        return Arc::new(sector_manager);
        // unimplemented!()
    }
}


pub mod transfer_public {
    use crate::{ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, RegisterCommand, SectorVec, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent, ACK, CONTENT_SIZE, EXTERNAL_UPPER_HALF, MAGIC_NUMBER, PROCESS_CUSTOM_MSG, PROCESS_RESPONSE_ADD, READ_CLIENT_REQ, READ_PROC, VALUE, WRITE_CLIENT_REQ, WRITE_PROC};
    use std::{alloc::System, io::{Error, ErrorKind}};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    // use hmac::digest::KeyInit;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use uuid::timestamp::context;

    // use sha2::Sha256;
    // use hmac::{Hmac, Mac};
    // // use hex_literal::hex;
    
    // Create alias for HMAC-SHA256
    type HmacSha256 = Hmac<Sha256>;

    fn get_system_msg_type(msg: &SystemRegisterCommandContent) -> u8 {
        match &msg {
            SystemRegisterCommandContent::ReadProc => {READ_PROC},
            SystemRegisterCommandContent::Value{..} => {VALUE},
            SystemRegisterCommandContent::WriteProc { .. } => {WRITE_PROC},
            SystemRegisterCommandContent::Ack => {ACK},
        }
    }

    async fn create_mac_or_get_error(writer: &mut (dyn AsyncWrite + Send + Unpin), hmac_key: &[u8], mut msg: Vec<u8>) -> Result<(), Error> {
        let mac_res = HmacSha256::new_from_slice(hmac_key);
                
        match mac_res {
            Err(msg) => {return Err(Error::new(std::io::ErrorKind::InvalidInput, msg.to_string()));},
            Ok(mut mac) => {
                mac.update(&msg);
                let tag = mac.finalize().into_bytes();

                msg.extend_from_slice(&tag);

                writer.write_all(&msg).await?;

                Ok(())
            }
        }
    }

    fn write_val_write_proc(msg: &mut Vec<u8>, timestamp: &u64, write_rank: &u8, sector_data: &SectorVec) -> () {
        msg.extend_from_slice(&timestamp.to_be_bytes());
        let pad_val: [u8; 7] = [0; 7];
        msg.extend_from_slice(&pad_val);
        msg.push(*write_rank);
        
        let SectorVec(vec_to_write) = sector_data;
        msg.extend_from_slice(&vec_to_write);
    }

    async fn is_magic_number_found(data: &mut (dyn AsyncRead + Send + Unpin)) -> Result<bool, Error> {
        let mut read_magic = vec![0; 4];
        let not_found = true;

        // let mut res = data.read_exact(read_magic.as_mut()).await;
        data.read_exact(&mut read_magic).await?;

        while not_found {
            // match res {
            //     Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => {
            //         return Ok(false)
            //     }
            //     Err(err) => {
            //         return Err(err);
            //     }
            //     Ok(_) => {
                    if read_magic == MAGIC_NUMBER {
                        return Ok(true)
                    }

                    // let (first_three, last_byte) = read_magic.split_at_mut(3);
                    // first_three.copy_from_slice(&read_magic[1..4]);
                    let mut temp: [u8; 3] = [0; 3];
                    temp.copy_from_slice(&read_magic[1..4]);
                    read_magic[0..3].copy_from_slice(&temp);

                    // res = data.read_exact(&mut read_magic[3..4]).await;
                    data.read_exact(&mut read_magic[3..4]).await?;

                    //     }
            // }
        }

        Ok(false)

    }

    fn is_message_type_valid(msg_type: u8) -> bool {
        let upper_half = 0xF0 & msg_type;
        let lower_half = 0x0F & msg_type;
        if (lower_half <= ACK) && (lower_half != 0x00) && (upper_half == EXTERNAL_UPPER_HALF || upper_half == PROCESS_RESPONSE_ADD) {
            return true;
        }
        else if upper_half == PROCESS_CUSTOM_MSG {
            return true;
        }

        return false;
    }

    fn create_hmac_wrapper(hmac_key: &[u8]) -> Result<HmacSha256, Error> {
        let mac_res = HmacSha256::new_from_slice(hmac_key);
        match mac_res {
            Err(msg) => {return Err(Error::new(std::io::ErrorKind::InvalidInput, msg.to_string()));},
            Ok(res) => {Ok(res)}
        }
    }

    fn verify_hmac_tag(message: &[u8], tag: &[u8], hmac_key: &[u8]) -> Result<bool, Error> {
        let mac_res = create_hmac_wrapper(hmac_key);

        match mac_res {
            Err(e) => {return Err(e)},
            Ok(mut mac) => {
                mac.update(message);

                return Ok(mac.verify_slice(tag).is_ok());
            }
        }
    }

    fn get_register_command_deserialize_writeproc_val(content: [u8; CONTENT_SIZE], timestamp: [u8; 8], padding_with_rank: [u8; 8], msg_type: u8) -> SystemRegisterCommandContent {

        if msg_type == WRITE_PROC {
            return SystemRegisterCommandContent::WriteProc { timestamp: u64::from_be_bytes(timestamp),
                write_rank: padding_with_rank[7],
                data_to_write: SectorVec(content.to_vec()) };
        }
        else {
            return SystemRegisterCommandContent::Value { timestamp: u64::from_be_bytes(timestamp),
                write_rank: padding_with_rank[7],
                sector_data: SectorVec(content.to_vec()) }
        }
    }

    // TODO use with capacity instead for optimisation
    pub async fn deserialize_register_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        hmac_system_key: &[u8; 64],
        hmac_client_key: &[u8; 32],
    ) -> Result<(RegisterCommand, bool), Error> {
        // let is_magic_nr_found = is_magic_number_found(data).await;

        // match is_magic_nr_found {
        //     Err(e) => {return Err(e)},
        //     Ok(false) => {return Err(Error::new(ErrorKind::UnexpectedEof, "No magic number found to the end of the message"))},
        //     Ok(true) => {
        let full_msg_found = false;

        while !full_msg_found {
            is_magic_number_found(data).await?;
            let mut msg = Vec::new();
            msg.extend_from_slice(&MAGIC_NUMBER);

            let mut padding_rank_msg_type = vec![0; 4];
            data.read_exact(padding_rank_msg_type.as_mut()).await?;

            let msg_type = padding_rank_msg_type[3];

            let is_msg_type_correct = is_message_type_valid(msg_type);
                // }
            
            if !is_msg_type_correct {
                continue;
            }

            msg.extend_from_slice(&padding_rank_msg_type);

            let upper_half = msg_type & 0xF0;
            let lower_half = msg_type & 0x0F;

            if upper_half == EXTERNAL_UPPER_HALF {
                if (lower_half == READ_CLIENT_REQ) || (lower_half == WRITE_CLIENT_REQ) {
                    let mut request_number: [u8; 8] = [0; 8];
                    data.read_exact(request_number.as_mut()).await?;

                    let mut sector_idx: [u8; 8] = [0; 8];
                    data.read_exact(sector_idx.as_mut()).await?;

                    // msg.extend_from_slice(u64::from_be_bytes(request_number));
                    msg.extend_from_slice(&request_number);
                    msg.extend_from_slice(&sector_idx);

                    let mut content: [u8; CONTENT_SIZE] = [0; CONTENT_SIZE];
                    if lower_half == WRITE_CLIENT_REQ {

                        data.read_exact(content.as_mut()).await?;
                        msg.extend_from_slice(&content);
                    }

                    let mut tag: [u8; 32] = [0; 32];
                    data.read_exact(tag.as_mut()).await?;

                    // TODO check if sector is correct

                    let hmac_result = verify_hmac_tag(&msg, &tag, hmac_client_key);

                    match hmac_result {
                        Err(e) => {return Err(e)},
                        Ok(is_hmac_valid) => {
                            let client_header = ClientCommandHeader{
                                request_identifier: u64::from_be_bytes(request_number),
                                sector_idx: u64::from_be_bytes(sector_idx),
                            };
        
                            let mut client_content = ClientRegisterCommandContent::Read;
                            if lower_half == WRITE_CLIENT_REQ {
                                client_content = ClientRegisterCommandContent::Write{data: SectorVec(content.to_vec())};
                            }
        
                            let client_command = RegisterCommand::Client(ClientRegisterCommand{
                                header: client_header,
                                content: client_content,
                            });
        
                            return Ok((client_command, is_hmac_valid));
                        }
                    } 
 
                    
                 }
                 else {
                    let process_rank = padding_rank_msg_type[2];

                    let mut msg_uuid: [u8; 16] = [0; 16];
                    data.read_exact(msg_uuid.as_mut()).await?;

                    let mut sector_idx: [u8; 8] = [0; 8];
                    data.read_exact(sector_idx.as_mut()).await?;

                    msg.extend_from_slice(&msg_uuid);
                    msg.extend_from_slice(&sector_idx);

                    let mut timestamp: [u8; 8] = [0; 8];
                    let mut padding_value_wr: [u8; 8] = [0; 8];
                    let mut content: [u8; CONTENT_SIZE] = [0; CONTENT_SIZE];

                    if (lower_half == VALUE) || (lower_half == WRITE_PROC) {
                        data.read_exact(timestamp.as_mut()).await?;
                        data.read_exact(&mut padding_value_wr).await?;
                        data.read_exact(content.as_mut()).await?;

                        msg.extend_from_slice(&timestamp);
                        msg.extend_from_slice(&padding_value_wr);
                        msg.extend_from_slice(&content);
                    }

                    // if (lower_half == READ_PROC) || (lower_half == ACK) {
                        // no content
                    let mut tag: [u8; 32] = [0; 32];
                    data.read_exact(tag.as_mut()).await?;


                    let hmac_result = verify_hmac_tag(&msg, &tag, hmac_system_key);

                    match hmac_result {
                        Err(e) => {return Err(e)},
                        Ok(is_hmac_valid) => {

                            let register_header = SystemCommandHeader{
                                process_identifier: process_rank,
                                msg_ident: uuid::Uuid::from_u128(u128::from_be_bytes(msg_uuid)),
                                sector_idx: u64::from_be_bytes(sector_idx),
                            };

                            let mut register_content = SystemRegisterCommandContent::ReadProc;
                            if lower_half == ACK {
                                register_content = SystemRegisterCommandContent::Ack;
                            }
                            else  {
                                register_content = get_register_command_deserialize_writeproc_val(content, timestamp, padding_value_wr, msg_type);
                            }

                            let register_command = RegisterCommand::System(
                                SystemRegisterCommand{
                                    header: register_header,
                                    content: register_content,
                                }
                            );

                            return Ok((register_command, is_hmac_valid));
                        }
                    }
                    // }
                 }
            }
            else {
                return Err(Error::new(ErrorKind::Other, "Not implemented other than basic messages"));
            }
            
        }

        return Err(Error::new(ErrorKind::Other, "Not implemented other than basic messages"));
        // unimplemented!()
    }

    pub async fn serialize_register_command(
        cmd: &RegisterCommand,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), Error> {
        let mut msg: Vec<u8> = Vec::new();
        msg.extend_from_slice(&MAGIC_NUMBER);

        match cmd {
            RegisterCommand::Client(client_rcmd) => {
                // TODO move read to static array

                let padding: [u8; 3] = [0; 3];
                msg.extend_from_slice(&padding);

                match &client_rcmd.content {
                    ClientRegisterCommandContent::Read => {
                        msg.push(READ_CLIENT_REQ);
                    },
                    ClientRegisterCommandContent::Write { .. } => {
                        msg.push(WRITE_CLIENT_REQ);
                    },
                }

                let request_number = client_rcmd.header.request_identifier;
                let sector_idx = client_rcmd.header.sector_idx;

                msg.extend_from_slice(&request_number.to_be_bytes());
                msg.extend_from_slice(&sector_idx.to_be_bytes());
                
                if let ClientRegisterCommandContent::Write { data } = &client_rcmd.content {
                    let SectorVec(vec_to_write) = data;
                    msg.extend_from_slice(&vec_to_write);
                }

                // let mut mac_res = HmacSha256::new_from_slice(hmac_key);
                
                // match mac_res {
                //     Err(msg) => {return Err(Error::new(std::io::ErrorKind::InvalidInput, msg.to_string()));},
                //     Ok(mut mac) => {
                //         mac.update(&msg);
                //         let tag = mac.finalize().into_bytes();

                //         msg.extend_from_slice(&tag);

                //         writer.write_all(&msg).await?
                //     }
                // }
                create_mac_or_get_error(writer, hmac_key, msg).await?;

                Ok(())
            },
            RegisterCommand::System(system_rcmd) => {
                let padding: [u8; 2] = [0; 2];
                msg.extend_from_slice(&padding);

                msg.push(system_rcmd.header.process_identifier);
                msg.push(get_system_msg_type(&system_rcmd.content));
                
                let msg_ident = system_rcmd.header.msg_ident.as_u128();
                let sector_idx = system_rcmd.header.sector_idx;
                msg.extend_from_slice(&msg_ident.to_be_bytes());
                msg.extend_from_slice(&sector_idx.to_be_bytes());

                match &system_rcmd.content {
                    SystemRegisterCommandContent::ReadProc => {
                        create_mac_or_get_error(writer, hmac_key, msg).await?;
                        Ok(())
                    },
                    SystemRegisterCommandContent::Value{timestamp, write_rank, sector_data} => {
                        // msg.extend_from_slice(&timestamp.to_be_bytes());
                        // let pad_val: [u8; 7] = [0; 7];
                        // msg.extend_from_slice(&pad_val);
                        // msg.push(*write_rank);
                        
                        // let SectorVec(vec_to_write) = sector_data;
                        // msg.extend_from_slice(&vec_to_write);
                        write_val_write_proc(&mut msg, timestamp, write_rank, sector_data);

                        create_mac_or_get_error(writer, hmac_key, msg).await?;
                        Ok(())
                    },
                    SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write } => {
                        write_val_write_proc(&mut msg, timestamp, write_rank, data_to_write);

                        // writer.write_all(&timestamp.to_be_bytes()).await?;
                        create_mac_or_get_error(writer, hmac_key, msg).await?;
                        Ok(())
                    },
                    SystemRegisterCommandContent::Ack => {
                        create_mac_or_get_error(writer, hmac_key, msg).await?;
                        Ok(())
                    },
                }
            }
        }
        // unimplemented!()
    }
}

pub mod register_client_public {
    use crate::SystemRegisterCommand;
    use std::sync::Arc;

    #[async_trait::async_trait]
    /// We do not need any public implementation of this trait. It is there for use
    /// in AtomicRegister. In our opinion it is a safe bet to say some structure of
    /// this kind must appear in your solution.
    pub trait RegisterClient: core::marker::Send + core::marker::Sync {
        /// Sends a system message to a single process.
        async fn send(&self, msg: Send);

        /// Broadcasts a system message to all processes in the system, including self.
        async fn broadcast(&self, msg: Broadcast);
    }

    pub struct Broadcast {
        pub cmd: Arc<SystemRegisterCommand>,
    }

    pub struct Send {
        pub cmd: Arc<SystemRegisterCommand>,
        /// Identifier of the target process. Those start at 1.
        pub target: u8,
    }
}
