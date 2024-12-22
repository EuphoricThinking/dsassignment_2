mod domain;

pub use crate::domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use transfer_public::*;

// use hmac::{Hmac, Mac};
// use sha2::Sha256;
// use hmac::digest::KeyInit;

// type HmacSha256 = Hmac<Sha256>;

// use hmac::Mac;
// use hmac::digest::KeyInit;

pub async fn run_register_process(config: Configuration) {
    unimplemented!()
}

pub mod atomic_register_public {
    use crate::{
        ClientRegisterCommand, OperationSuccess, RegisterClient, SectorIdx, SectorsManager,
        SystemRegisterCommand,
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

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
        unimplemented!()
    }
}

pub mod sectors_manager_public {
    use crate::{SectorIdx, SectorVec};
    use std::collections::HashSet;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use hmac::digest::generic_array::{ArrayLength, GenericArray};
    use sha2::{Sha256, Digest};
    use tokio::fs::{DirEntry, ReadDir, File};
    use tokio::sync::Mutex;
    use uuid::timestamp;
    use std::io::Error;
    use std::ffi::OsStr;

    
    struct ProcessSectorManager {
        root_dir: PathBuf,
        // RWLock to be implemented
        written_sectors: Arc<Mutex<HashSet<u64>>>,
        hasher: Sha256,
    }

    impl ProcessSectorManager {
        // timestamp_writerank separated by "_"
        fn create_filename(&self, timestamp: u64, write_rank: u8) -> String {
            return timestamp.to_string() + "_" + &write_rank.to_string();
        }

        fn get_sector_dir(&self, idx: SectorIdx) -> PathBuf {
            return self.root_dir.join(idx.to_string());
        }

        fn sector_dir_exists(&self, dirname: &PathBuf) -> bool {
            let checked = dirname.try_exists();
            match checked {
                Err(_) => false,
                Ok(if_exists) => if_exists,
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

        async fn sync_dir(&self, dirname: &PathBuf) -> () {
            tokio::fs::File::open(&self.root_dir).await.unwrap().sync_data().await.unwrap();
        }

        fn get_tmp_dir_for_sector(&self, sector_path: &PathBuf) -> PathBuf {
            return sector_path.join("tmp");
        }

        fn get_timestamp_write_rank_from_filename(&self, path: PathBuf) -> (u64, u8) {

            
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
            
            // let split_pair: Vec<&str> = filename.split("_").collect();
            // let timestamp: u64 = split_pair[0].parse().unwrap();
            // let write_rank: u8 = split_pair[1].parse().unwrap();
            



        // fn handle_filename_retrieval_get_metadata(&self, filename: Option<&OsStr>) -> (u64, u8) {
        //     match filename {
        //         None => return (0, 0),
        //         Some(fname) => return self.get_timestamp_write_rank_from_filename(fname),
        //     }
        // }

        async fn get_entry(&self, entry_reader: &mut ReadDir) -> Option<DirEntry> {
            
                    let fst_entry = entry_reader.next_entry().await;
                    match fst_entry {
                        Err(_) => return None,
                        Ok(None) => return None,
                        Ok(Some(entry1)) => {
                            return Some(entry1);
                        }
                    
            }
            // return None;
        }

    }

    #[async_trait::async_trait]
    impl SectorsManager for ProcessSectorManager {
        async fn read_data(&self, idx: SectorIdx) -> SectorVec {
            unimplemented!()
        }

        async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
            // Every atomic register has its own subdirectory for the sector,
            // therefore we can just access the right subdirectory,
            // once knwoing the pattern

            // onlu &self - this is not a monitor

            // if subdir does not exist - not written yet
            // let sector_path = self.root_dir.join(idx.to_string());
            let sector_path = self.get_sector_dir(idx);

            let mut entries_res = tokio::fs::read_dir(sector_path).await;


            // TODO change to while loop?
            match entries_res {
                Err(_) => return (0, 0),
                Ok(mut entry_reader) => {
                    // let fst_entry = entry_reader.next_entry().await;
                    // match fst_entry {
                    //     Err(_) => return (0,0),
                    //     Ok(None) => return (0,0),
                    //     Ok(Some(entry1)) => {
                    //         if !entry1.path().is_file() {
                    //             // look for the next entry

                    //         }
                    //     }
                    // }
                    let first_entry = self.get_entry(&mut entry_reader).await;
                    match first_entry {
                        None => return (0, 0),
                        Some(entry1) => {
                            let entry1_path = entry1.path();
                            if !entry1_path.is_file() {
                                // tmp dir - we are looking for more entries
                                let second_entry = self.get_entry(&mut entry_reader).await;

                                match second_entry {
                                    None => {return (0, 0)},
                                    Some(entry2) => {
                                        let entry2_path = entry2.path();
                                        if entry2.path().is_file() {
                                            return self.get_timestamp_write_rank_from_filename(entry2_path);
                                        }
                                    }
                                }
                            }
                            else {
                                // this is the destinationf file
                                return self.get_timestamp_write_rank_from_filename(entry1_path);
                            }
                        }
                    }
                }
            }



            // After the recovery - there should be destination file and tmp directory

            unimplemented!()
        }

        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
            let sector_path = self.get_sector_dir(idx);
            let tmp_sector_path = self.get_tmp_dir_for_sector(&sector_path);

            // if there is already a file
            // write first tmp
            // then remove dst
            // recovery - check if tmp exist
            let sector_exists = self.sector_dir_exists(&sector_path);

            // create a new subdir and fsync if does not exist
            if !sector_exists {
                // I am the only register responsible for the the sector creation,
                // therefore I can create the dir and then fsync 
                File::create(&sector_path).await.unwrap();
                // sync root dir
                // tokio::fs::File::open(&self.root_dir).await.unwrap().sync_data().await.unwrap();
                self.sync_dir(&self.root_dir).await;

                // create tmp dir
                File::create(tmp_sector_path).await.unwrap();
                // sync sector dir
                self.sync_dir(&sector_path).await;
            }

            // if the sector exists - there should be dst file and a tmp folder
            // from recovery
            let (SectorVec(value), timestamp, write_rank) = sector;
            let checksum = self.get_checksum(&value);
            let content_with_checksum = self.get_file_content_with_checksum(value, checksum);


            unimplemented!()
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
        unimplemented!()
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
