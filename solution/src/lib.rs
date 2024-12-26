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
        Broadcast, ClientRegisterCommand, ClientRegisterCommandContent, OperationSuccess, RegisterClient, SectorIdx, SectorVec, SectorsManager, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent
    };
    use crate::register_client_public::Send as RegisterSend;
    use std::collections::{HashSet, HashMap};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use uuid::Uuid;

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

        async fn get_value(&mut self) -> SectorVec {
            match &self.value {
                None => {
                    // there should have been value after recovery
                    return self.sectors_manager.read_data(self.sector_idx).await;
                },
                Some(val) => {return val.clone();},
            }
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

            match content {
                ClientRegisterCommandContent::Read => {
                    self.operation_id = Uuid::new_v4();
                    self.readlist = HashMap::new();
                    self.acklist = HashSet::new();
                    self.reading = true;

                    // self.register_client.broadcast(msg)
                    self.send_broadcast_readproc_command().await;
                    
                },
                ClientRegisterCommandContent::Write{data} => {
                    self.operation_id = Uuid::new_v4();
                    self.writeval = Some(data);
                    self.acklist = HashSet::new();
                    self.readlist = HashMap::new();
                    self.writing = true;

                    self.send_broadcast_readproc_command().await;
                }
            }
            // unimplemented!()
        }

        async fn system_command(&mut self, cmd: SystemRegisterCommand) {
            let SystemRegisterCommand{header, content} = cmd;

            let SystemCommandHeader{process_identifier, msg_ident, sector_idx} = header;

            let receiver_id = process_identifier;

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
                            target: receiver_id,
                        };

                        self.register_client.send(msg).await;
                    },
                    SystemRegisterCommandContent::Value { timestamp, write_rank, sector_data } => {
                        if (header.msg_ident == self.operation_id) && !self.write_phase {
                            self.readlist.insert(header.process_identifier, SectorData { timestamp, write_rank, value: sector_data});

                            if (self.readlist.len() > (self.processes_count / 2).into()) && (self.reading || self.writing) {
                                
                            }
                        }

                    },
                    SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write } => {

                    },
                    SystemRegisterCommandContent::Ack => {

                    },
                }
            }
            unimplemented!()
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

            // last_issued_command: None,
        };

        new_register.recovery().await;

        return Box::new(new_register);
        // unimplemented!()
    }
}

pub mod sectors_manager_public {
    use crate::{SectorIdx, SectorVec, CONTENT_SIZE};
    use std::collections::HashSet;
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

    
    struct ProcessSectorManager {
        root_dir: PathBuf,
        // RWLock to be implemented
        // written_sectors: Arc<Mutex<HashSet<u64>>>,
        hasher: Sha256,
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

        async fn recovery(&self) {
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
            // println!("here");
            let iterator_dir = tokio::fs::read_dir(&self.root_dir).await;
            if let Ok(mut root_iterator)  = iterator_dir{
                // iteratate over sectors
                // println!("iterator is ok");
                while let Ok(Some(sector_entry)) = root_iterator.next_entry().await {
                    // println!("root_iter {:?}", sector_dir);
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
                            // check if there is a file
                            let tmp_iter_res = tokio::fs::read_dir(&tmp_dir_path).await;
                            if let Ok(mut tmp_iterator) = tmp_iter_res {
                                // there should be one tmp file
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
                                               let dst_path_res = self.get_current_dst_file_path(&sector_path).await;

                                               if let Some(dst_path) = dst_path_res {
                                                // remove previous dst
                                                self.remove_file(&dst_path, &sector_path).await;
                                               }

                                               let metadata_filename = tmp_file.file_name();

                                               let new_dst_path = sector_path.join(metadata_filename);
                                               let new_dst_file_res = File::create(new_dst_path).await;

                                               if let Ok(mut new_dst_file) = new_dst_file_res {
                                                self.write_to_file_sync_file_and_dir(&mut new_dst_file, &sector_path, &content).await;

                                                    self.remove_file(&tmp_file_path, &tmp_dir_path).await;
                                               }

                                            }
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
            let dst_path = self.get_current_dst_file_path(&sector_path).await.unwrap();
            let content_or_err = tokio::fs::read(dst_path).await;
            match content_or_err {
                Err(_) => {
                    // File probably not found
                    // file not written yet
                    let empty_vec: Vec<u8> = vec![0; CONTENT_SIZE];

                    return SectorVec(empty_vec);
                }
                Ok(content) => {
                    return SectorVec(content);
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

                // println!("sector: {:?}, tmp: {:?}", sector_path, tmp_dir_per_sector_path);
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
        let sector_manager = ProcessSectorManager{
            root_dir: path,
            hasher: Sha256::new(),
        };
        // println!("new");
        sector_manager.recovery().await;
        // println!("recovered");

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
