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
    use std::path::PathBuf;
    use std::sync::Arc;

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
    use crate::{ClientRegisterCommandContent, RegisterCommand, SectorVec, SystemRegisterCommand, SystemRegisterCommandContent, ACK, MAGIC_NUMBER, READ_CLIENT_REQ, READ_PROC, VALUE, WRITE_CLIENT_REQ, WRITE_PROC};
    use std::{alloc::System, io::{Error, ErrorKind}};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    // use hmac::digest::KeyInit;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

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
        let mut mac_res = HmacSha256::new_from_slice(hmac_key);
                
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

    fn write_val_write_proc(mut msg: Vec<u8>, timestamp: &u64, write_rank: &u8, sector_data: &SectorVec) -> () {
        msg.extend_from_slice(&timestamp.to_be_bytes());
        let pad_val: [u8; 7] = [0; 7];
        msg.extend_from_slice(&pad_val);
        msg.push(*write_rank);
        
        let SectorVec(vec_to_write) = sector_data;
        msg.extend_from_slice(&vec_to_write);
    }

    async fn is_magic_number_not_found(data: &mut (dyn AsyncRead + Send + Unpin)) -> Result<bool, Error> {
        let mut read_magic = vec![0; 4];
        let not_found = true;

        let mut res = data.read_exact(read_magic.as_mut()).await;

        while not_found {
            match res {
                Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => {
                    return Ok(false)
                }
                Err(err) => {
                    return Err(err);
                }
                Ok(_) => {
                    if read_magic == MAGIC_NUMBER {
                        return Ok(true)
                    }

                    // let (first_three, last_byte) = read_magic.split_at_mut(3);
                    // first_three.copy_from_slice(&read_magic[1..4]);
                    let mut temp: [u8; 3] = [0; 3];
                    temp.copy_from_slice(&read_magic[1..4]);
                    read_magic[0..3].copy_from_slice(&temp);

                    res = data.read_exact(&mut read_magic[3..4]).await;
                }
            }
        }

        Ok(true)

    }
    pub async fn deserialize_register_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        hmac_system_key: &[u8; 64],
        hmac_client_key: &[u8; 32],
    ) -> Result<(RegisterCommand, bool), Error> {

        unimplemented!()
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
                    ClientRegisterCommandContent::Write { data } => {
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
                create_mac_or_get_error(writer, hmac_key, msg).await?
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
                        create_mac_or_get_error(writer, hmac_key, msg).await?
                    },
                    SystemRegisterCommandContent::Value{timestamp, write_rank, sector_data} => {
                        // msg.extend_from_slice(&timestamp.to_be_bytes());
                        // let pad_val: [u8; 7] = [0; 7];
                        // msg.extend_from_slice(&pad_val);
                        // msg.push(*write_rank);
                        
                        // let SectorVec(vec_to_write) = sector_data;
                        // msg.extend_from_slice(&vec_to_write);
                        write_val_write_proc(msg, timestamp, write_rank, sector_data);

                        create_mac_or_get_error(writer, hmac_key, msg).await?
                    },
                    SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write } => {
                        write_val_write_proc(msg, timestamp, write_rank, data_to_write);

                        // writer.write_all(&timestamp.to_be_bytes()).await?;
                        create_mac_or_get_error(writer, hmac_key, msg).await?
                    },
                    SystemRegisterCommandContent::Ack => {
                        create_mac_or_get_error(writer, hmac_key, msg).await?
                    },
                }
            }
        }
        unimplemented!()
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
