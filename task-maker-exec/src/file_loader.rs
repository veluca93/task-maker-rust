//! Some doc
use serde::{Deserialize, Serialize};
use task_maker_rpc as tmrpc;
use task_maker_store::FileStoreKey;

#[derive(Serialize, Deserialize)]
enum FileId {
    TemporaryFile(usize),
    StoredFile(FileStoreKey),
}

/// Some doc here.
#[tmrpc::service]
pub trait FileLoader {
    async fn open_file(&self, sha256sum: String) -> FileId;
    // Why is there a separate open_file and read_chunk?
    // - might want to read a file multiple times
    // - might want to read a file you don't know the sha of yet (i.e. streaming stdout)
    async fn read_chunk(&self, file_id: FileId) -> Vec<u8>;
}

/// More doc here.
pub struct FileLoaderServer {
    // TODO
}

impl FileLoaderServer {
    async fn open_file(&self, sha256sum: String) -> FileId {
        FileId::TemporaryFile(0)
    }
    async fn read_chunk(&self, file_id: FileId) -> Vec<u8> {
        vec![]
    }
}
