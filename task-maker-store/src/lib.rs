//! This crate manages the file store on disk, a folder with many files indexed by their hash.
//!
//! The files are stored in a read-only manner (removing the write bit permission) and their access
//! is granted via their hash. The size of the store folder is limited to a specific amount and the
//! least-recently-used files are removed automatically.
//!
//! The access to the store directory via this crate is exclusive even between processes.
//!
//! # Example
//!
//! Storing a file into the store and getting it back later.
//! ```
//! use task_maker_store::{FileStore, FileStoreKey, ReadFileIterator};
//!
//! # use failure::Error;
//! # use std::fs;
//! # use tempdir::TempDir;
//! # fn main() -> Result<(), Error> {
//! # let tmp = TempDir::new("tm-test").unwrap();
//! # let store_dir = tmp.path().join("store");
//! # let path = tmp.path().join("file.txt");
//! # fs::write(&path, "hello world")?;
//! // make a new store based on a directory, this will lock if the store is already in use
//! let mut store = FileStore::new(store_dir)?;
//! // compute the key of a file and make an iterator over its content
//! let key = FileStoreKey::from_file(&path)?;
//! let iter = ReadFileIterator::new(&path)?;
//! // store the file inside the file store
//! store.store(&key, iter)?;
//! // store.get(&key) will return the path on disk of the file if present inside the store
//! assert!(store.get(&key)?.is_some());
//! # Ok(())
//! # }
//! ```

#![deny(missing_docs)]

#[macro_use]
extern crate log;

mod read_file_iterator;
pub use read_file_iterator::ReadFileIterator;

use blake2::{Blake2b, Digest};
use chrono::prelude::*;
use failure::{Error, Fail};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// How long a file must persist on disk after an access
const PERSISTENCY_DURATION: Duration = Duration::from_secs(600);
/// Whether to check the file integrity on the store before getting it
const CHECK_INTEGRITY: bool = true;
const FILE_STORE_FILE: &str = "store_info";

/// The type of an hash of a file
type HashData = Vec<u8>;

/// A file store will manage all the files in the store directory.
///
/// This will manage a file storage directory with the ability of:
/// * remove files not needed anymore that takes too much space
/// * locking so no other instances of FileStorage can access the storage while
///   this is still running
/// * do not remove files useful for the current computations
#[derive(Debug)]
pub struct FileStore {
    /// Base directory of the FileStore
    base_path: PathBuf,
    /// Handle of the file with the data of the store. This handle keeps the
    /// lock alive.
    file: File,
    /// Data of the FileStore with the list of known files
    data: FileStoreData,
}

/// Handle of a file in the FileStore, this must be computable given the
/// content of the file, i.e. an hash of the content.
#[derive(Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct FileStoreKey {
    /// An hash of the content of the file
    hash: HashData,
}

/// Errors generated by the FileStore
#[derive(Debug, Fail)]
pub enum FileStoreError {
    /// An invalid path is provided.
    #[fail(display = "invalid path provided")]
    InvalidPath,
    /// The file is not present in the store.
    #[fail(display = "file not present in the store")]
    NotFound,
}

/// The content of an entry of a file in the FileStore
#[derive(Debug, Serialize, Deserialize)]
struct FileStoreItem {
    /// Timestamp of when the file may be deleted
    persistent: DateTime<Utc>,
    // TODO change this to a refcounted struct which holds the lock to that
    // file
}

/// Internal data of the FileStore
#[derive(Debug, Serialize, Deserialize)]
struct FileStoreData {
    /// List of the known files, this should be JSON serializable
    items: HashMap<String, FileStoreItem>,
}

impl FileStoreKey {
    /// Make the key related to the specified file.
    ///
    /// ```
    /// use task_maker_store::FileStoreKey;
    ///
    /// # use failure::Error;
    /// # use std::fs;
    /// # fn main() -> Result<(), Error> {
    /// # fs::write("/tmp/file.txt", "hello world")?;
    /// let key = FileStoreKey::from_file("/tmp/file.txt")?;
    /// println!("The key is {}", key.to_string());
    /// # // clear the test data
    /// # fs::remove_file("/tmp/file.txt")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<FileStoreKey, Error> {
        let mut hasher = Blake2b::new();
        let file_reader = ReadFileIterator::new(path.as_ref())?;
        file_reader.map(|buf| hasher.input(&buf)).last();
        Ok(FileStoreKey {
            hash: hasher.result().to_vec(),
        })
    }
}

impl std::string::ToString for FileStoreKey {
    fn to_string(&self) -> String {
        hex::encode(&self.hash)
    }
}

impl std::fmt::Debug for FileStoreKey {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        fmt.write_str(&hex::encode(&self.hash))
    }
}

impl FileStoreItem {
    /// Make a new FileStoreItem
    fn new() -> FileStoreItem {
        FileStoreItem {
            persistent: Utc::now(),
        }
    }

    /// Mark the file as persistent
    fn persist(&mut self) {
        let now = Utc::now().timestamp();
        let target = now + (PERSISTENCY_DURATION.as_secs() as i64);
        self.persistent = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(target, 0), Utc);
    }
}

impl FileStoreData {
    /// Make a new FileStoreData
    fn new() -> FileStoreData {
        FileStoreData {
            items: HashMap::new(),
        }
    }

    /// Get a mutable reference to the item with that key, creating it if
    /// needed
    fn get_mut(&mut self, key: &FileStoreKey) -> &mut FileStoreItem {
        let key = key.to_string();
        if !self.items.contains_key(&key) {
            self.items.insert(key.clone(), FileStoreItem::new());
        }
        self.items.get_mut(&key).unwrap()
    }

    /// Remove an item from the list of know files. This wont remove the actual
    /// file on disk
    fn remove(&mut self, key: &FileStoreKey) -> Option<FileStoreItem> {
        self.items.remove(&key.to_string())
    }
}

impl FileStore {
    /// Make a new FileStore in the specified base directory, will lock if
    /// another instance of a FileStore is locking the data file.
    ///
    /// ```
    /// use task_maker_store::FileStore;
    ///
    /// # use failure::Error;
    /// # use std::fs;
    /// # fn main() -> Result<(), Error> {
    /// // make a new store based on a directory, this will lock if the store is already in use
    /// let mut store = FileStore::new("/tmp/store")?;
    /// // FileStore::new("/tmp/store") // this will lock!!
    /// # // clear the test data
    /// # std::mem::drop(store);
    /// # fs::remove_dir_all("/tmp/store")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<FileStore, Error> {
        std::fs::create_dir_all(base_path.as_ref())?;
        let path = base_path.as_ref().join(FILE_STORE_FILE);
        if !path.exists() {
            serde_json::to_writer(File::create(&path)?, &FileStoreData::new())?;
        }
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        if let Err(e) = file.try_lock_exclusive() {
            if e.to_string() != fs2::lock_contended_error().to_string() {
                return Err(e.into());
            }
            warn!("Store locked... waiting");
            file.lock_exclusive()?;
        }
        let data = FileStore::read_store_file(&file, base_path.as_ref())?;
        Ok(FileStore {
            base_path: base_path.as_ref().to_owned(),
            file,
            data,
        })
    }

    /// Given an iterator of `Vec<u8>` consume all of it writing the content to
    /// the disk if the file is not already present on disk. The file is stored
    /// inside the base directory and `chmod -w`.
    ///
    /// If the file is already present it is not overwritten but the iterator is consumed
    /// nevertheless.
    pub fn store<I>(&mut self, key: &FileStoreKey, content: I) -> Result<(), Error>
    where
        I: Iterator<Item = Vec<u8>>,
    {
        let path = self.key_to_path(key);
        trace!("Storing {:?}", path);
        if self.has_key(key) {
            trace!("File {:?} already exists", path);
            content.last(); // consume all the iterator
            self.data.get_mut(key).persist();
            self.flush()?;
            return Ok(());
        }
        // TODO make write the file to a .temp and then move to the final place?
        // not sure if needed since this is in a &mut self and should not be executed
        // in parallel even between processes
        std::fs::create_dir_all(path.parent().unwrap())?;
        let mut file = std::fs::File::create(&path)?;
        content.map(|data| file.write_all(&data)).last();
        FileStore::mark_readonly(&path)?;
        self.data.get_mut(key).persist();
        self.flush()?;
        Ok(())
    }

    /// Returns the path of the file with that key or `None` if it's not in the
    /// [`FileStore`](struct.FileStore.html).
    ///
    /// This requires mutability because it will actively fix any corrupted or missing files in the
    /// store.
    ///
    /// Note that accessing a file will mark it as _persistent_, preventing its flushing for a
    /// while.
    pub fn get(&mut self, key: &FileStoreKey) -> Result<Option<PathBuf>, Error> {
        let path = self.key_to_path(key);
        if !path.exists() {
            self.data.remove(&key);
            self.flush()?;
            return Ok(None);
        }
        if CHECK_INTEGRITY && !self.check_integrity(key) {
            warn!("File {:?} failed the integrity check", path);
            self.data.remove(key);
            FileStore::remove_file(&path)?;
            return Ok(None);
        }
        self.persist(key)?;
        Ok(Some(path))
    }

    /// Checks if the store has that key inside.
    ///
    /// This may drop the file if it's corrupted, because of that this requires `&mut self`.
    pub fn has_key(&mut self, key: &FileStoreKey) -> bool {
        let path = self.key_to_path(key);
        if !path.exists() {
            return false;
        }
        if CHECK_INTEGRITY && !self.check_integrity(&key) {
            warn!("File {:?} failed the integrity check", path);
            self.data.remove(key);
            FileStore::remove_file(&path).expect("Cannot remove corrupted file");
            return false;
        }
        true
    }

    /// Mark the file as persistent. Being persistent means that the file will be used in the near
    /// future, telling the store to not drop it for a while.
    ///
    /// The exact time the file is kept in the store is hardcoded to be 5 minutes.
    pub fn persist(&mut self, key: &FileStoreKey) -> Result<(), Error> {
        let path = self.key_to_path(key);
        if !path.exists() {
            return Err(FileStoreError::NotFound.into());
        }
        self.data.get_mut(key).persist();
        self.flush()?;
        Ok(())
    }

    /// Write the FileStore data to disk. Some internal structures are kept in memory for
    /// performance reasons, flushing them will prevent losing in case of a panic or an abort.
    ///
    /// This method will be called internally on Drop.
    pub fn flush(&mut self) -> Result<(), Error> {
        let serialized = serde_json::to_string(&self.data)?;
        self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.write_all(serialized.as_bytes())?;
        self.file.set_len(serialized.len() as u64)?;
        Ok(())
    }

    /// Path of the file to disk.
    fn key_to_path(&self, key: &FileStoreKey) -> PathBuf {
        let first = hex::encode(vec![key.hash[0]]);
        let second = hex::encode(vec![key.hash[1]]);
        let full = hex::encode(&key.hash);
        Path::new(&self.base_path)
            .join(first)
            .join(second)
            .join(full)
            .to_owned()
    }

    /// Read the FileStore data file from disk and remove missing files from it. This call is pretty
    /// sys-call expensive.
    fn read_store_file(file: &File, base_path: &Path) -> Result<FileStoreData, Error> {
        let mut data: FileStoreData = serde_json::from_reader(file)?;
        // remove files not present anymore
        data.items = data
            .items
            .into_iter()
            .filter(|(key, _)| {
                base_path
                    .join(&key[0..2])
                    .join(&key[2..4])
                    .join(key)
                    .exists()
            })
            .collect();
        Ok(data)
    }

    /// Mark a file as readonly.
    fn mark_readonly(path: &Path) -> Result<(), Error> {
        let mut perms = std::fs::metadata(path)?.permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(path, perms)?;
        Ok(())
    }

    /// Remove a file from disk.
    fn remove_file(path: &Path) -> Result<(), Error> {
        let mut perms = std::fs::metadata(path)?.permissions();
        perms.set_readonly(false);
        std::fs::set_permissions(path, perms)?;
        std::fs::remove_file(path)?;
        Ok(())
    }

    /// Check if the file is not corrupted.
    fn check_integrity(&self, key: &FileStoreKey) -> bool {
        let path = self.key_to_path(key);
        let metadata = std::fs::metadata(&path);
        // if the last modified time is the same of creation time assume it's
        // not corrupted
        if let Ok(metadata) = metadata {
            let created = metadata.created();
            let modified = metadata.modified();
            match (created, modified) {
                (Ok(created), Ok(modified)) => {
                    if created == modified {
                        return true;
                    }
                }
                (_, _) => {}
            }
        }
        match FileStoreKey::from_file(&path) {
            Ok(key2) => key2.hash == key.hash,
            Err(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::{assert_eq, assert_ne};
    use std::fs::*;
    use std::io::{Read, Write};
    use tempdir::TempDir;

    fn get_cwd() -> TempDir {
        TempDir::new("tm-test").unwrap()
    }

    fn fake_key() -> FileStoreKey {
        FileStoreKey {
            hash: vec![1, 2, 3, 4, 5, 6, 7, 8],
        }
    }

    fn fake_file(path: &Path, content: &str) -> FileStoreKey {
        File::create(path)
            .unwrap()
            .write_all(&content.as_bytes())
            .unwrap();
        FileStoreKey::from_file(path).unwrap()
    }

    fn add_file_to_store(path: &Path, content: &str, store: &mut FileStore) -> FileStoreKey {
        let key = fake_file(path, content);
        let iter = ReadFileIterator::new(path).unwrap();
        store.store(&key, iter).unwrap();
        key
    }

    fn corrupt_file(path: &Path) {
        {
            let file = File::open(&path).unwrap();
            let mut perm = file.metadata().unwrap().permissions();
            perm.set_readonly(false);
            file.set_permissions(perm).unwrap();
        }
        OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .write_all(b"lol")
            .unwrap();
    }

    #[test]
    fn test_new_filestore() {
        let cwd = get_cwd();
        let _store = FileStore::new(cwd.path()).unwrap();
        assert!(cwd.path().join(FILE_STORE_FILE).exists());
    }

    #[test]
    fn test_new_filestore_concurrent() {
        use std::time::*;

        let cwd = get_cwd();
        let store_dir = cwd.path().to_owned();
        let store = FileStore::new(cwd.path()).unwrap();
        let thr = std::thread::spawn(move || {
            let start = Instant::now();
            let _store = FileStore::new(&store_dir).unwrap();
            let end = Instant::now();
            assert!(end - start >= Duration::from_millis(300));
        });
        std::thread::sleep(Duration::from_millis(500));
        drop(store);
        thr.join().unwrap();
    }

    #[test]
    fn test_corrupted_filestore() {
        let cwd = get_cwd();
        File::create(cwd.path().join(FILE_STORE_FILE))
            .unwrap()
            .write_all(&[1, 2, 3, 4, 5])
            .unwrap();
        assert!(FileStore::new(cwd.path()).is_err());
    }

    #[test]
    fn test_store() {
        let cwd = get_cwd();
        let mut store = FileStore::new(cwd.path()).unwrap();
        let key = add_file_to_store(&cwd.path().join("test.txt"), "test", &mut store);
        let path_in_store = store.key_to_path(&key);
        assert!(path_in_store.exists());
        let mut content = String::new();
        File::open(&path_in_store)
            .unwrap()
            .read_to_string(&mut content)
            .unwrap();
        assert_eq!(&content, "test");
        assert!(File::open(&path_in_store)
            .unwrap()
            .metadata()
            .unwrap()
            .permissions()
            .readonly());
    }

    #[test]
    fn test_get() {
        let cwd = get_cwd();
        let mut store = FileStore::new(cwd.path()).unwrap();
        let key = add_file_to_store(&cwd.path().join("test.txt"), "ciao", &mut store);
        let path = store.get(&key).unwrap();
        let path_in_store = store.key_to_path(&key);
        assert_eq!(Some(path_in_store), path);
    }

    #[test]
    fn test_get_removed() {
        let cwd = get_cwd();
        let mut store = FileStore::new(cwd.path()).unwrap();
        let key = add_file_to_store(&cwd.path().join("test.txt"), "ciao", &mut store);
        let path_in_store = store.key_to_path(&key);
        remove_file(path_in_store).unwrap();
        let path = store.get(&key).unwrap();
        assert_eq!(None, path);
    }

    #[test]
    fn test_get_not_known() {
        let cwd = get_cwd();
        let mut store = FileStore::new(cwd.path()).unwrap();
        let key = fake_file(&cwd.path().join("test.txt"), "ciao");
        let path = store.get(&key).unwrap();
        assert_eq!(None, path);
    }

    #[test]
    fn test_corrupted_file() {
        if !CHECK_INTEGRITY {
            return;
        }
        let cwd = get_cwd();
        let mut store = FileStore::new(cwd.path()).unwrap();
        let key = add_file_to_store(&cwd.path().join("test.txt"), "ciao", &mut store);
        let path_in_store = store.key_to_path(&key);
        corrupt_file(&path_in_store);
        let path = store.get(&key).unwrap();
        assert_eq!(None, path);
    }

    #[test]
    fn test_has_key() {
        let cwd = get_cwd();
        let mut store = FileStore::new(cwd.path()).unwrap();
        let key = add_file_to_store(&cwd.path().join("test.txt"), "ciao", &mut store);
        assert!(store.has_key(&key));
    }

    #[test]
    fn test_has_key_not_present() {
        let cwd = get_cwd();
        let mut store = FileStore::new(cwd.path()).unwrap();
        let key = fake_file(&cwd.path().join("test.txt"), "ciaone");
        assert!(!store.has_key(&key));
    }

    #[test]
    fn test_has_key_corrupted() {
        if !CHECK_INTEGRITY {
            return;
        }
        let cwd = get_cwd();
        let mut store = FileStore::new(cwd.path()).unwrap();
        let key = add_file_to_store(&cwd.path().join("test.txt"), "ciaone", &mut store);
        let path = store.key_to_path(&key);
        corrupt_file(&path);
        assert!(!store.has_key(&key));
    }

    #[test]
    fn test_key_to_path() {
        let cwd = get_cwd();
        let store = FileStore::new(cwd.path()).unwrap();
        let key = fake_file(&cwd.path().join("test.txt"), "ciao");
        let path = store.key_to_path(&key);
        assert!(path.starts_with(store.base_path));
        assert!(path.ends_with(key.to_string()));
    }

    #[test]
    fn test_mark_readonly() {
        let cwd = get_cwd();
        let path = cwd.path().join("test.txt");
        File::create(&path).unwrap();
        FileStore::mark_readonly(&path).unwrap();
        assert!(File::open(&path)
            .unwrap()
            .metadata()
            .unwrap()
            .permissions()
            .readonly());
    }

    #[test]
    fn test_remove_file() {
        let cwd = get_cwd();
        let path = cwd.path().join("test.txt");
        File::create(&path).unwrap();
        FileStore::mark_readonly(&path).unwrap();
        FileStore::remove_file(&path).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn test_check_integrity() {
        let cwd = get_cwd();
        let mut store = FileStore::new(&cwd.path()).unwrap();
        let key = add_file_to_store(&cwd.path().join("test.txt"), "ciaone", &mut store);
        let path = store.key_to_path(&key);
        corrupt_file(&path);
        assert!(!store.check_integrity(&key));
    }

    // TODO add tests for read_store_file

    #[test]
    fn test_file_store_data_get_mut() {
        let mut data = FileStoreData::new();
        let key = fake_key();
        let date = Utc::now();
        data.items.insert(key.to_string(), FileStoreItem::new());
        data.get_mut(&key).persistent = date;
        assert_eq!(data.get_mut(&key).persistent, date);
    }

    #[test]
    fn test_file_store_data_get_mut_create() {
        let mut data = FileStoreData::new();
        let key = fake_key();
        let date = Utc::now();
        data.get_mut(&key).persistent = date;
        assert_eq!(data.get_mut(&key).persistent, date);
    }

    #[test]
    fn test_file_store_data_remove() {
        let mut data = FileStoreData::new();
        let key = fake_key();
        data.items.insert(key.to_string(), FileStoreItem::new());
        data.remove(&key);
        assert!(!data.items.contains_key(&key.to_string()));
    }

    #[test]
    fn test_file_store_key_from_file() {
        let cwd = get_cwd();
        fake_file(&cwd.path().join("file1a.txt"), "ciao");
        fake_file(&cwd.path().join("file1b.txt"), "ciao");
        fake_file(&cwd.path().join("file2.txt"), "ciaone");

        let key1a = FileStoreKey::from_file(&cwd.path().join("file1a.txt")).unwrap();
        let key1b = FileStoreKey::from_file(&cwd.path().join("file1b.txt")).unwrap();
        let key2 = FileStoreKey::from_file(&cwd.path().join("file2.txt")).unwrap();

        assert_eq!(key1a, key1b);
        assert_ne!(key1a, key2);
        assert_ne!(key1b, key2);
    }
}
