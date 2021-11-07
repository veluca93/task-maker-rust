//! Crate for managing the cache of the executions of a DAG.
//!
//! It provides the `Cache` struct which holds the cache data and stores it on disk on Drop. The
//! executions are cached computing a cache key based on the execution command, arguments and
//! inputs. For each cache key there may be more than one cache entry, allowing different execution
//! limits to be used.
//!
//! The algorithm for extending a cache entry for a different limit is the following:
//! - call `E1` the cached execution's result and `L1` its limits
//! - call `E2` the execution to check and `L2` its limits
//! - if `E1` was successful with `L1` and `L2` is _less restrictive_ than `L1`, `E2` will be
//!   successful
//! - if `E1` wasn't successful with `L1` and `L1` is _less restrictive_ than `L2`, `E2` won't be
//!   successful
//!
//! `L1` is _less restrictive_ than `L2` if there is no limit on `L1` that is _more restrictive_
//! than the corresponding one in `L2`. If a limit is not present, its value is assumed to be
//! _infinite_.
//!
//! # Example
//!
//! ```
//! use tempdir::TempDir;
//! use task_maker_cache::{Cache, CacheResult};
//! use std::collections::HashMap;
//! use task_maker_dag::{Execution, ExecutionCommand, ExecutionResult, ExecutionStatus, ExecutionResourcesUsage, File};
//! use task_maker_store::{FileStore, FileStoreKey, ReadFileIterator};
//!
//! // make a new store and a new cache in a testing environment
//! let dir = TempDir::new("tm-test").unwrap();
//! let mut cache = Cache::new(dir.path()).expect("Cannot create the cache");
//! let mut store = FileStore::new(dir.path(), 1000, 1000).expect("Cannot create the store");
//!
//! // setup a testing file
//! let path = dir.path().join("file.txt");
//! std::fs::write(&path, [1, 2, 3, 4]).unwrap();
//!
//! // build a testing execution
//! let mut exec = Execution::new("Testing exec", ExecutionCommand::system("true"));
//! let input = File::new("Input file");
//! exec.input(&input, "sandbox_path", false);
//!
//! // emulate the execution
//! let result = ExecutionResult {
//!     status: ExecutionStatus::Success,
//!     resources: ExecutionResourcesUsage {
//!         cpu_time: 1.123,
//!         sys_time: 0.2,
//!         wall_time: 1.5,
//!         memory: 12345
//!     },
//!     was_killed: false,
//!     was_cached: false,
//!     stderr: None,
//!     stdout: None,
//! };
//!
//! // make the FileUuid -> FileStoreHandle map
//! let key = FileStoreKey::from_file(&path).unwrap();
//! let mut file_keys = HashMap::new();
//! file_keys.insert(input.uuid, store.store(&key, ReadFileIterator::new(&path).unwrap()).unwrap());
//!
//! // insert the result in the cache
//! cache.insert(&exec.clone().into(), &file_keys, vec![result]);
//!
//! // retrieve the result from the cache
//! let res = cache.get(&exec.into(), &file_keys, &mut store);
//! match res {
//!     CacheResult::Miss => panic!("Expecting a hit"),
//!     CacheResult::Hit { result, outputs } => {
//!         assert_eq!(result[0].status, ExecutionStatus::Success);
//!         assert_eq!(result[0].resources.memory, 12345);
//!     }
//! }
//! ```

#![deny(missing_docs)]
#![allow(clippy::upper_case_acronyms)]

#[macro_use]
extern crate log;

mod entry;
mod key;
mod storage;
use entry::CacheEntry;
use key::CacheKey;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Error;
use itertools::Itertools;

use task_maker_dag::{ExecutionGroup, ExecutionResult, ExecutionStatus, FileUuid};
use task_maker_store::{FileStore, FileStoreHandle};

/// The name of the file which holds the cache data.
const CACHE_FILE: &str = "cache.bin";

/// Handle the cached executions, loading and storing them to disk.
#[derive(Debug)]
pub struct Cache {
    /// All the cached entries.
    pub(crate) entries: HashMap<CacheKey, Vec<CacheEntry>>,
    /// The path to the cache file.
    pub(crate) cache_file: PathBuf,
}

/// The result of a cache query, can be either successful (`Hit`) or unsuccessful (`Miss`).
pub enum CacheResult {
    /// The requested entry is not present in the cache.
    Miss,
    /// The requested entry is present in the cache.
    Hit {
        /// The result of the execution.
        result: Vec<ExecutionResult>,
        /// The outputs of the execution.
        outputs: HashMap<FileUuid, FileStoreHandle>,
    },
}

impl Cache {
    /// Make a new `Cache` stored in the specified cache directory. If the cache file is present
    /// it will be used and its content will be loaded, if valid, otherwise an error is returned.
    pub fn new<P: AsRef<Path>>(cache_dir: P) -> Result<Cache, Error> {
        let path = cache_dir.as_ref().join(CACHE_FILE);
        if path.exists() {
            let entries = storage::load(&path);
            if let Err(e) = &entries {
                error!("Cache store is broken, resetting: {:?}", e);
            }
            Ok(Cache {
                entries: entries.unwrap_or_default(),
                cache_file: path,
            })
        } else {
            Ok(Cache {
                entries: HashMap::new(),
                cache_file: path,
            })
        }
    }

    /// Insert a new entry inside the cache. They key is computed based on the execution's metadata
    /// and on the hash of it's inputs, defined by the mapping `file_keys` from the UUIDs of the DAG
    /// to the persistent `FileStoreKey`s.
    pub fn insert(
        &mut self,
        group: &ExecutionGroup,
        file_keys: &HashMap<FileUuid, FileStoreHandle>,
        result: Vec<ExecutionResult>,
    ) {
        let key = CacheKey::from_execution_group(group, file_keys);
        let set = self.entries.entry(key).or_default();
        let entry = CacheEntry::from_execution_group(group, file_keys, result);
        // do not insert duplicated keys, replace if the limits are the same
        let pos = set.iter().find_position(|e| e.same_limits(&entry));
        if let Some((pos, _)) = pos {
            set[pos] = entry;
        } else {
            set.push(entry);
        }
    }

    /// Search in the cache for a valid entry, returning a cache hit if it's found or a cache miss
    /// if not.
    ///
    /// The result contains the handles to the files in the `FileStore`, preventing the flushing
    /// from erasing them.
    pub fn get(
        &mut self,
        group: &ExecutionGroup,
        file_keys: &HashMap<FileUuid, FileStoreHandle>,
        file_store: &FileStore,
    ) -> CacheResult {
        let key = CacheKey::from_execution_group(group, file_keys);
        if !self.entries.contains_key(&key) {
            return CacheResult::Miss;
        }
        for entry in self.entries[&key].iter() {
            match entry.outputs(file_store, group) {
                None => {
                    // TODO: remove the entry because it's not valid anymore
                }
                Some(outputs) => {
                    if entry.is_compatible(group) {
                        let mut results = Vec::new();
                        for (exec, item) in group.executions.iter().zip(entry.items.iter()) {
                            let (exit_status, signal) = match &item.result.status {
                                ExecutionStatus::ReturnCode(c) => (*c, None),
                                ExecutionStatus::Signal(s, name) => (0, Some((*s, name.clone()))),
                                _ => (0, None),
                            };
                            results.push(ExecutionResult {
                                status: exec.status(exit_status, signal, &item.result.resources),
                                was_killed: item.result.was_killed,
                                was_cached: true,
                                resources: item.result.resources.clone(),
                                stdout: item.result.stdout.clone(),
                                stderr: item.result.stderr.clone(),
                            });
                        }
                        return CacheResult::Hit {
                            result: results,
                            outputs,
                        };
                    }
                }
            }
        }
        CacheResult::Miss
    }

    /// Checks whether a result is allowed in the cache.
    pub fn is_cacheable(result: &ExecutionResult) -> bool {
        !matches!(result.status, ExecutionStatus::InternalError(_))
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        if let Err(e) = storage::store(self) {
            error!("Failed to store the cache: {:?}", e);
        }
    }
}
