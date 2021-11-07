use std::collections::HashMap;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, Error};
use ductile::{ChannelReceiver, ChannelSender};

use task_maker_dag::{ExecutionDAG, FileCallbacks, FileUuid, ProvidedFile, WriteToCallback};
use task_maker_store::*;

use crate::executor::{ExecutionDAGWatchSet, ExecutorStatus, ExecutorWorkerStatus};
use crate::proto::*;

/// Interval between each Status message is sent asking for server status updates.
const STATUS_POLL_INTERVAL_MS: u64 = 1000;

/// This is a client of the `Executor`, the client is who sends a DAG for an evaluation, provides
/// some files and receives the callbacks from the server. When the server notifies a callback
/// function is called by the client.
pub struct ExecutorClient;

impl ExecutorClient {
    /// Begin the evaluation sending the DAG to the server, sending the files as needed and storing
    /// the files from the server.
    ///
    /// This method is blocking until the server ends the computation.
    ///
    /// * `eval` - The EvaluationData to evaluate.
    /// * `sender` - A channel that sends messages to the server.
    /// * `receiver` - A channel that receives messages from the server.
    ///
    /// ```
    /// use task_maker_dag::ExecutionDAG;
    /// use task_maker_store::FileStore;
    /// use task_maker_exec::{executors::LocalExecutor, ExecutorClient, ErrorSandboxRunner};
    /// use std::sync::mpsc::channel;
    /// use std::sync::{Arc, Mutex};
    /// use std::thread;
    /// use std::path::PathBuf;
    /// use task_maker_cache::Cache;
    /// use ductile::new_local_channel;
    /// # use tempdir::TempDir;
    ///
    /// // make a new, empty, DAG
    /// let dag = ExecutionDAG::new();
    /// // setup the communication channels
    /// let (tx, rx_remote) = new_local_channel();
    /// let (tx_remote, rx) = new_local_channel();
    /// # let tmpdir = TempDir::new("tm-test").unwrap();
    /// # let path = tmpdir.path().to_owned();
    /// # let sandbox_runner = ErrorSandboxRunner::default();
    /// let file_store = Arc::new(FileStore::new(&path, 1000, 1000).expect("Cannot create the file store"));
    /// let server_file_store = file_store.clone();
    /// // make a new local executor in a second thread
    /// let server = thread::spawn(move || {
    ///     let cache = Cache::new(&path).expect("Cannot create the cache");
    ///     let mut executor = LocalExecutor::new(server_file_store, 4, path);
    ///     executor.evaluate(tx_remote, rx_remote, cache, sandbox_runner).unwrap();
    /// });
    ///
    /// ExecutorClient::evaluate(dag, tx, &rx, file_store, |_| Ok(())).unwrap(); // this will block!
    ///
    /// server.join().expect("Server paniced");
    /// ```
    #[allow(clippy::cognitive_complexity)]
    pub fn evaluate<F>(
        mut dag: ExecutionDAG,
        sender: ChannelSender<ExecutorClientMessage>,
        receiver: &ChannelReceiver<ExecutorServerMessage>,
        file_store: Arc<FileStore>,
        mut status_callback: F,
    ) -> Result<(), Error>
    where
        F: FnMut(ExecutorStatus<SystemTime>) -> Result<(), Error>,
    {
        trace!("ExecutorClient started");
        ExecutorClient::start_evaluation(&mut dag, &sender)?;

        let provided_files = &dag.data.provided_files;

        // setup the status poller that will send to the server a Status message every
        // STATUS_POLL_INTERVAL_MS milliseconds.
        let done = Arc::new(AtomicBool::new(false));
        let file_mode = Arc::new(Mutex::new(()));
        let status_poller =
            ExecutorClient::spawn_status_poller(done.clone(), file_mode.clone(), sender.clone());

        defer! {{
            info!("Client has done, exiting");
            done.store(true, Ordering::Relaxed);
            status_poller
                .join()
                .map_err(|e| anyhow!("Failed to join status poller: {:?}", e)).unwrap();
        }}

        let mut missing_files = None;
        while missing_files.unwrap_or(1) > 0 {
            match receiver.recv() {
                Ok(ExecutorServerMessage::AskFile(uuid)) => {
                    info!("Server is asking for {}", uuid);
                    // prevent the status poller for sending messages while sending the file
                    let _lock = file_mode
                        .lock()
                        .map_err(|e| anyhow!("Failed to lock: {:?}", e))?;
                    handle_server_ask_file(uuid, provided_files, &sender)?;
                }
                Ok(ExecutorServerMessage::ProvideFile(uuid, success)) => {
                    info!("Server sent the file {}, success: {}", uuid, success);
                    if let Some(missing) = missing_files {
                        missing_files = Some(missing - 1);
                    }
                    let iterator = ChannelFileIterator::new(receiver);
                    process_provided_file(&mut dag.file_callbacks, uuid, success, iterator, None)?;
                }
                Ok(ExecutorServerMessage::NotifyStart(uuid, worker)) => {
                    info!("Execution {} started on {}", uuid, worker);
                    if let Some(callbacks) = dag.execution_callbacks.get_mut(&uuid) {
                        for callback in callbacks.on_start.drain(..) {
                            if let Err(e) = callback.call(worker) {
                                warn!("Start callback for {} failed: {:?}", uuid, e);
                                return Err(e);
                            }
                        }
                    }
                }
                Ok(ExecutorServerMessage::NotifyDone(uuid, result)) => {
                    info!("Execution {} completed with {:?}", uuid, result);
                    if let Some(callbacks) = dag.execution_callbacks.get_mut(&uuid) {
                        for callback in callbacks.on_done.drain(..) {
                            if let Err(e) = callback.call(result.clone()) {
                                warn!("Done callback for {} failed: {:?}", uuid, e);
                                return Err(e);
                            }
                        }
                    }
                }
                Ok(ExecutorServerMessage::NotifySkip(uuid)) => {
                    info!("Execution {} skipped", uuid);
                    if let Some(callbacks) = dag.execution_callbacks.get_mut(&uuid) {
                        for callback in callbacks.on_skip.drain(..) {
                            if let Err(e) = callback.call() {
                                warn!("Skip callback for {} failed: {:?}", uuid, e);
                                return Err(e);
                            }
                        }
                    }
                }
                Ok(ExecutorServerMessage::Error(error)) => {
                    error!("Error occurred: {}", error);
                    sender.send(ExecutorClientMessage::Stop)?;
                    break;
                }
                Ok(ExecutorServerMessage::Status(status)) => {
                    info!("Server status: {:#?}", status);
                    handle_server_status(status, &mut status_callback)?;
                }
                Ok(ExecutorServerMessage::Done(result)) => {
                    info!("Execution completed producing {} files!", result.len());
                    let mut missing = 0;
                    for (uuid, key, success) in result {
                        if let Some(handle) = file_store.get(&key) {
                            let iterator = ReadFileIterator::new(handle.path())?;
                            process_provided_file(
                                &mut dag.file_callbacks,
                                uuid,
                                success,
                                iterator,
                                None,
                            )?;
                        } else {
                            sender.send(ExecutorClientMessage::AskFile(uuid, key, success))?;
                            missing += 1;
                        }
                    }
                    missing_files = Some(missing);
                }
                Err(e) => {
                    let cause = e.root_cause().to_string();
                    if cause == "receiving on an empty and disconnected channel" {
                        trace!("Connection closed: {}", cause);
                    } else {
                        error!("Connection error: {}", cause);
                    }
                    break;
                }
            }
        }
        Ok(())
    }

    /// Start the evaluation calling the file callbacks on the input files and sending the start
    /// message to the Executor.
    fn start_evaluation(
        dag: &mut ExecutionDAG,
        sender: &ChannelSender<ExecutorClientMessage>,
    ) -> Result<(), Error> {
        // list all the files/executions that want callbacks
        let dag_callbacks = ExecutionDAGWatchSet {
            executions: dag.execution_callbacks.keys().cloned().collect(),
            files: dag.file_callbacks.keys().cloned().collect(),
            urgent_files: dag.urgent_files.clone(),
        };
        for (uuid, file) in dag.data.provided_files.iter() {
            match file {
                ProvidedFile::LocalFile { local_path, .. } => {
                    let iterator = ReadFileIterator::new(&local_path)?;
                    process_provided_file(
                        &mut dag.file_callbacks,
                        *uuid,
                        true,
                        iterator,
                        Some(local_path),
                    )?;
                }
                ProvidedFile::Content { content, .. } => {
                    process_provided_file(
                        &mut dag.file_callbacks,
                        *uuid,
                        true,
                        vec![content.clone()],
                        None,
                    )?;
                }
            }
        }
        sender.send(ExecutorClientMessage::Evaluate {
            dag: Box::new(dag.data.clone()),
            callbacks: Box::new(dag_callbacks),
        })
    }

    /// Spawn a thread that will ask the server status every `STATUS_POLL_INTERVAL_MS`, making sure
    /// that the messages are not sent while being in the middle of sending a file.
    fn spawn_status_poller(
        done: Arc<AtomicBool>,
        file_mode: Arc<Mutex<()>>,
        sender: ChannelSender<ExecutorClientMessage>,
    ) -> JoinHandle<()> {
        thread::Builder::new()
            .name("Client status poller".into())
            .spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    {
                        // make sure to not interfere with the file sending protocol.
                        let _lock = file_mode.lock().unwrap();
                        // this may fail if the server is gone
                        if sender.send(ExecutorClientMessage::Status).is_err() {
                            break;
                        }
                    }
                    thread::sleep(Duration::from_millis(STATUS_POLL_INTERVAL_MS));
                }
            })
            .expect("Failed to start client status poller thread")
    }
}

/// Server is asking for a file, handle the request sending the local file or the provided content.
/// Note that this will trigger a protocol change for sending the file, no messages should be sent
/// meanwhile.
fn handle_server_ask_file(
    uuid: FileUuid,
    provided_files: &HashMap<FileUuid, ProvidedFile>,
    sender: &ChannelSender<ExecutorClientMessage>,
) -> Result<(), Error> {
    match &provided_files[&uuid] {
        ProvidedFile::LocalFile {
            local_path, key, ..
        } => {
            sender.send(ExecutorClientMessage::ProvideFile(uuid, key.clone()))?;
            ChannelFileSender::send(&local_path, sender)?;
        }
        ProvidedFile::Content { content, key, .. } => {
            sender.send(ExecutorClientMessage::ProvideFile(uuid, key.clone()))?;
            ChannelFileSender::send_data(content.clone(), sender)?;
        }
    }
    Ok(())
}

/// Handle the server response to the status request.
fn handle_server_status<F>(
    status: ExecutorStatus<Duration>,
    status_callback: &mut F,
) -> Result<(), Error>
where
    F: FnMut(ExecutorStatus<SystemTime>) -> Result<(), Error>,
{
    status_callback(ExecutorStatus {
        connected_workers: status
            .connected_workers
            .into_iter()
            .map(|worker| ExecutorWorkerStatus {
                uuid: worker.uuid,
                name: worker.name,
                current_job: worker.current_job.map(|status| status.into_system_time()),
            })
            .collect(),
        ready_execs: status.ready_execs,
        waiting_execs: status.waiting_execs,
    })
}

/// Process a file provided either by the client or by the server, calling the callback and writing
/// it to the `write_to` path. This will consume the iterator even if the callback is not present.
///
/// If the iterator is reading the same file this function writes to, the result is the file getting
/// truncated, for this reason a best-effort approach is implemented: if the iterator reads a local
/// file pass to this function also the path to the file. The file wont be truncated if write_to
/// points to the same file as the hint.
fn process_provided_file<I: IntoIterator<Item = Vec<u8>>>(
    file_callbacks: &mut HashMap<FileUuid, FileCallbacks>,
    uuid: FileUuid,
    success: bool,
    iterator: I,
    source_path_hint: Option<&Path>,
) -> Result<(), Error> {
    if let Some(callback) = file_callbacks.get_mut(&uuid) {
        let limit = callback
            .get_content
            .as_ref()
            .map(|(limit, _)| *limit)
            .unwrap_or(0);
        let mut buffer: Vec<u8> = Vec::new();
        let mut file = match &callback.write_to {
            Some(WriteToCallback {
                dest,
                allow_failure,
                ..
            }) => {
                if !success && !*allow_failure {
                    None
                } else {
                    let mut skip = false;
                    if let Some(source) = source_path_hint {
                        match (source.canonicalize(), dest.canonicalize()) {
                            (Ok(path), Ok(path2)) if path == path2 => {
                                info!("Not writing {} from itself", path.display());
                                skip = true;
                            }
                            _ => {}
                        }
                    }
                    if skip {
                        None
                    } else {
                        info!("Writing file {} to {}", uuid, dest.display());
                        std::fs::create_dir_all(
                            dest.parent()
                                .ok_or_else(|| anyhow!("Invalid file destination path"))?,
                        )?;
                        Some(std::fs::File::create(dest)?)
                    }
                }
            }
            _ => None,
        };
        for chunk in iterator {
            if let Some(file) = &mut file {
                file.write_all(&chunk)?;
            }
            if buffer.len() < limit {
                let len = std::cmp::min(chunk.len(), limit - buffer.len());
                buffer.extend_from_slice(&chunk[..len]);
            }
        }
        drop(file);
        if let Some(write_to) = &callback.write_to {
            if write_to.executable && write_to.dest.exists() {
                let mut perm = std::fs::metadata(&write_to.dest)?.permissions();
                perm.set_mode(0o755);
                std::fs::set_permissions(&write_to.dest, perm)?;
            }
        }

        if let Some(get_content) = callback.get_content.take().map(|(_, f)| f) {
            get_content.call(buffer)?;
        }
    } else {
        iterator.into_iter().last();
    }
    Ok(())
}
