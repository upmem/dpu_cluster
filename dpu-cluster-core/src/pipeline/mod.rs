pub mod data;
pub mod plan;
pub mod transfer;
pub mod output;

mod stages;

use std::thread::JoinHandle;
use std::sync::Mutex;
use std::sync::Arc;
use std::sync::mpsc::Receiver;

use error::ClusterError;
use dpu::DpuId;
use pipeline::stages::initializer::InputInitializer;
use pipeline::stages::loader::InputLoader;
use pipeline::stages::tracker::ExecutionTracker;
use pipeline::stages::fetcher::OutputFetcher;
use std::sync::mpsc::channel;
use driver::Driver;
use cluster::Cluster;
use view::View;
use pipeline::transfer::MemoryTransfers;

pub enum PipelineError {
    InfrastructureError(ClusterError),
    ExecutionError(DpuId)
}

impl From<ClusterError> for PipelineError {
    fn from(err: ClusterError) -> Self {
        PipelineError::InfrastructureError(err)
    }
}

type OutputResult = Result<Vec<u8>, PipelineError>;
type ThreadHandle = Option<JoinHandle<()>>;

struct Pipeline {
    output_receiver: Receiver<OutputResult>,

    input_initializer: ThreadHandle,
    input_loader: ThreadHandle,
    execution_tracker: ThreadHandle,
    output_fetcher: ThreadHandle,
    shutdown: Arc<Mutex<bool>>
}

impl Pipeline {
    fn new<I, F>(iterator: Box<dyn Iterator<Item = I>>, cluster: Arc<Cluster>, view: View, transfers_fn: Box<F>) -> Self
        where I: Send,
              Iterator<Item=I>: Send,
              F: Fn(I) -> MemoryTransfers + Send
    {
        let shutdown = Arc::new(Mutex::new(false));

        let (input_tx, input_rx) = channel();
        let (output_tx, output_rx) = channel();
        let (group_tx, group_rx) = channel();
        let (incoming_job_tx, incoming_job_rx) = channel();
        let (finished_job_tx, finished_job_rx) = channel();

        let groups = unimplemented!();

        let input_initializer = InputInitializer {
            iterator, sender: input_tx, shutdown: shutdown.clone()
        }.launch();

        let input_loader = InputLoader {
            cluster: cluster.clone(), get_transfers: transfers_fn, groups, input_receiver: input_rx,
            group_receiver: group_rx, job_sender: incoming_job_tx, output_sender: output_tx.clone(),
            shutdown: shutdown.clone()
        }.launch();

        let execution_tracker = ExecutionTracker {
            cluster: cluster.clone(), job_receiver: incoming_job_rx, finish_sender: finished_job_tx,
            output_sender: output_tx.clone(), shutdown: shutdown.clone()
        }.launch();

        let output_fetcher = OutputFetcher {
            cluster: cluster.clone(), finish_receiver: finished_job_rx, output_sender: output_tx.clone(),
            group_sender: group_tx, shutdown: shutdown.clone()
        }.launch();

        Pipeline {
            output_receiver: output_rx,
            input_initializer,
            input_loader,
            execution_tracker,
            output_fetcher,
            shutdown
        }
    }
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        {
            *self.shutdown.lock().unwrap() = true;
        }

        self.input_initializer.take().unwrap().join();
        self.input_loader.take().unwrap().join();
        self.execution_tracker.take().unwrap().join();
        self.output_fetcher.take().unwrap().join();
    }
}