use std::sync::mpsc::Receiver;
use pipeline::OutputResult;
use pipeline::ThreadHandle;
use std::sync::Mutex;
use std::sync::Arc;
use cluster::Cluster;
use pipeline::transfer::MemoryTransfers;
use std::sync::mpsc::channel;
use pipeline::stages::initializer::InputInitializer;
use pipeline::stages::loader::InputLoader;
use pipeline::stages::tracker::ExecutionTracker;
use pipeline::stages::fetcher::OutputFetcher;
use dpu::DpuId;
use pipeline::stages::DpuGroup;
use pipeline::monitoring::EventMonitor;

pub struct Pipeline {
    pub output_receiver: Receiver<OutputResult>,

    input_initializer: ThreadHandle,
    input_loader: ThreadHandle,
    execution_tracker: ThreadHandle,
    output_fetcher: ThreadHandle,
    shutdown: Arc<Mutex<bool>>
}

impl Pipeline {
    pub fn new<I, F, IT>(iterator: Box<IT>, cluster: Arc<Cluster>, transfers_fn: Box<F>, monitoring: EventMonitor) -> Self
        where I: Send + 'static,
              IT: Iterator<Item=I> + Send + 'static,
              F: Fn(I) -> MemoryTransfers + Send + 'static
    {
        let shutdown = Arc::new(Mutex::new(false));

        // todo: should we offer the possibility of sync_channel (buffered sender) ?
        let (input_tx, input_rx) = channel();
        let (output_tx, output_rx) = channel();
        let (group_tx, group_rx) = channel();
        let (incoming_job_tx, incoming_job_rx) = channel();
        let (finished_job_tx, finished_job_rx) = channel();

        let (nr_ranks, nr_slices, nr_dpus) = cluster.topology();

        let groups = {
            let mut vec = Vec::with_capacity((nr_ranks as usize) * (nr_dpus  as usize));

            for rank_idx in 0..nr_ranks {
                for dpu_idx in 0..nr_dpus {
                    let mut dpus = Vec::with_capacity(nr_slices as usize);

                    for slice_idx in 0..nr_slices {
                        dpus.push(DpuId::new(rank_idx, slice_idx, dpu_idx));
                    }

                    vec.push(DpuGroup { id: ((rank_idx as u32) * (nr_dpus as u32)) + (dpu_idx as u32), dpus } );
                }
            }

            vec
        };

        let input_initializer = InputInitializer::new(
            iterator, input_tx, monitoring.clone(), shutdown.clone()
        ).launch();

        let input_loader = InputLoader::new(
            cluster.clone(), transfers_fn, groups, input_rx,
            group_rx, incoming_job_tx, output_tx.clone(),
            monitoring.clone(), shutdown.clone()
        ).launch();

        let execution_tracker = ExecutionTracker::new(
            cluster.clone(), incoming_job_rx, finished_job_tx,
            output_tx.clone(), monitoring.clone(), shutdown.clone()
        ).launch();

        let output_fetcher = OutputFetcher::new(
            cluster.clone(), finished_job_rx, output_tx.clone(),
            group_tx, monitoring.clone(), shutdown.clone()
        ).launch();

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
    #[allow(unused_must_use)]
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