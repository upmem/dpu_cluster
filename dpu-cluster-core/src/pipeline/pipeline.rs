use std::sync::mpsc::Receiver;
use crate::pipeline::OutputResult;
use crate::pipeline::ThreadHandle;
use std::sync::Mutex;
use std::sync::Arc;
use crate::cluster::Cluster;
use crate::pipeline::transfer::MemoryTransfers;
use std::sync::mpsc::channel;
use std::sync::mpsc::sync_channel;
use crate::pipeline::stages::initializer::InputInitializer;
use crate::pipeline::stages::loader::InputLoader;
use crate::pipeline::stages::tracker::ExecutionTracker;
use crate::pipeline::stages::fetcher::OutputFetcher;
use crate::dpu::DpuId;
use crate::pipeline::stages::DpuGroup;
use crate::pipeline::monitoring::EventMonitor;
use std::time::Duration;
use dpu_sys::DpuType;
use crate::pipeline::stages::Stage;
use crate::pipeline::stages::mapper::SimpleMapper;
use crate::pipeline::GroupPolicy;
use crate::pipeline::stages::mapper::PersistentMapper;
use std::hash::Hash;
use crate::pipeline::transfer::InputMemoryTransfer;

pub struct Pipeline<K> {
    pub output_receiver: Receiver<OutputResult<K>>,

    input_initializer: ThreadHandle,
    input_mapper: ThreadHandle,
    input_loader: ThreadHandle,
    execution_tracker: ThreadHandle,
    output_fetcher: ThreadHandle,
    shutdown: Arc<Mutex<bool>>
}

impl <K: Send + 'static> Pipeline<K> {
    // todo factorize simple and persistent?
    pub fn simple<I, F, IT>(iterator: Box<IT>, cluster: Arc<Cluster>, transfers_fn: Box<F>,
                            group_policy: GroupPolicy, monitoring: EventMonitor) -> Self
        where I: Send + 'static,
              IT: Iterator<Item=I> + Send + 'static,
              F: Fn(I) -> MemoryTransfers<K> + Send + 'static
    {
        let shutdown = Arc::new(Mutex::new(false));

        let (nr_ranks, nr_slices, nr_dpus) = cluster.topology();

        // todo: input sync channel bound should be a config parameter
        let (input_tx, input_rx) = sync_channel(2 * (nr_slices as usize));
        // todo: output sync channel bound should be a config parameter
        let (output_tx, output_rx) = sync_channel(2 * (nr_slices as usize));
        let (transfer_tx, transfer_rx) = channel();
        let (group_tx, group_rx) = channel();
        let (incoming_job_tx, incoming_job_rx) = channel();
        let (finished_job_tx, finished_job_rx) = channel();

        let groups = create_groups_from(group_policy, nr_ranks, nr_slices, nr_dpus);

        // todo: tracker_sleep_duration should be a config parameter
        let tracker_sleep_duration = match cluster.target().dpu_type {
            DpuType::Hardware => Some(Duration::from_millis(10)),
            DpuType::BackupSpi => Some(Duration::from_millis(10)),
            _ => None,
        };

        let input_initializer = InputInitializer::new(
            iterator, input_tx, monitoring.clone(), shutdown.clone()
        ).launch();

        let input_mapper = SimpleMapper::new(
            transfers_fn, groups, input_rx, group_rx,
            transfer_tx, monitoring.clone(), shutdown.clone()
        ).launch();

        let input_loader = InputLoader::new(
            cluster.clone(), transfer_rx, incoming_job_tx,
            output_tx.clone(), monitoring.clone(), shutdown.clone()
        ).launch();

        let execution_tracker = ExecutionTracker::new(
            cluster.clone(), incoming_job_rx, finished_job_tx,
            output_tx.clone(), tracker_sleep_duration, monitoring.clone(),
            shutdown.clone()
        ).launch();

        let output_fetcher = OutputFetcher::new(
            cluster.clone(), finished_job_rx, output_tx.clone(),
            group_tx, monitoring.clone(), shutdown.clone()
        ).launch();

        Pipeline {
            output_receiver: output_rx,
            input_initializer,
            input_mapper,
            input_loader,
            execution_tracker,
            output_fetcher,
            shutdown
        }
    }

    pub fn persistent<I, F, IT, D, DIT>(iterator: Box<IT>, cluster: Arc<Cluster>, transfers_fn: Box<F>,
                                           mapping_iterator: Box<DIT>,
                                           group_policy: GroupPolicy, monitoring: EventMonitor) -> Self
        where I: Send + 'static,
              IT: Iterator<Item=I> + Send + 'static,
              F: Fn(I) -> (D, MemoryTransfers<K>) + Send + 'static,
              D: Eq + Hash + Send + 'static,
              DIT: Iterator<Item=(D, InputMemoryTransfer)> + Send + 'static
    {
        let shutdown = Arc::new(Mutex::new(false));

        let (nr_ranks, nr_slices, nr_dpus) = cluster.topology();

        // todo: input sync channel bound should be a config parameter
        let (input_tx, input_rx) = sync_channel(2 * (nr_slices as usize));
        // todo: output sync channel bound should be a config parameter
        let (output_tx, output_rx) = sync_channel(2 * (nr_slices as usize));
        let (transfer_tx, transfer_rx) = channel();
        let (group_tx, group_rx) = channel();
        let (incoming_job_tx, incoming_job_rx) = channel();
        let (finished_job_tx, finished_job_rx) = channel();

        let groups = create_groups_from(group_policy, nr_ranks, nr_slices, nr_dpus);

        // todo: tracker_sleep_duration should be a config parameter
        let tracker_sleep_duration = match cluster.target().dpu_type {
            DpuType::Hardware => Some(Duration::from_millis(10)),
            DpuType::BackupSpi => Some(Duration::from_millis(10)),
            _ => None,
        };

        let input_initializer = InputInitializer::new(
            iterator, input_tx, monitoring.clone(), shutdown.clone()
        ).launch();

        let input_mapper = PersistentMapper::new(
            transfers_fn, groups, input_rx, group_rx,
            cluster.clone(), transfer_tx, output_tx.clone(),
            mapping_iterator, monitoring.clone(), shutdown.clone()
        ).launch();

        let input_loader = InputLoader::new(
            cluster.clone(), transfer_rx, incoming_job_tx,
            output_tx.clone(), monitoring.clone(), shutdown.clone()
        ).launch();

        let execution_tracker = ExecutionTracker::new(
            cluster.clone(), incoming_job_rx, finished_job_tx,
            output_tx.clone(), tracker_sleep_duration, monitoring.clone(),
            shutdown.clone()
        ).launch();

        let output_fetcher = OutputFetcher::new(
            cluster.clone(), finished_job_rx, output_tx.clone(),
            group_tx, monitoring.clone(), shutdown.clone()
        ).launch();

        Pipeline {
            output_receiver: output_rx,
            input_initializer,
            input_mapper,
            input_loader,
            execution_tracker,
            output_fetcher,
            shutdown
        }
    }
}

fn create_groups_from(policy: GroupPolicy, nr_ranks: u8, nr_slices: u8, nr_dpus: u8) -> Vec<DpuGroup> {
    match policy {
        GroupPolicy::Dpu => {
            let mut vec = Vec::with_capacity((nr_ranks as usize) * (nr_slices as usize) * (nr_dpus as usize));

            for rank_idx in 0..nr_ranks {
                for slice_idx in 0..nr_slices {
                    for dpu_idx in 0..nr_dpus {
                        let id = ((rank_idx as u32) * (nr_slices as u32) *  (nr_dpus as u32)) + ((slice_idx as u32) * (nr_dpus as u32)) + (dpu_idx as u32);
                        let dpus = vec![DpuId::new(rank_idx, slice_idx, dpu_idx)];
                        vec.push(DpuGroup { id, dpus });
                    }
                }
            }

            vec
        },
        GroupPolicy::Slice => {
            let mut vec = Vec::with_capacity((nr_ranks as usize) * (nr_dpus as usize));

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
        }
    }
}

impl <K> Drop for Pipeline<K> {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        {
            *self.shutdown.lock().unwrap() = true;
        }

        self.input_initializer.take().unwrap().join();
        self.input_mapper.take().unwrap().join();
        self.input_loader.take().unwrap().join();
        self.execution_tracker.take().unwrap().join();
        self.output_fetcher.take().unwrap().join();
    }
}