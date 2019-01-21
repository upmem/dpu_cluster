use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use pipeline::transfer::OutputMemoryTransfer;
use pipeline::PipelineError;
use std::sync::Arc;
use std::sync::Mutex;
use pipeline::transfer::InputMemoryTransfer;
use view::View;
use pipeline::stages::DpuGroup;
use pipeline::OutputResult;
use pipeline::stages::GroupJob;
use cluster::Cluster;
use memory::MemoryTransfer;
use error::ClusterError;
use pipeline::monitoring::EventMonitor;
use pipeline::monitoring::Process;
use pipeline::monitoring::Event;
use driver::Driver;
use std::sync::mpsc::SyncSender;
use pipeline::stages::Stage;

pub struct InputLoader<InputHandle> {
    cluster: Arc<Cluster>,
    transfer_receiver: Receiver<(DpuGroup, Vec<Vec<InputMemoryTransfer>>, Vec<(InputHandle, OutputMemoryTransfer)>)>,
    job_sender: Sender<GroupJob<InputHandle>>,
    output_sender: SyncSender<OutputResult<InputHandle>>,
    monitoring: EventMonitor,
    // todo: use or remove
    shutdown: Arc<Mutex<bool>>
}

impl <InputHandle> InputLoader<InputHandle>
    where InputHandle: Send + 'static
{
    pub fn new(cluster: Arc<Cluster>,
               transfer_receiver: Receiver<(DpuGroup, Vec<Vec<InputMemoryTransfer>>, Vec<(InputHandle, OutputMemoryTransfer)>)>,
               job_sender: Sender<GroupJob<InputHandle>>,
               output_sender: SyncSender<OutputResult<InputHandle>>,
               mut monitoring: EventMonitor,
               shutdown: Arc<Mutex<bool>>) -> Self {
        monitoring.set_process(Process::Loader);

        InputLoader { cluster, transfer_receiver, job_sender, output_sender, monitoring, shutdown }
    }
}

impl <InputHandle> Stage for InputLoader<InputHandle>
    where InputHandle: Send + 'static
{
    fn run(self) {
        let monitoring = self.monitoring;

        monitoring.record(Event::ProcessBegin);

        let driver = self.cluster.driver();

        for (group, inputs, outputs) in self.transfer_receiver {
            let group_id = group.id;

            monitoring.record(Event::GroupLoadingBegin(group_id));

            let is_ok = load_input_chunk(driver, &group, inputs, &self.output_sender);

            monitoring.record(Event::GroupLoadingEnd(group_id));

            if is_ok {
                self.job_sender.send((group, outputs)).unwrap();
            }
        }
    }
}

fn load_input_chunk<T>(driver: &Driver, group: &DpuGroup, chunk: Vec<Vec<InputMemoryTransfer>>,
                       output_sender: &SyncSender<OutputResult<T>>) -> bool {
    match chunk.iter().max_by_key(|t| t.len()).map(|t| t.len()) {
        // the None case (empty group) never happens
        None => true,
        Some(max_len) =>
            match do_memory_transfers(driver, group, chunk, max_len) {
                Err(err) => {
                    output_sender.send(Err(PipelineError::InfrastructureError(err))).unwrap();
                    false
                },
                Ok(_) => {
                    let mut is_ok = true;

                    for dpu in &group.dpus {
                        match driver.boot(&View::one(dpu.clone())) {
                            Ok(_) => (),
                            Err(err) => {
                                output_sender.send(Err(PipelineError::InfrastructureError(err))).unwrap();
                                is_ok = false;
                                break;
                            }
                        }
                    }

                    is_ok
                },
            },
    }
}

fn do_memory_transfers(driver: &Driver, group: &DpuGroup, mut chunk: Vec<Vec<InputMemoryTransfer>>, max_len: usize) -> Result<(), ClusterError> {
    let mut memory_transfers = Vec::with_capacity(max_len);

    for _ in 0..max_len {
        memory_transfers.push(MemoryTransfer::default());
    }

    for (idx, transfers) in chunk.iter_mut().enumerate() {
        for (i, transfer) in transfers.iter_mut().enumerate() {
            let memory_transfer = memory_transfers.get_mut(i).unwrap();

            memory_transfer.add_in_place(group.dpus.get(idx).unwrap().clone(), transfer.offset, transfer.content.as_mut_slice());
        }
    }

    for memory_transfer in memory_transfers.iter_mut() {
        driver.copy_to_memory(memory_transfer)?;
    }

    Ok(())
}