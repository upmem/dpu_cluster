use pipeline::ThreadHandle;
use std::thread;
use pipeline::transfer::MemoryTransfers;
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

pub struct InputLoader<I, M: EventMonitor + Send + 'static> {
    cluster: Arc<Cluster>,
    get_transfers: Box<Fn(I) -> MemoryTransfers + Send>,
    groups: Vec<DpuGroup>,
    input_receiver: Receiver<I>,
    group_receiver: Receiver<DpuGroup>,
    job_sender: Sender<GroupJob>,
    output_sender: Sender<OutputResult>,
    monitoring: M,
    shutdown: Arc<Mutex<bool>>
}

impl <I, M> InputLoader<I, M>
    where I: Send + 'static,
          M: EventMonitor + Send + 'static
{
    pub fn new(cluster: Arc<Cluster>,
               get_transfers: Box<Fn(I) -> MemoryTransfers + Send>,
               groups: Vec<DpuGroup>,
               input_receiver: Receiver<I>,
               group_receiver: Receiver<DpuGroup>,
               job_sender: Sender<GroupJob>,
               output_sender: Sender<OutputResult>,
               mut monitoring: M,
               shutdown: Arc<Mutex<bool>>) -> Self {
        monitoring.set_process(Process::Loader);

        InputLoader { cluster, get_transfers, groups, input_receiver, group_receiver, job_sender, output_sender, monitoring, shutdown }
    }

    pub fn launch(self) -> ThreadHandle {
        Some(thread::spawn(|| self.run()))
    }

    fn run(mut self) {
        let mut monitoring = self.monitoring;

        monitoring.record(Event::ProcessBegin);

        let mut iterator = self.input_receiver.iter();

        while let Some(item) = iterator.next() {
            monitoring.record(Event::GroupSearchBegin);
            let group = fetch_next_group(&mut self.groups, &mut self.group_receiver);
            let group_id = group.id;
            monitoring.record(Event::GroupSearchEnd(group_id));

            let group_size = group.dpus.len();
            let mut inputs = Vec::with_capacity(group_size);
            let mut outputs = Vec::with_capacity(group_size);

            let transfers = (self.get_transfers)(item);
            inputs.push(transfers.inputs);
            outputs.push(transfers.output);

            while inputs.len() != group_size {
                match iterator.next() {
                    None => break,
                    Some(item) => {
                        let transfers = (self.get_transfers)(item);
                        inputs.push(transfers.inputs);
                        outputs.push(transfers.output);
                    }
                }
            }

            monitoring.record(Event::GroupLoadingBegin(group_id));
            load_input_chunk(self.cluster.driver(), group, inputs, outputs, &self.job_sender, &self.output_sender);
            monitoring.record(Event::GroupLoadingEnd(group_id));
        }

        monitoring.record(Event::ProcessEnd);
    }
}

fn fetch_next_group(groups: &mut Vec<DpuGroup>, group_receiver: &Receiver<DpuGroup>) -> DpuGroup {
    match groups.pop() {
        Some(grp) => grp,
        None => {
            // todo: fix the issue where no group may be sent because all have failed
            let grp = group_receiver.recv().unwrap();

            loop {
                match group_receiver.try_recv() {
                    Ok(other_group) => groups.push(other_group),
                    Err(_) => break,
                }
            }

            grp
        },
    }
}

fn load_input_chunk(driver: &Driver, group: DpuGroup, chunk: Vec<Vec<InputMemoryTransfer>>,
                    outs: Vec<OutputMemoryTransfer>, job_sender: &Sender<GroupJob>, output_sender: &Sender<OutputResult>) {
    match chunk.iter().max_by_key(|t| t.len()).map(|t| t.len()) {
        None => (),
        Some(max_len) =>
            match do_memory_transfers(driver, &group, chunk, max_len) {
                Err(err) => {
                    output_sender.send(Err(PipelineError::InfrastructureError(err))).unwrap()
                },
                Ok(_) => {
                    let mut fault = false;
                    for dpu in &group.dpus {
                        match driver.boot(&View::one(dpu.clone())) {
                            Ok(_) => (),
                            Err(err) => {
                                output_sender.send(Err(PipelineError::InfrastructureError(err))).unwrap();
                                fault = true;
                                break;
                            }
                        }
                    }

                    if !fault {
                        job_sender.send((group, outs)).unwrap();
                    }
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