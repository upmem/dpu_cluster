use pipeline::OutputResult;
use pipeline::stages::DpuGroup;
use std::sync::mpsc::Sender;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::sync::Mutex;
use memory::MemoryTransfer;
use pipeline::PipelineError;
use pipeline::stages::GroupJob;
use cluster::Cluster;
use pipeline::monitoring::EventMonitor;
use pipeline::monitoring::Process;
use pipeline::monitoring::Event;
use std::sync::mpsc::SyncSender;
use pipeline::stages::Stage;

pub struct OutputFetcher<InputHandle> {
    cluster: Arc<Cluster>,
    finish_receiver: Receiver<GroupJob<InputHandle>>,
    output_sender: SyncSender<OutputResult<InputHandle>>,
    group_sender: Sender<DpuGroup>,
    monitoring: EventMonitor,
    // todo: use or remove
    shutdown: Arc<Mutex<bool>>
}

impl <InputHandle> OutputFetcher<InputHandle>
    where InputHandle: Send + 'static
{
    pub fn new(cluster: Arc<Cluster>,
               finish_receiver: Receiver<GroupJob<InputHandle>>,
               output_sender: SyncSender<OutputResult<InputHandle>>,
               group_sender: Sender<DpuGroup>,
               mut monitoring: EventMonitor,
               shutdown: Arc<Mutex<bool>>) -> Self {
        monitoring.set_process(Process::Fetcher);

        OutputFetcher { cluster, finish_receiver, output_sender, group_sender, monitoring, shutdown }
    }
}

impl <InputHandle> Stage for OutputFetcher<InputHandle>
    where InputHandle: Send + 'static
{
    fn run(self) {
        let monitoring = self.monitoring;

        monitoring.record(Event::ProcessBegin);

        for (group, transfers) in self.finish_receiver {
            let group_id = group.id;
            monitoring.record(Event::OutputFetchingBegin(group_id));

            let mut vectors = Vec::with_capacity(transfers.len());

            for (handle, transfer) in transfers {
                vectors.push((vec![0u8; transfer.length as usize], transfer.offset, handle));
            }

            let copy_result = {
                let mut memory_transfer = MemoryTransfer::default();
                for ((vector, offset, _), dpu) in vectors.iter_mut().zip(&group.dpus) {
                    monitoring.record(Event::OutputFetchingInfo { dpu: dpu.clone(), offset: *offset, length: vector.len() as u32});
                    memory_transfer.add_in_place(dpu.clone(), *offset, vector.as_mut_slice());
                }
                self.cluster.driver().copy_from_memory(&mut memory_transfer)
            };

            monitoring.record(Event::OutputFetchingEnd(group_id));

            match copy_result {
                Ok(_) => {
                    self.group_sender.send(group);
                    for (result, _, handle) in vectors {
                        self.output_sender.send(Ok((handle, result))).unwrap();
                    }
                },
                Err(err) => {
                    self.output_sender.send(Err(PipelineError::InfrastructureError(err))).unwrap();
                },
            };
        }

        monitoring.record(Event::ProcessEnd);
    }
}