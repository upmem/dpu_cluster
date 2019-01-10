use pipeline::OutputResult;
use pipeline::stages::DpuGroup;
use std::sync::mpsc::Sender;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::sync::Mutex;
use memory::MemoryTransfer;
use pipeline::PipelineError;
use pipeline::ThreadHandle;
use std::thread;
use pipeline::stages::GroupJob;
use cluster::Cluster;
use pipeline::monitoring::EventMonitor;
use pipeline::monitoring::Process;
use pipeline::monitoring::Event;

pub struct OutputFetcher<M: EventMonitor + Send + 'static> {
    cluster: Arc<Cluster>,
    finish_receiver: Receiver<GroupJob>,
    output_sender: Sender<OutputResult>,
    group_sender: Sender<DpuGroup>,
    monitoring: M,
    shutdown: Arc<Mutex<bool>>
}

impl <M: EventMonitor + Send + 'static> OutputFetcher<M> {
    pub fn new(cluster: Arc<Cluster>,
               finish_receiver: Receiver<GroupJob>,
               output_sender: Sender<OutputResult>,
               group_sender: Sender<DpuGroup>,
               mut monitoring: M,
               shutdown: Arc<Mutex<bool>>) -> Self {
        monitoring.set_process(Process::Fetcher);

        OutputFetcher { cluster, finish_receiver, output_sender, group_sender, monitoring, shutdown }
    }

    pub fn launch(self) -> ThreadHandle {
        Some(thread::spawn(|| self.run()))
    }

    fn run(self) {
        let mut monitoring = self.monitoring;

        monitoring.record(Event::ProcessBegin);

        for (group, transfers) in self.finish_receiver {
            let group_id = group.id;
            monitoring.record(Event::OutputFetchingBegin(group_id));

            let mut vectors = transfers.iter()
                .map(|t| (vec![0u8; t.length as usize], t.offset))
                .collect::<Vec<_>>();

            let copy_result = {
                let mut memory_transfer = MemoryTransfer::default();
                for ((vector, offset), dpu) in vectors.iter_mut().zip(&group.dpus) {
                    monitoring.record(Event::OutputFetchingInfo { dpu: dpu.clone(), offset: *offset, length: vector.len() as u32});
                    memory_transfer.add_in_place(dpu.clone(), *offset, vector.as_mut_slice());
                }
                self.cluster.driver().copy_from_memory(&mut memory_transfer)
            };

            monitoring.record(Event::OutputFetchingEnd(group_id));

            match copy_result {
                Ok(_) => {
                    for (result, _) in vectors {
                        self.output_sender.send(Ok(result)).unwrap();
                    }
                    self.group_sender.send(group);
                },
                Err(err) => {
                    self.output_sender.send(Err(PipelineError::InfrastructureError(err))).unwrap();
                },
            };
        }

        monitoring.record(Event::ProcessEnd);
    }
}