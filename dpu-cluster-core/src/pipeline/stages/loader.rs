use pipeline::ThreadHandle;
use std::thread;
use driver::Driver;
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

pub struct InputLoader<I> {
    cluster: Arc<Cluster>,
    get_transfers: Box<Fn(I) -> MemoryTransfers + Send>,
    groups: Vec<DpuGroup>,
    input_receiver: Receiver<I>,
    group_receiver: Receiver<DpuGroup>,
    job_sender: Sender<GroupJob>,
    output_sender: Sender<OutputResult>,
    shutdown: Arc<Mutex<bool>>
}

impl <I> InputLoader<I>
    where I: Send
{
    pub fn launch(self) -> ThreadHandle {
        Some(thread::spawn(|| self.run()))
    }

    fn run(mut self) {
        let mut iterator = self.input_receiver.iter();

        while let Some(item) = iterator.next() {
            let group = fetch_next_group(&mut self.groups, &mut self.group_receiver);
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

            self.load_input_chunk(group, inputs, outputs);
        }
    }

    fn load_input_chunk(&self, group: DpuGroup, chunk: Vec<Vec<InputMemoryTransfer>>, outs: Vec<OutputMemoryTransfer>) {



        for dpu in &group.dpus {
            match self.cluster.driver().boot(&View::one(dpu.clone())) {
                Ok(_) => (),
                Err(err) => {
                    self.output_sender.send(Err(PipelineError::InfrastructureError(err))).unwrap()
                }
            }
        }

        self.job_sender.send((group, outs)).unwrap();
        unimplemented!()
    }
}

fn fetch_next_group(groups: &mut Vec<DpuGroup>, group_receiver: &Receiver<DpuGroup>) -> DpuGroup {
    match groups.pop() {
        Some(grp) => grp,
        None => {
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