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
use std::time::Instant;
use std::time::Duration;

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
    where I: Send + 'static
{
    pub fn new(cluster: Arc<Cluster>,
               get_transfers: Box<Fn(I) -> MemoryTransfers + Send>,
               groups: Vec<DpuGroup>,
               input_receiver: Receiver<I>,
               group_receiver: Receiver<DpuGroup>,
               job_sender: Sender<GroupJob>,
               output_sender: Sender<OutputResult>,
               shutdown: Arc<Mutex<bool>>) -> Self {
        InputLoader { cluster, get_transfers, groups, input_receiver, group_receiver, job_sender, output_sender, shutdown }
    }

    pub fn launch(self) -> ThreadHandle {
        Some(thread::spawn(|| self.run()))
    }

    fn run(mut self) {
        let mut iterator = self.input_receiver.iter();
        let mut start = None;
        let mut wait_input: Option<Instant> = None;
        let mut wait_input_time = Duration::new(0, 0);
        let mut wait_group_time = Duration::new(0, 0);

        while let Some(item) = iterator.next() {
            start.get_or_insert_with(|| Instant::now());

            match wait_input {
                None => (),
                Some(start_instant) => {
                    wait_input_time = wait_input_time + start_instant.elapsed();
                    wait_input = None;
                },
            }

            let wait_group = Instant::now();
            let group = fetch_next_group(&mut self.groups, &mut self.group_receiver);
            wait_group_time = wait_group_time + wait_group.elapsed();

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

            wait_input.get_or_insert_with(|| Instant::now());
        }

        match start {
            None => println!("LOADER: No input"),
            Some(start_instant) =>  {
                println!("LOADER: Duration: {:?}", start_instant.elapsed());
                println!("LOADER: WAIT INPUT: {:?}", wait_input_time);
                println!("LOADER: WAIT GROUP: {:?}", wait_group_time);
            },
        }
    }

    fn load_input_chunk(&self, group: DpuGroup, chunk: Vec<Vec<InputMemoryTransfer>>, outs: Vec<OutputMemoryTransfer>) {
        match chunk.iter().max_by_key(|t| t.len()).map(|t| t.len()) {
            None => (),
            Some(max_len) =>
                match self.do_memory_transfers(&group, chunk, max_len) {
                    Err(err) => {
                        self.output_sender.send(Err(PipelineError::InfrastructureError(err))).unwrap()
                    },
                    Ok(_) => {
                        for dpu in &group.dpus {
                            match self.cluster.driver().boot(&View::one(dpu.clone())) {
                                Ok(_) => (),
                                Err(err) => {
                                    self.output_sender.send(Err(PipelineError::InfrastructureError(err))).unwrap()
                                }
                            }
                        }

                        self.job_sender.send((group, outs)).unwrap();
                    },
                },
        }
    }

    fn do_memory_transfers(&self, group: &DpuGroup, mut chunk: Vec<Vec<InputMemoryTransfer>>, max_len: usize) -> Result<(), ClusterError> {
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
            self.cluster.driver().copy_to_memory(memory_transfer)?;
        }

        Ok(())
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