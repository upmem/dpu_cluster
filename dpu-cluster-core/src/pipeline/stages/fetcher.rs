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
use std::time::Instant;
use std::time::Duration;

pub struct OutputFetcher {
    cluster: Arc<Cluster>,
    finish_receiver: Receiver<GroupJob>,
    output_sender: Sender<OutputResult>,
    group_sender: Sender<DpuGroup>,
    shutdown: Arc<Mutex<bool>>
}

impl OutputFetcher {
    pub fn new(cluster: Arc<Cluster>,
               finish_receiver: Receiver<GroupJob>,
               output_sender: Sender<OutputResult>,
               group_sender: Sender<DpuGroup>,
               shutdown: Arc<Mutex<bool>>) -> Self {
        OutputFetcher { cluster, finish_receiver, output_sender, group_sender, shutdown }
    }

    pub fn launch(self) -> ThreadHandle {
        Some(thread::spawn(|| self.run()))
    }

    fn run(self) {
        let mut start = None;
        let mut wait: Option<Instant> = None;
        let mut wait_time = Duration::new(0, 0);

        for (group, transfers) in self.finish_receiver {
            start.get_or_insert_with(|| Instant::now());

            match wait {
                None => (),
                Some(start_instant) => {
                    wait_time = wait_time + start_instant.elapsed();
                    wait = None;
                },
            }

            let mut vectors = transfers.iter().map(|transfer| {
                let mut v = Vec::with_capacity(transfer.length as usize);
                v.resize(transfer.length as usize, 0u8);
                (v, transfer.offset)
            }).collect::<Vec<_>>();

            let copy_result = {
                let mut memory_transfer = MemoryTransfer::default();
                for ((vector, offset), dpu) in vectors.iter_mut().zip(&group.dpus) {
                    memory_transfer.add_in_place(dpu.clone(), *offset, vector.as_mut_slice());
                }
                self.cluster.driver().copy_from_memory(&mut memory_transfer)
            };

            match copy_result {
                Ok(_) => {
                    for (result, _) in vectors {
                        self.output_sender.send(Ok(result)).unwrap();
                    }
                },
                Err(err) => {
                    self.output_sender.send(Err(PipelineError::InfrastructureError(err))).unwrap();
                },
            };

            self.group_sender.send(group);

            wait.get_or_insert_with(|| Instant::now());
        }

        match start {
            None => println!("FETCHER: No input"),
            Some(start_instant) => {
                println!("FETCHER: Duration: {:?}", start_instant.elapsed());
                println!("FETCHER: WAIT: {:?}", wait_time)
            },
        }
    }
}