use view::View;
use dpu::DpuId;
use memory::MemoryTransfer;
use program::Program;
use std::collections::HashMap;
use memory::MemoryTransferEntry;
use driver::Driver;
use error::ClusterError;
use std::thread;
use std::thread::JoinHandle;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::mpsc::TryRecvError;
use std::sync::Arc;
use std::sync::Mutex;
use driver::RunStatus;
use driver::Mergeable;

pub struct DataPlan<'a, T, F> {
    base_iterator: Box<Iterator<Item = T>>,
    program: Option<&'a Program>,
    grouped_by: Option<&'a View>,
    transfers_fn: F,
}

pub struct MemoryTransfers {
    inputs: Vec<InputMemoryTransfer>,
    output: OutputMemoryTransfer
}

pub struct InputMemoryTransfer {
    offset: u32,
    content: Vec<u8>
}

pub struct OutputMemoryTransfer {
    offset: u32,
    length: u32,
}

impl <'a, T, F> DataPlan<'a, T, F> {
    pub fn grouped(mut self, view: &'a View) -> Self {
        self.grouped_by = Some(view);
        self
    }

    pub fn running(mut self, program: &'a Program) -> Self {
        self.program = Some(program);
        self
    }

    pub fn transfers(mut self, func: F) -> Self
        where F: Fn(T) -> MemoryTransfers
    {
        self.transfers_fn = func;
        self
    }

    pub fn execute<K>(self, driver: &Driver) -> Result<ExecutionIterator<Vec<u8>>, ClusterError> {
        let all = View::all();
        let view = self.grouped_by.unwrap_or_else(|| &all);
        if let Some(program) = self.program {
            driver.load(view, program)?
        }

        unimplemented!()
    }
}

struct DpuGroup {
    dpus: Vec<DpuId>
}

struct InputInitializer<I> {
    iterator: Box<Iterator<Item = I>>,
    sender: Sender<I>,
    shutdown: Arc<Mutex<bool>>
}

impl <I> InputInitializer<I> {
    fn run(self) {
        for item in self.iterator {
            if *self.shutdown.lock().unwrap() {
                return;
            } else {
                self.sender.send(item).unwrap();
            }
        }
    }
}

struct InputLoader<'a, I, O> {
    driver: &'a Driver,
    get_transfers: Box<Fn(I) -> MemoryTransfers>,
    groups: Vec<DpuGroup>,
    input_receiver: Receiver<I>,
    group_receiver: Receiver<DpuGroup>,
    job_sender: Sender<(DpuGroup, Vec<OutputMemoryTransfer>)>,
    output_sender: Sender<Result<O, ClusterError>>,
    shutdown: Arc<Mutex<bool>>
}

impl <'a, I, O> InputLoader<'a, I, O> {
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
            match self.driver.boot(&View::one(dpu.clone())) {
                Ok(_) => (),
                Err(err) => {
                    self.output_sender.send(Err(err)).unwrap()
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

struct ExecutionTracker<'a, O> {
    driver: &'a Driver,
    job_receiver: Receiver<(DpuGroup, Vec<OutputMemoryTransfer>)>,
    finish_sender: Sender<(DpuGroup, Vec<OutputMemoryTransfer>)>,
    output_sender: Sender<Result<O, ClusterError>>,
    shutdown: Arc<Mutex<bool>>
}

impl <'a, O> ExecutionTracker<'a, O> {
    fn run(self) {
        let mut jobs = Vec::default();

        loop {
            loop {
                match self.job_receiver.try_recv() {
                    Ok(job) => jobs.push(job),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }

            let mut new_jobs = Vec::with_capacity(jobs.len());

            for job in jobs {
                match self.fetch_group_status(&job.0) {
                    Ok(RunStatus::Idle) => self.finish_sender.send(job).unwrap(),
                    Ok(RunStatus::Running) => new_jobs.push(job),
                    Ok(RunStatus::Fault(faults)) => {
                        if faults.len() != job.0.dpus.len() {
//                        new_jobs.push(); todo
                            unimplemented!()
                        }

                        for faulting_dpu in faults {
                            self.output_sender.send(Err(ClusterError::DpuIsInFault(faulting_dpu))).unwrap();
                        }
                    },
                    Err(err) => {
                        for _ in job.0.dpus {
                            self.output_sender.send(Err(err.clone()).unwrap());
                        }
                    }
                }
            }

            jobs = new_jobs;
        }
    }

    fn fetch_group_status(&self, group: &DpuGroup) -> Result<RunStatus, ClusterError> {
        // todo add this as a view optimization?

        let mut global_status = RunStatus::default();

        for dpu in &group.dpus {
            let status = self.driver.fetch_status(&View::one(dpu.clone()))?;
            global_status = global_status.merge_with(&status)
        }

        Ok(global_status)
    }
}

struct OutputFetcher<'a> {
    driver: &'a Driver,
    finish_receiver: Receiver<(DpuGroup, Vec<OutputMemoryTransfer>)>,
    output_sender: Sender<Result<Vec<u8>, ClusterError>>,
    group_sender: Sender<DpuGroup>,
    shutdown: Arc<Mutex<bool>>
}

impl <'a> OutputFetcher<'a> {
    fn run(self) {
        for (group, transfers) in self.finish_receiver {
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
                self.driver.copy_from_memory(&mut memory_transfer)
            };

            match copy_result {
                Ok(_) => {
                    for (result, _) in vectors {
                        self.output_sender.send(Ok(result)).unwrap();
                    }
                },
                Err(err) => {
                    for _ in vectors {
                        self.output_sender.send(Err(err.clone())).unwrap()
                    }
                },
            };

            self.group_sender.send(group).unwrap();
        }
    }
}

pub struct ExecutionIterator<O> {
    output_receiver: Receiver<Result<O, ClusterError>>,
    input_initializer: Option<JoinHandle<()>>,
    input_loader: Option<JoinHandle<()>>,
    execution_tracker: Option<JoinHandle<()>>,
    output_fetcher: Option<JoinHandle<()>>,
    shutdown: Arc<Mutex<bool>>
}

impl <O> Iterator for ExecutionIterator<O> {
    type Item = Result<O, ClusterError>;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        self.output_receiver.iter().next()
    }
}

impl <O> Drop for ExecutionIterator<O> {
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