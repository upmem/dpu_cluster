use driver::Driver;
use std::sync::mpsc::Receiver;
use pipeline::stages::DpuGroup;
use pipeline::transfer::OutputMemoryTransfer;
use std::sync::mpsc::Sender;
use pipeline::OutputResult;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::TryRecvError;
use driver::RunStatus;
use error::ClusterError;
use view::View;
use driver::Mergeable;
use pipeline::PipelineError;
use pipeline::ThreadHandle;
use std::thread;
use pipeline::stages::GroupJob;
use cluster::Cluster;

pub struct ExecutionTracker {
    cluster: Arc<Cluster>,
    job_receiver: Receiver<GroupJob>,
    finish_sender: Sender<GroupJob>,
    output_sender: Sender<OutputResult>,
    shutdown: Arc<Mutex<bool>>
}

impl ExecutionTracker {
    pub fn launch(self) -> ThreadHandle {
        Some(thread::spawn(|| self.run()))
    }

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
                            self.output_sender.send(Err(PipelineError::ExecutionError(faulting_dpu))).unwrap();
                        }
                    },
                    Err(err) => {
                        for _ in job.0.dpus {
                            self.output_sender.send(Err(PipelineError::InfrastructureError(err.clone())).unwrap());
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
            let status = self.cluster.driver().fetch_status(&View::one(dpu.clone()))?;
            global_status = global_status.merge_with(&status)
        }

        Ok(global_status)
    }
}