use std::sync::mpsc::Receiver;
use crate::pipeline::stages::DpuGroup;
use std::sync::mpsc::Sender;
use std::sync::mpsc::SyncSender;
use crate::pipeline::OutputResult;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::TryRecvError;
use crate::driver::RunStatus;
use crate::error::ClusterError;
use crate::view::View;
use crate::driver::Mergeable;
use crate::pipeline::PipelineError;
use std::thread;
use crate::pipeline::stages::GroupJob;
use crate::cluster::Cluster;
use crate::pipeline::monitoring::EventMonitor;
use crate::pipeline::monitoring::Process;
use crate::pipeline::monitoring::Event;
use crate::driver::Driver;
use std::time::Duration;
use crate::pipeline::stages::Stage;

pub struct ExecutionTracker<InputHandle> {
    cluster: Arc<Cluster>,
    job_receiver: Receiver<GroupJob<InputHandle>>,
    finish_sender: Sender<GroupJob<InputHandle>>,
    output_sender: SyncSender<OutputResult<InputHandle>>,
    sleep_duration: Option<Duration>,
    monitoring: EventMonitor,
    // todo: use or remove
    shutdown: Arc<Mutex<bool>>
}

impl <InputHandle> ExecutionTracker<InputHandle>
    where InputHandle: Send + 'static
{
    pub fn new(cluster: Arc<Cluster>,
               job_receiver: Receiver<GroupJob<InputHandle>>,
               finish_sender: Sender<GroupJob<InputHandle>>,
               output_sender: SyncSender<OutputResult<InputHandle>>,
               sleep_duration: Option<Duration>,
               mut monitoring: EventMonitor,
               shutdown: Arc<Mutex<bool>>) -> Self {
        monitoring.set_process(Process::Tracker);

        ExecutionTracker { cluster, job_receiver, finish_sender, output_sender, sleep_duration, monitoring, shutdown }
    }
}

impl <InputHandle> Stage for ExecutionTracker<InputHandle>
    where InputHandle: Send + 'static
{
    fn run(self) {
        let monitoring = self.monitoring;

        monitoring.record(Event::ProcessBegin);

        let mut jobs = Vec::default();

        loop {
            loop {
                match self.job_receiver.try_recv() {
                    Ok(job) => {
                        monitoring.record(Event::JobExecutionTrackingBegin(job.0.id));
                        jobs.push(job);
                    },
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) =>
                        if jobs.len() == 0 {
                            monitoring.record(Event::ProcessEnd);
                            return
                        } else {
                            break
                        },
                }
            }

            let mut new_jobs = Vec::with_capacity(jobs.len());

            for job in jobs {
                let group_id = job.0.id;
                match fetch_group_status(self.cluster.driver(), &job.0) {
                    Ok(RunStatus::Running) => new_jobs.push(job),
                    Ok(RunStatus::Idle) => {
                        monitoring.record(Event::JobExecutionTrackingEnd(group_id));
                        self.finish_sender.send(job).unwrap();
                    },
                    Ok(RunStatus::Fault(faults)) => {
                        monitoring.record(Event::JobExecutionTrackingEnd(group_id));
                        for faulting_dpu in faults {
                            self.output_sender.send(Err(PipelineError::ExecutionError(faulting_dpu))).unwrap();
                        }
                    },
                    Err(err) => {
                        monitoring.record(Event::JobExecutionTrackingEnd(group_id));
                        self.output_sender.send(Err(PipelineError::InfrastructureError(err))).unwrap();
                    }
                }
            }

            jobs = new_jobs;

            // todo: can we avoid destructuring self.sleep_duration at each iteration? (not costly at all, but not needed)
            if let Some(sleep_duration) = self.sleep_duration {
                thread::sleep(sleep_duration);
            }
        }
    }
}

fn fetch_group_status(driver: &Driver, group: &DpuGroup) -> Result<RunStatus, ClusterError> {
    // todo add this as a view optimization?

    let mut global_status = RunStatus::default();

    for dpu in group.active_dpus() {
        let status = driver.fetch_status(&View::one(dpu.clone()))?;
        global_status = global_status.merge_with(&status)
    }

    Ok(global_status)
}