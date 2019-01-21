use crate::pipeline::GroupId;
use chrono::Local;
use crate::dpu::DpuId;

#[derive(Debug)]
pub enum Event {
    Initialization { nr_ranks: u8, nr_slices: u8, nr_dpus: u8 },
    LoadingProgramBegin { nr_instructions: u32, nr_data_bytes: u32 },
    LoadingProgramEnd,
    ProcessBegin,
    ProcessEnd,
    NewInput,
    GroupSearchBegin,
    GroupSearchEnd(GroupId),
    GroupLoadingBegin(GroupId),
    GroupLoadingEnd(GroupId),
    JobExecutionTrackingBegin(GroupId),
    JobExecutionTrackingEnd(GroupId),
    OutputFetchingBegin(GroupId),
    OutputFetchingInfo { dpu: DpuId, offset: u32, length: u32 },
    OutputFetchingEnd(GroupId),
}

#[derive(Debug, Clone)]
pub enum Process {
    Pipeline,
    Initializer,
    Mapper,
    Loader,
    Tracker,
    Fetcher
}

#[derive(Clone)]
pub enum RecordPolicy {
    Disabled,
    Stdout
}

impl Default for Process {
    fn default() -> Self {
        Process::Pipeline
    }
}

impl Default for RecordPolicy {
    fn default() -> Self {
        RecordPolicy::Disabled
    }
}

#[derive(Clone)]
pub struct EventMonitor {
    process: Process,
    policy: RecordPolicy
}

impl EventMonitor {
    pub fn new() -> Self {
        EventMonitor { process: Default::default(), policy: Default::default() }
    }

    pub fn set_process(&mut self, process: Process) {
        self.process = process;
    }

    pub fn set_policy(&mut self, policy: RecordPolicy) {
        self.policy = policy;
    }

    pub fn record(&self, event: Event) {
        match self.policy {
            RecordPolicy::Disabled => {},
            RecordPolicy::Stdout => {
                println!("[{}][{:?}] {:?}", Local::now().format("%F %T%.f").to_string(), self.process, event);
            },
        }
    }
}

impl Default for EventMonitor {
    fn default() -> Self {
        EventMonitor::new()
    }
}