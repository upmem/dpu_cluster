use pipeline::GroupId;
use chrono::Local;
use dpu::DpuId;

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
    Initializer,
    Loader,
    Tracker,
    Fetcher
}

pub trait EventMonitor {
    fn set_process(&mut self, process: Process);
    fn record(&mut self, event: Event);
}

#[derive(Clone)]
pub struct StdoutEventMonitor {
    process: Option<Process>
}

impl StdoutEventMonitor {
    pub fn new() -> Self {
        StdoutEventMonitor { process: Default::default() }
    }
}

impl EventMonitor for StdoutEventMonitor {
    fn set_process(&mut self, process: Process) {
        self.process = Some(process);
    }

    fn record(&mut self, event: Event) {
        let process_str = self.process.as_ref().map_or("".to_string(), |p| format!("{:?}", p));

        println!("[{}][{}] {:?}", Local::now().format("%F %T%.f").to_string(), process_str, event);
    }
}

#[derive(Clone)]
pub struct EmptyEventMonitor;

impl EmptyEventMonitor {
    pub fn new() -> Self {
        EmptyEventMonitor { }
    }
}

impl EventMonitor for EmptyEventMonitor {
    fn set_process(&mut self, _: Process) { }

    fn record(&mut self, _: Event) { }
}