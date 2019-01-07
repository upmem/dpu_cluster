use std::time::Instant;
use pipeline::GroupId;
use chrono::Local;

#[derive(Debug)]
pub enum Event {
    LoadingProgramBegin,
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
    OutputFetchingEnd(GroupId),
}

#[derive(Debug, Clone)]
pub enum Process {
    Initializer,
    Loader,
    Tracker,
    Fetcher
}

#[derive(Clone)]
pub struct EventMonitor {
    process: Option<Process>
}

impl EventMonitor {
    pub fn new() -> Self {
        EventMonitor {  process: Default::default() }
    }

    pub fn set_process(&mut self, process: Process) {
        self.process = Some(process);
    }

    pub fn record(&mut self, event: Event) {
        let process_str = self.process.iter().fold("".to_string(), |_, p| format!("{:?}", p));

        println!("[{}][{}] {:?}", Local::now().format("%F %T%.f").to_string(), process_str, event);
    }
}