use crate::dpu::DpuId;
use crate::pipeline::transfer::OutputMemoryTransfer;
use crate::pipeline::GroupId;
use crate::pipeline::ThreadHandle;
use std::thread;
use crate::pipeline::PipelineError;

pub mod initializer;
pub mod mapper;
pub mod loader;
pub mod tracker;
pub mod fetcher;

pub trait Stage: Sized + Send + 'static {
    fn launch(mut self) -> Result<ThreadHandle, PipelineError> {
        self.init()?;

        Ok(Some(thread::spawn(|| self.run())))
    }

    fn init(&mut self) -> Result<(), PipelineError> { Ok(()) }
    fn run(self);
}

#[derive(Clone)]
pub struct DpuGroup {
    pub id: GroupId,
    pub dpus: Vec<(DpuId, bool)>
}

impl DpuGroup {
    pub fn active_dpus(&self) -> impl Iterator<Item=&DpuId> {
        self.dpus.iter().filter_map(|(dpu , active)| if *active { Some(dpu) } else { None })
    }
}

type GroupJob<K> = (DpuGroup, Vec<(K, OutputMemoryTransfer)>);