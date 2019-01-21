use crate::dpu::DpuId;
use crate::pipeline::transfer::OutputMemoryTransfer;
use crate::pipeline::GroupId;
use crate::pipeline::ThreadHandle;
use std::thread;

pub mod initializer;
pub mod mapper;
pub mod loader;
pub mod tracker;
pub mod fetcher;

pub trait Stage: Sized + Send + 'static {
    fn launch(self) -> ThreadHandle {
        Some(thread::spawn(|| self.run()))
    }

    fn run(self);
}

pub struct DpuGroup {
    pub id: GroupId,
    pub dpus: Vec<DpuId>
}
type GroupJob<K> = (DpuGroup, Vec<(K, OutputMemoryTransfer)>);