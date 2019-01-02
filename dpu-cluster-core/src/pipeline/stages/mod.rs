use dpu::DpuId;
use pipeline::transfer::OutputMemoryTransfer;

pub mod initializer;
pub mod loader;
pub mod tracker;
pub mod fetcher;

pub struct DpuGroup {
    pub dpus: Vec<DpuId>
}

type GroupJob = (DpuGroup, Vec<OutputMemoryTransfer>);