use dpu::DpuId;
use pipeline::transfer::OutputMemoryTransfer;
use pipeline::GroupId;

pub mod initializer;
pub mod loader;
pub mod tracker;
pub mod fetcher;

pub struct DpuGroup {
    pub id: GroupId,
    pub dpus: Vec<DpuId>
}
type GroupJob = (DpuGroup, Vec<OutputMemoryTransfer>);