use std::thread;
use pipeline::ThreadHandle;
use dpu::DpuId;
use pipeline::transfer::OutputMemoryTransfer;

pub mod initializer;
pub mod loader;
pub mod tracker;
pub mod fetcher;

struct DpuGroup {
    dpus: Vec<DpuId>
}

type GroupJob = (DpuGroup, Vec<OutputMemoryTransfer>);