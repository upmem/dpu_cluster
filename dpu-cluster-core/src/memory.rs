use std::collections::HashMap;
use dpu::DpuId;
use error::ClusterError;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct Location {
    pub dpu: DpuId,
    pub offset: u32
}

pub trait MemoryImage {
    fn content(&self) -> Result<Vec<u8>, ClusterError>;
    fn len(&self) -> usize;
}

impl Location {
    pub fn new(dpu: DpuId, offset: u32) -> Location {
        Location {dpu, offset}
    }
}