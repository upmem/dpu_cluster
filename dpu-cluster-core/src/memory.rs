use std::collections::HashMap;
use dpu::DpuId;
use error::ClusterError;
use std::fs::File;
use std::io::Read;
use std::time::SystemTime;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct Location {
    pub dpu: DpuId,
    pub offset: u32
}

impl Location {
    pub fn new(dpu: DpuId, offset: u32) -> Location {
        Location {dpu, offset}
    }
}

pub trait MemoryImage {
    fn content(&mut self) -> Result<&mut [u8], ClusterError>;
}

pub struct MemoryImageBuffer(Vec<u8>);

impl MemoryImage for MemoryImageBuffer {
    fn content(&mut self) -> Result<&mut [u8], ClusterError> {
        Ok(&mut self.0)
    }
}

// todo file based memory image