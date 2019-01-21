use std::thread::JoinHandle;

use crate::dpu::DpuId;
use crate::error::ClusterError;

pub mod plan;
pub mod transfer;
pub mod output;
pub mod monitoring;

mod stages;
mod pipeline;

#[derive(Debug)]
pub enum PipelineError {
    UndefinedCluster,
    InfrastructureError(ClusterError),
    ExecutionError(DpuId),
    // todo: add fragment id info
    UnknownFragmentId
}

impl From<ClusterError> for PipelineError {
    fn from(err: ClusterError) -> Self {
        PipelineError::InfrastructureError(err)
    }
}

pub enum GroupPolicy {
    Dpu,
    Slice
}

impl Default for GroupPolicy {
    fn default() -> Self {
        GroupPolicy::Slice
    }
}

type OutputResult<K> = Result<(K, Vec<u8>), PipelineError>;
type ThreadHandle = Option<JoinHandle<()>>;

type GroupId = u32;