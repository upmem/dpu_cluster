use std::thread::JoinHandle;

use dpu::DpuId;
use error::ClusterError;

pub mod plan;
pub mod transfer;
pub mod output;

mod stages;
mod pipeline;

#[derive(Debug)]
pub enum PipelineError {
    InfrastructureError(ClusterError),
    ExecutionError(DpuId)
}

impl From<ClusterError> for PipelineError {
    fn from(err: ClusterError) -> Self {
        PipelineError::InfrastructureError(err)
    }
}

type OutputResult = Result<Vec<u8>, PipelineError>;
type ThreadHandle = Option<JoinHandle<()>>;
