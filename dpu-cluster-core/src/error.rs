use dpu_sys::DpuError;
use crate::dpu::DpuId;

#[derive(Debug, Clone)]
pub enum ClusterError {
    NotEnoughResources {expected: u32, found: u32 },
    LowLevelError(DpuError),
    DpuIsAlreadyRunning,
    DpuIsInFault(DpuId)
}

impl From<DpuError> for ClusterError {
    fn from(err: DpuError) -> Self {
        ClusterError::LowLevelError(err)
    }
}