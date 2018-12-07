use dpu_sys::DpuError;

#[derive(Debug)]
pub enum ClusterError {
    NotEnoughResources {expected: u32, found: u32 },
    LowLevelError(DpuError)
}

impl From<DpuError> for ClusterError {
    fn from(err: DpuError) -> Self {
        ClusterError::LowLevelError(err)
    }
}