use dpu_sys::DpuTarget;

#[derive(Default)]
pub struct ClusterConfiguration {
    pub target: DpuTarget,
    pub nr_of_dpus_expected: Option<u32>
}

impl ClusterConfiguration {
    pub fn for_functional_simulator(nr_of_dpus: u32) -> ClusterConfiguration {
        ClusterConfiguration {
            target: DpuTarget::for_functional_simulator(),
            nr_of_dpus_expected: Some(nr_of_dpus)
        }
    }

    pub fn for_hardware(nr_of_dpus: u32) -> ClusterConfiguration {
        ClusterConfiguration {
            target: DpuTarget::for_hardware_implementation(),
            nr_of_dpus_expected: Some(nr_of_dpus)
        }
    }
}