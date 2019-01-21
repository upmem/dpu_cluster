

use dpu_sys::DpuRank;
use dpu_sys::DpuType;
use dpu_sys::DpuTarget;
use dpu_sys::DpuError;

#[test]
fn can_allocate_functional_simulator() {
    let target = DpuTarget::for_functional_simulator();
    let (dpu_type, ref profile) = target.to_cni_args();
    if let Err(err) =  DpuRank::allocate_for(dpu_type, profile) {
        panic!("{:?}", err)
    }
}

#[test]
fn can_allocate_functional_simulator_with_profile() {
    let target = DpuTarget::for_functional_simulator().nr_of_dpus_per_control_interface(8);
    let (dpu_type, ref profile) = target.to_cni_args();
    if let Err(err) =  DpuRank::allocate_for(dpu_type, profile) {
        panic!("{:?}", err)
    }
}

#[test]
fn cannot_allocate_functional_simulator_with_invalid_profile() {
    let target = DpuTarget::for_functional_simulator().nr_of_dpus_per_control_interface(2);
    let (dpu_type, ref profile) = target.to_cni_args();
    if let Ok(_) =  DpuRank::allocate_for(dpu_type, profile) {
        panic!("should not be able to allocate rank with nr_of_dpus_per_control_interface = 2")
    }
}

#[test]
fn can_get_functional_simulator_description() {
    let target = DpuTarget::for_functional_simulator();
    let (dpu_type, ref profile) = target.to_cni_args();
    match DpuRank::get_description_for(dpu_type, profile) {
        Ok(description) => {
            assert_eq!(1, description.topology.nr_of_control_interfaces);
            assert_eq!(1, description.topology.nr_of_dpus_per_control_interface);
            assert_eq!(24, description.info.nr_of_threads);
            assert_eq!(24, description.info.nr_of_work_registers_per_thread);
            assert_eq!(40, description.info.nr_of_notify_bits);
            assert_eq!(256, description.info.nr_of_atomic_bits);
        },
        Err(err) => panic!("{:?}", err),
    }
}

#[test]
fn can_get_allocated_functional_simulator_description() {
    let target = DpuTarget::for_functional_simulator();
    let (dpu_type, ref profile) = target.to_cni_args();
    let description_result = DpuRank::allocate_for(dpu_type, profile)
        .and_then(|rank| rank.get_description());

    match description_result {
        Ok(description) => {
            assert_eq!(1, description.topology.nr_of_control_interfaces);
            assert_eq!(1, description.topology.nr_of_dpus_per_control_interface);
            assert_eq!(24, description.info.nr_of_threads);
            assert_eq!(24, description.info.nr_of_work_registers_per_thread);
            assert_eq!(40, description.info.nr_of_notify_bits);
            assert_eq!(256, description.info.nr_of_atomic_bits);
        },
        Err(err) => panic!("{:?}", err),
    }
}

#[test]
fn can_get_allocated_functional_simulator_description_with_modified_profile() {
    let target = DpuTarget::for_functional_simulator().nr_of_dpus_per_control_interface(8);
    let (dpu_type, ref profile) = target.to_cni_args();
    let description_result = DpuRank::allocate_for(dpu_type, profile)
        .and_then(|rank| rank.get_description());

    match description_result {
        Ok(description) => {
            assert_eq!(1, description.topology.nr_of_control_interfaces);
            assert_eq!(8, description.topology.nr_of_dpus_per_control_interface);
            assert_eq!(24, description.info.nr_of_threads);
            assert_eq!(24, description.info.nr_of_work_registers_per_thread);
            assert_eq!(40, description.info.nr_of_notify_bits);
            assert_eq!(256, description.info.nr_of_atomic_bits);
        },
        Err(err) => panic!("{:?}", err),
    }
}