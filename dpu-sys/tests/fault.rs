extern crate dpu_sys;

use dpu_sys::DpuError;
use dpu_sys::DpuRank;
use dpu_sys::DpuTarget;
use dpu_sys::DpuDebugContext;

fn allocate_rank() -> Result<DpuRank, DpuError> {
    let target = DpuTarget::for_functional_simulator();
    let (dpu_type, ref profile) = target.to_cni_args();
    let rank = DpuRank::allocate_for(dpu_type, profile)?;
    rank.reset_all()?;

    Ok(rank)
}

#[test]
fn can_do_simple_fault_operations() -> Result<(), DpuError> {
    let rank = allocate_rank()?;
    let description = rank.get_description()?;
    let mut context = DpuDebugContext::new(description.info.nr_of_threads,
                                       description.info.nr_of_work_registers_per_thread,
                                       description.info.nr_of_atomic_bits);

    // todo: can we check anything in the context to verify something?
    rank.fault_dpu(0, 0)?;
    rank.initialize_fault_process_for_dpu(0, 0, &mut context)?;
    rank.finalize_fault_process_for_dpu(0, 0, &mut context)?;

    Ok(())
}