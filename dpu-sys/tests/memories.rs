extern crate dpu_sys;

use dpu_sys::DpuError;
use dpu_sys::DpuRank;
use dpu_sys::DpuTarget;
use dpu_sys::DpuRankTransferMatrix;

fn allocate_rank() -> Result<DpuRank, DpuError> {
    let target = DpuTarget::for_functional_simulator().nr_of_dpus_per_control_interface(8);
    let (dpu_type, ref profile) = target.to_cni_args();
    let rank = DpuRank::allocate_for(dpu_type, profile)?;
    rank.reset_all()?;

    Ok(rank)
}

#[test]
fn can_access_irams() -> Result<(), DpuError> {
    let rank = allocate_rank()?;
    let description = rank.get_description()?;
    let input = vec![0x0ABCDEF012345678, 0xFFFFFFFFFFFFFFFF, 0xAAAAAAAAAAAAAAAA];

    rank.copy_to_irams(input.as_ptr(), input.len() as u16, 10)?;

    for slice_id in 0..description.topology.nr_of_control_interfaces {
        for member_id in 0..description.topology.nr_of_dpus_per_control_interface {
            let mut output = vec![0; input.len()];
            rank.copy_from_iram(slice_id, member_id, output.as_mut_ptr(), output.len() as u16, 10)?;
            assert_eq!(0x0000DEF012345678, output[0]);
            assert_eq!(0x0000FFFFFFFFFFFF, output[1]);
            assert_eq!(0x0000AAAAAAAAAAAA, output[2]);
        }
    }

    Ok(())
}

#[test]
fn can_handle_iram_error() -> Result<(), DpuError> {
    let rank = allocate_rank()?;
    let description = rank.get_description()?;
    let input = vec![0x0ABCDEF012345678, 0xFFFFFFFFFFFFFFFF, 0xAAAAAAAAAAAAAAAA];

    if let Ok(_) = rank.copy_to_irams(input.as_ptr(), input.len() as u16, description.memories.iram_size) {
        panic!("should not be able to copy to irams after the end of the memory")
    }

    Ok(())
}

#[test]
fn can_access_wrams() -> Result<(), DpuError> {
    let rank = allocate_rank()?;
    let description = rank.get_description()?;
    let input = vec![0x12345678, 0xFFFFFFFF, 0xAAAAAAAA];

    rank.copy_to_wrams(input.as_ptr(), input.len() as u32, 4)?;

    for slice_id in 0..description.topology.nr_of_control_interfaces {
        for member_id in 0..description.topology.nr_of_dpus_per_control_interface {
            let mut output = vec![0; input.len()];
            rank.copy_from_wram(slice_id, member_id, output.as_mut_ptr(), output.len() as u32, 4)?;
            assert_eq!(0x12345678, output[0]);
            assert_eq!(0xFFFFFFFF, output[1]);
            assert_eq!(0xAAAAAAAA, output[2]);
        }
    }

    Ok(())
}

#[test]
fn can_handle_wram_error() -> Result<(), DpuError> {
    let rank = allocate_rank()?;
    let description = rank.get_description()?;
    let input = vec![0x0ABCDEF0, 0xFFFFFFFF, 0xAAAAAAAA];

    if let Ok(_) = rank.copy_to_wrams(input.as_ptr(), input.len() as u32, description.memories.wram_size) {
        panic!("should not be able to copy to wrams after the end of the memory")
    }

    Ok(())
}

#[test]
fn can_access_mrams() -> Result<(), DpuError> {
    let rank = allocate_rank()?;
    let description = rank.get_description()?;
    let input_matrix = DpuRankTransferMatrix::allocate_for(&rank)?;
    let output_matrix = DpuRankTransferMatrix::allocate_for(&rank)?;
    let mut input = vec![0xAA, 0xBB, 0xCC, 0x12, 0x00, 0x42, 0x2A];
    let mut first_output = vec![0; input.len()];
    let mut second_output = vec![0; input.len()];

    input_matrix.add_dpu(0, 0, input.as_mut_ptr(), input.len() as u32, 2, 0);
    input_matrix.add_dpu(0, 1, input.as_mut_ptr(), input.len() as u32, 2, 0);

    output_matrix.add_dpu(0, 0, first_output.as_mut_ptr(), input.len() as u32, 2, 0);
    output_matrix.add_dpu(0, 1, second_output.as_mut_ptr(), input.len() as u32, 2, 0);

    rank.copy_to_mrams(&input_matrix)?;
    rank.copy_from_mrams(&output_matrix)?;

    assert_eq!(0xAA, first_output[0]);
    assert_eq!(0xBB, first_output[1]);
    assert_eq!(0xCC, first_output[2]);
    assert_eq!(0x12, first_output[3]);
    assert_eq!(0x00, first_output[4]);
    assert_eq!(0x42, first_output[5]);
    assert_eq!(0x2A, first_output[6]);

    assert_eq!(0xAA, second_output[0]);
    assert_eq!(0xBB, second_output[1]);
    assert_eq!(0xCC, second_output[2]);
    assert_eq!(0x12, second_output[3]);
    assert_eq!(0x00, second_output[4]);
    assert_eq!(0x42, second_output[5]);
    assert_eq!(0x2A, second_output[6]);

    Ok(())
}

#[test]
fn can_handle_mram_error() -> Result<(), DpuError> {
    let rank = allocate_rank()?;
    let description = rank.get_description()?;
    let matrix = DpuRankTransferMatrix::allocate_for(&rank)?;
    let mut input = vec![0xAA, 0xBB, 0xCC, 0x12, 0x00, 0x42, 0x2A];

    matrix.add_dpu(0, 0, input.as_mut_ptr(), input.len() as u32, description.memories.mram_size, 0);

    if let Ok(_) = rank.copy_to_mrams(&matrix) {
        panic!("should not be able to copy to mrams after the end of the memory")
    }

    Ok(())
}