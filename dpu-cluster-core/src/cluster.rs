use crate::config::ClusterConfiguration;
use crate::driver::Driver;
use dpu_sys::DpuRank;
use dpu_sys::DpuRankDescription;
use dpu_sys::DpuTarget;
use crate::error::ClusterError;
use crate::dpu::Mapping;
use crate::dpu::DpuId;

#[derive(Debug)]
pub struct Cluster {
    driver: Driver,
    workers: Mapping
}

impl Cluster {
    pub fn create(config: ClusterConfiguration) -> Result<Self, ClusterError> {
        // todo: take system-wide lock for specific dpu target
        let max_nr_dpus = find_nr_of_available_dpus_for(&config.target)?;

        let nr_of_dpus_expected = config.nr_of_dpus_expected.unwrap_or(max_nr_dpus);

        if nr_of_dpus_expected > max_nr_dpus {
            return Err(ClusterError::NotEnoughResources { expected: nr_of_dpus_expected, found: max_nr_dpus});
        }

        let rank_description = find_description_for(&config.target)?;
        let ranks = allocate_at_least(nr_of_dpus_expected, &rank_description, &config.target)?;

        let mut dpu_ids= Vec::default();
        for rank_id in 0..ranks.len() as u8 {
            for slice_id in 0..rank_description.topology.nr_of_control_interfaces {
                for member_id in 0..rank_description.topology.nr_of_dpus_per_control_interface {
                    let dpu_id = DpuId::new(rank_id, slice_id, member_id);
                    dpu_ids.push(dpu_id);
                }
            }
        }

        let driver = Driver::new(ranks, rank_description, config.target);

        let workers = Mapping::new(dpu_ids);

        Ok(Cluster { driver, workers })
    }

    pub fn driver(&self) -> &Driver {
        &self.driver
    }

    pub fn topology(&self) -> (u8, u8, u8) {
        (
            self.driver.nr_of_ranks,
            self.driver.rank_description.topology.nr_of_control_interfaces,
            self.driver.rank_description.topology.nr_of_dpus_per_control_interface
        )
    }

    pub fn target(&self) -> DpuTarget {
        self.driver.target.clone()
    }
}

fn find_nr_of_available_dpus_for(target: &DpuTarget) -> Result<u32, ClusterError> {
    let (dpu_type, ref profile) = target.to_cni_args();
    let nr_of_dpus = DpuRank::find_nr_of_available_dpus_for(dpu_type, profile)?;

    Ok(nr_of_dpus)
}

fn find_description_for(target: &DpuTarget) -> Result<DpuRankDescription, ClusterError> {
    let (dpu_type, ref profile) = target.to_cni_args();
    let description = DpuRank::get_description_for(dpu_type, profile)?;

    Ok(description)
}

fn allocate_at_least(nr_of_dpus: u32, description: &DpuRankDescription, target: &DpuTarget) -> Result<Vec<DpuRank>, ClusterError> {
    let nr_of_dpus_per_rank = (description.topology.nr_of_control_interfaces as u32) * (description.topology.nr_of_dpus_per_control_interface as u32);
    let nr_of_ranks = (nr_of_dpus / nr_of_dpus_per_rank) + if (nr_of_dpus % nr_of_dpus_per_rank) == 0 { 0 } else { 1 };
    let mut ranks = Vec::with_capacity(nr_of_ranks as usize);

    let (dpu_type, ref profile) = target.to_cni_args();

    for _ in 0..nr_of_ranks {
        let rank = DpuRank::allocate_for(dpu_type, profile)?;
        rank.reset_all()?;
        ranks.push(rank);
    }

    Ok(ranks)
}