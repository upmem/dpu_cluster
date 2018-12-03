use config::ClusterConfiguration;
use driver::Driver;
use dpu_sys::DpuRank;
use dpu_sys::DpuRankDescription;
use dpu_sys::DpuTarget;
use error::ClusterError;
use view::View;
use view::ViewSelection;

#[derive(Debug)]
pub struct Cluster {
    config: ClusterConfiguration,
    all_selected: ViewSelection,
    driver: Driver
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

        let all_selected = ViewSelection::all();

        let driver = Driver::new(ranks, rank_description);

        Ok(Cluster { config, all_selected, driver })
    }
}

impl View for Cluster {
    fn selection(&self) -> &ViewSelection {
        &self.all_selected
    }

    fn driver(&self) -> &Driver {
        &self.driver
    }
}

fn find_nr_of_available_dpus_for(target: &DpuTarget) -> Result<u32, ClusterError> {
    let (dpu_type, ref profile) = target.to_cni_args();
    let nr_of_dpus = dpu_sys::find_nr_of_available_dpus_for(dpu_type, profile)?;

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