extern crate dpu_cluster_core;

use dpu_cluster_core::cluster::Cluster;
use dpu_cluster_core::error::ClusterError;
use dpu_cluster_core::memory_utilities::MemoryImageCollection;
use dpu_cluster_core::program::Program;
use dpu_cluster_core::view::View;
use dpu_cluster_core::config::ClusterConfiguration;
use dpu_cluster_core::memory_utilities;

fn main() {
    match basic_use() {
        Ok(_) => println!("ok"),
        Err(err) => println!("err: {:?}", err),
    }
}

fn basic_use() -> Result<MemoryImageCollection, ClusterError> {
    let config = ClusterConfiguration::default();
    let cluster = Cluster::create(config)?;
    let driver = cluster.driver();

    let nr_of_dpus = driver.nr_of_dpus();

    let inputs = memory_utilities::from_directory("images")?;
    let mut outputs = MemoryImageCollection::default();

    let program = Program::from_file("dpu.bin")?;

    let view = View::all();

    driver.load(&view, &program)?;

    for images in inputs.chunks(nr_of_dpus) {
//        driver.populate(&view, images)?;
        driver.run(&view)?;
//        let new_outputs = driver.dump(&view)?;
//        outputs.extend(new_outputs);
    }

    Ok(outputs)
}