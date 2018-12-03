extern crate dpu_cluster_core;

use dpu_cluster_core::cluster::Cluster;
use dpu_cluster_core::error::ClusterError;
use dpu_cluster_core::memory::MemoryImageCollection;
use dpu_cluster_core::program::Program;
use dpu_cluster_core::view::View;
use dpu_cluster_core::config::ClusterConfiguration;
use dpu_cluster_core::memory;

fn main() {
    match basic_use() {
        Ok(_) => println!("ok"),
        Err(err) => println!("err: {:?}", err),
    }
}

fn basic_use() -> Result<MemoryImageCollection, ClusterError> {
    let config = ClusterConfiguration::default();
    let cluster = Cluster::create(config)?;

    let nr_of_dpus = cluster.nr_of_dpus();

    let inputs = memory::from_directory("images")?;
    let mut outputs = MemoryImageCollection::default();

    let program = Program::from_file("dpu.bin")?;

    cluster.load(&program)?;

    for images in inputs.chunks(nr_of_dpus) {
        cluster.populate(images)?;
        cluster.run()?;
        let new_outputs = cluster.dump()?;
        outputs.extend(new_outputs);
    }

    Ok(outputs)
}