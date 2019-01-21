use dpu_cluster_core::config::ClusterConfiguration;
use dpu_cluster_core::cluster::Cluster;
use dpu_cluster_core::program::Program;
use std::fs::File;
use std::io::BufReader;
use std::io::BufRead;
use dpu_cluster_core::pipeline::PipelineError;
use std::io;
use std::fs::OpenOptions;
use std::io::Write;
use dpu_cluster_core::pipeline::plan::Plan;
use dpu_cluster_core::pipeline::transfer::MemoryTransfers;
use dpu_cluster_core::pipeline::transfer::OutputMemoryTransfer;
use dpu_cluster_core::pipeline::transfer::InputMemoryTransfer;
use dpu_cluster_core::error::ClusterError;

const NB_WORD_MAX: u32 = 7450;

const NB_WORD_SIZE: u32 = 8;
const STATS_SIZE: u32 = 64;
const ADDRESSES_SIZE: u32 = NB_WORD_MAX * 4;

const NB_OF_WORDS_OFFSET: u32 = 0;
const STATS_OFFSET: u32 = NB_OF_WORDS_OFFSET + NB_WORD_SIZE;
const ADDRESSES_OFFSET: u32 = STATS_OFFSET + STATS_SIZE;
const STRINGS_OFFSET: u32 = ADDRESSES_OFFSET + ADDRESSES_SIZE;

// The DPU program and the input files can be generated in the main sort_strings repo
const DPU_PROGRAM_IRAM: &'static str = "dpu.iram";
const DPU_PROGRAM_WRAM: &'static str = "dpu.wram";

#[derive(Debug)]
enum AppError {
    DpuInitError(ClusterError),
    DpuError(PipelineError),
    FileManagementError(io::Error),
    InputFileTooBig(usize)
}

const INPUTS: &'static [&'static str] = &["input.txt"; 16];

fn main() -> Result<(), AppError> {
    let config = ClusterConfiguration::for_functional_simulator(1);
    let cluster = Cluster::create(config)?;
    let mram_size = cluster.driver().rank_description.memories.mram_size;

    let program = fetch_dpu_program()?;

    let inputs = INPUTS.iter()
        .enumerate()
        .map(move |(idx, filename)| extract_inputs(filename, mram_size, idx).unwrap());

    let outputs = Plan::from(inputs)
        .for_simple_model(map_transfers)
        .driving(cluster)
        .running(&program)
        .build()?;

    for output in outputs {
        let (idx, output) = output?;
        let filename = format!("output{}.txt", idx);
        process_outputs(output, &filename)?;
    }

    Ok(())
}

fn fetch_dpu_program() -> Result<Program, AppError> {
    Ok(Program::new_raw(std::fs::read(DPU_PROGRAM_IRAM)?, std::fs::read(DPU_PROGRAM_WRAM)?))
}

fn map_transfers(input: (usize, Vec<u8>, Vec<u32>)) -> MemoryTransfers<usize> {
    let (idx, strings, addresses) = input;
    let nr_of_words = addresses.len() as u32;

    MemoryTransfers {
        inputs: vec![
            InputMemoryTransfer::from_u8_vec(STRINGS_OFFSET, strings),
            InputMemoryTransfer::from_u32_vec(ADDRESSES_OFFSET, addresses),
            InputMemoryTransfer::from_u32_vec(NB_OF_WORDS_OFFSET, vec![nr_of_words, 0]),
        ],
        output: OutputMemoryTransfer {
            offset: ADDRESSES_OFFSET,
            length: nr_of_words * 4
        },
        key: idx
    }
}

fn extract_inputs(filename: &str, mram_size: u32, idx: usize) -> Result<(usize, Vec<u8>, Vec<u32>), AppError> {
    let file = File::open(filename)?;
    let file = BufReader::new(file);

    let mut string_addresses = Vec::default();
    let mut strings = Vec::default();

    for line in file.lines() {
        let offset = strings.len();
        let line = line?;

        strings.extend(line.as_bytes());
        strings.push(b'\0');

        let end = strings.len();
        strings.resize((end + 7) & !7, b'\0');

        string_addresses.push(offset as u32);
    }

    if strings.len() > ((mram_size - STRINGS_OFFSET) as usize) {
        return Err(AppError::InputFileTooBig(strings.len()))
    }

    Ok((idx, strings, string_addresses))
}

fn process_outputs(output: Vec<u8>, filename: &str) -> Result<(), AppError> {
    let mut file = OpenOptions::new().write(true).create(true).truncate(true).open(filename)?;

    for entry in output.chunks(4) {
        let index = {
            let mut val = 0u32;
            for (idx, byte) in entry.iter().enumerate() {
                val = val | (((*byte as u32) & 0xff) << (idx * 8));
            }
            val
        };

        let index_as_string = format!("{}", index);
        file.write(index_as_string.as_bytes())?;
        file.write(b"\n")?;
    }

    Ok(())
}

impl From<ClusterError> for AppError {
    fn from(err: ClusterError) -> Self {
        AppError::DpuInitError(err)
    }
}

impl From<PipelineError> for AppError {
    fn from(err: PipelineError) -> Self {
        AppError::DpuError(err)
    }
}

impl From<io::Error> for AppError {
    fn from(err: io::Error) -> Self {
        AppError::FileManagementError(err)
    }
}