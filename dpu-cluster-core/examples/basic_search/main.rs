use dpu_cluster_core::config::ClusterConfiguration;
use dpu_cluster_core::cluster::Cluster;
use dpu_cluster_core::program::Program;
use dpu_cluster_core::error::ClusterError;
use dpu_cluster_core::pipeline::PipelineError;
use std::io;
use dpu_cluster_core::pipeline::plan::Plan;
use std::sync::mpsc::channel;
use dpu_cluster_core::pipeline::transfer::MemoryTransfers;
use dpu_cluster_core::pipeline::transfer::OutputMemoryTransfer;
use dpu_cluster_core::pipeline::transfer::InputMemoryTransfer;
use std::sync::Mutex;
use std::sync::Arc;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::fs::OpenOptions;
use std::io::Write;
use dpu_cluster_core::pipeline::monitoring::RecordPolicy;

const QUERY_BUFFER_SIZE: u32 = 16;
const INPUT_BUFFER_SIZE: u32 = 1 << 24;
const OUTPUT_BUFFER_SIZE: u32 = 8 + (1 << 24);

const QUERY_BUFFER_ADDRESS: u32 = 0;
const INPUT_BUFFER_ADDRESS: u32 = QUERY_BUFFER_ADDRESS + QUERY_BUFFER_SIZE;
const OUTPUT_BUFFER_ADDRESS: u32 = INPUT_BUFFER_ADDRESS + INPUT_BUFFER_SIZE;

// The DPU program and the input files can be generated
const DPU_PROGRAM_IRAM: &'static str = "dpu.iram";
const DPU_PROGRAM_WRAM: &'static str = "dpu.wram";

#[derive(Debug)]
enum AppError {
    DpuInitError(ClusterError),
    DpuError(PipelineError),
    FileManagementError(io::Error),
    InvalidQueryId(usize)
}

struct Query {
    prefix: String
}

impl Query {
    pub fn new<S: ToString >(prefix: S) -> Self {
        Query { prefix: prefix.to_string() }
    }
}

struct Input {
    id: usize,
    fragment_id: usize,
    prefix: String
}

struct Context {
    next_fragment_id: Arc<Mutex<usize>>,
    input_file: File
}

impl Context {
    pub fn new<S: ToString>(next_fragment_id: Arc<Mutex<usize>>, file: S) -> Result<Self, AppError> {
        let input_file = File::open(file.to_string())?;

        Ok(Context { next_fragment_id, input_file })
    }
}

impl Iterator for Context {
    type Item = (usize, InputMemoryTransfer);

    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = vec![0u8; INPUT_BUFFER_SIZE as usize];

        match self.input_file.read(buffer.as_mut_slice()) {
            Ok(n) => {
                if n == 0 {
                    None
                } else {
                    let mut next_fragment_id = self.next_fragment_id.lock().unwrap();
                    let fragment_id = *next_fragment_id;
                    *next_fragment_id += 1;

                    let transfer = InputMemoryTransfer {
                        offset: INPUT_BUFFER_ADDRESS,
                        content: buffer
                    };

                    Some((fragment_id, transfer))
                }
            },
            Err(_) => None,
        }
    }
}

const DATA_FILE: &str = "data.txt";

fn main() -> Result<(), AppError> {
    let queries = vec![
        Query::new("Artemis"),
        Query::new("turtle"),
        Query::new("Schroedinger")
    ];

    let config = ClusterConfiguration::for_functional_simulator(1);
    let cluster = Cluster::create(config)?;

    let program = fetch_dpu_program()?;

    let (input_tx, input_rx) = channel();

    let nr_of_fragments = Arc::new(Mutex::new(0));
    let context = Context::new(nr_of_fragments.clone(), DATA_FILE)?;

    let outputs = Plan::from(input_rx)
        .for_persistent_model(map_input_query, context)
        .driving(cluster)
        .running(&program).monitored_by(RecordPolicy::Stdout)
        .build()?;

    let nr_of_fragments = *nr_of_fragments.lock().unwrap();

    let mut query_map: HashMap<usize, &str> = Default::default();

    for (query_id, query) in queries.iter().enumerate() {
        query_map.insert(query_id, query.prefix.as_str());

        for fragment_id in 0..nr_of_fragments {
            let input = Input {
                id: (query_id << 16) | fragment_id,
                fragment_id,
                prefix: query.prefix.clone()
            };

            input_tx.send(input).unwrap();
        }
    }

    drop(input_tx);

    for output in outputs {
        let (id, output) = output?;
        let query_id = id >> 16;

        match query_map.get(&query_id) {
            None => return Err(AppError::InvalidQueryId(query_id)),
            Some(name) => {
                process_outputs(name, output)?;
            },
        }
    }

    Ok(())
}

fn fetch_dpu_program() -> Result<Program, AppError> {
    Ok(Program::new_raw(std::fs::read(DPU_PROGRAM_IRAM)?, std::fs::read(DPU_PROGRAM_WRAM)?))
}

fn map_input_query(input: Input) -> (usize, MemoryTransfers<usize>) {
    let mut query_content = input.prefix.into_bytes();
    query_content.resize(QUERY_BUFFER_SIZE as usize, 0);

    let transfers = MemoryTransfers {
        inputs: vec![InputMemoryTransfer {
            offset: QUERY_BUFFER_ADDRESS,
            content: query_content
        }],
        output: OutputMemoryTransfer {
            offset: OUTPUT_BUFFER_ADDRESS,
            length: OUTPUT_BUFFER_SIZE
        },
        key: input.id
    };

    (input.fragment_id, transfers)
}

fn process_outputs(name: &str, output: Vec<u8>) -> Result<(), AppError> {
    let mut file = OpenOptions::new().append(true).create(true).open(format!("{}.out.txt", name))?;
    let output_bytes = (((output[0] as u32) & 0xFF) |
        (((output[1] as u32) & 0xFF) << 8) |
        (((output[2] as u32) & 0xFF) << 16) |
        (((output[3] as u32) & 0xFF) << 24)) * 32;

    let output_results = &output.as_slice()[8..(8 + output_bytes as usize)];

    file.write(output_results)?;

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