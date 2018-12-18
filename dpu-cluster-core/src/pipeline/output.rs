use pipeline::PipelineError;
use pipeline::Pipeline;
use pipeline::OutputResult;

pub struct Output {
    pipeline: Pipeline
}

impl Iterator for Output {
    type Item = OutputResult;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        self.pipeline.output_receiver.iter().next()
    }
}