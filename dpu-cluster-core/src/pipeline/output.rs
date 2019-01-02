use pipeline::OutputResult;
use pipeline::pipeline::Pipeline;

pub struct Output {
    pipeline: Pipeline
}

impl Iterator for Output {
    type Item = OutputResult;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        self.pipeline.output_receiver.iter().next()
    }
}

impl Output {
    pub fn new(pipeline: Pipeline) -> Self {
        Output { pipeline }
    }
}