use crate::pipeline::OutputResult;
use crate::pipeline::pipeline::Pipeline;

pub struct Output<K> {
    pipeline: Pipeline<K>
}

impl <K> Iterator for Output<K> {
    type Item = OutputResult<K>;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        self.pipeline.output_receiver.iter().next()
    }
}

impl <K> Output<K> {
    pub fn new(pipeline: Pipeline<K>) -> Self {
        Output { pipeline }
    }
}