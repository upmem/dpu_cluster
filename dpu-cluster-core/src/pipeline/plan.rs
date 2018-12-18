use program::Program;
use view::View;
use std::iter::FromIterator;
use driver::Driver;
use pipeline::transfer::MemoryTransfers;
use pipeline::PipelineError;
use pipeline::output::Output;

pub struct Plan<'a, T, F> {
    base_iterator: Box<Iterator<Item = T>>,
    program: Option<&'a Program>,
    grouped_by: Option<&'a View>,
    transfers_fn: F,
}

impl <'a, T, F> FromIterator<T> for Plan<'a, T, F> {
    fn from_iter<I: IntoIterator<Item=T>>(iter: I) -> Self {
        unimplemented!()
    }
}

impl <'a, T, F> Plan<'a, T, F> {
    pub fn using(mut self, view: &'a View) -> Self {
        self.grouped_by = Some(view);
        self
    }

    pub fn running(mut self, program: &'a Program) -> Self {
        self.program = Some(program);
        self
    }

    pub fn transfers(mut self, func: F) -> Self
        where F: Fn(T) -> MemoryTransfers
    {
        self.transfers_fn = func;
        self
    }

    pub fn execute(self, driver: &Driver) -> Result<Output, PipelineError> {
        let all = View::all();
        let view = self.grouped_by.unwrap_or_else(|| &all);
        if let Some(program) = self.program {
            driver.load(view, program)?
        }

        let pipeline = unimplemented!();

        Ok(Output { pipeline })
    }
}