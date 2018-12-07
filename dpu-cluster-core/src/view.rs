use dpu::DpuId;

pub enum Selection<T> {
    All,
    None,
    Some(Vec<T>)
}

pub enum FastSelection<T> {
    Fast(DpuId),
    Normal(Selection<T>)
}

impl <T> Default for Selection<T> {
    fn default() -> Self {
        Selection::None
    }
}

impl <T> Default for FastSelection<T> {
    fn default() -> Self {
        FastSelection::Normal(Default::default())
    }
}

#[derive(Default)]
pub struct View(pub FastSelection<Selection<Selection<DpuId>>>);

impl View {
    pub fn all() -> View {
        View(FastSelection::Normal(Selection::All))
    }
}