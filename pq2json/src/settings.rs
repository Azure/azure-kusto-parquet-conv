#[derive(Debug, Clone)]
pub struct Settings {
    pub omit_nulls: bool,
    pub omit_empty_bags: bool,
    pub omit_empty_lists: bool,
    pub timestamp_rendering: TimestampRendering,
    pub columns: Option<Vec<String>>,
    pub csv: bool,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TimestampRendering {
    Ticks,
    IsoStr,
    UnixMs,
}
