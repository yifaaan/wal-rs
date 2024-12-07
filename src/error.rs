use thiserror::Error;

#[derive(Error, Debug)]
pub enum WalError {
    #[error("Open file failed")]
    OpenFileFailed(#[from] std::io::Error),

    #[error("OsString to String failed")]
    FileNameCovertFailed,

    #[error("Parse int failed")]
    ParseIntFailed(#[from] std::num::ParseIntError),

    #[error("Segment file not found")]
    SegmentFileNotFound,
}
