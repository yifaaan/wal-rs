use thiserror::Error;

#[derive(Error, Debug)]
pub enum WalError {
    #[error("Open file failed")]
    OpenFileFailed(#[from] std::io::Error),
}
