use std::{io::Write, os::unix::fs::FileExt, path::Path};

use crate::error::WalError;

/// 7 Bytes
///
/// Checksum: 4
///
/// Type: 2
///
/// Lenght: 1
const CHUNK_HEADER_SIZE: u32 = 7;

/// 32 KB
const BLOCK_SIZE: u32 = 32 * 1024;
const FILE_MODE_PERM: u32 = 0o644;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ChunkType {
    Full,
    First,
    Middle,
    Last,
}

impl From<u8> for ChunkType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Full,
            1 => Self::First,
            2 => Self::Middle,
            3 => Self::Last,
            _ => unreachable!(),
        }
    }
}

impl From<ChunkType> for u8 {
    fn from(value: ChunkType) -> Self {
        match value {
            ChunkType::Full => 0,
            ChunkType::First => 1,
            ChunkType::Middle => 2,
            ChunkType::Last => 3,
        }
    }
}
struct Wal {
    file: std::sync::RwLock<std::fs::File>,
    current_block_number: u32,
    current_block_size: u32,
}

struct ChunkStartPosition {
    block_number: u32,
    chunk_offset: u64,
}

impl Wal {
    pub fn open(filename: impl AsRef<Path>) -> Result<Self, WalError> {
        let file = std::fs::File::options()
            .write(true)
            .read(true)
            .create(true)
            .open(filename.as_ref())?;
        Ok(Self {
            file: std::sync::RwLock::new(file),
            current_block_number: 0,
            current_block_size: 0,
        })
    }

    pub fn sync(&self) -> Result<(), WalError> {
        let file = self.file.write().unwrap();
        file.sync_all()?;
        Ok(())
    }

    pub fn write(&mut self, data: Vec<u8>) -> Result<ChunkStartPosition, WalError> {
        // The left block space is not enough for a chunk header
        if self.current_block_size + CHUNK_HEADER_SIZE >= BLOCK_SIZE {
            // Zeror padding if necessary
            if self.current_block_size < BLOCK_SIZE {
                let padding = vec![0; (BLOCK_SIZE - self.current_block_size) as usize];
                let mut file = self.file.write().unwrap();
                file.write(&padding)?;
            }
            // Need a new block, clear the current block size.
            self.current_block_number += 1;
            self.current_block_size = 0;
        }
        // The start position(for read)
        let position = ChunkStartPosition {
            block_number: self.current_block_number,
            chunk_offset: self.current_block_size as u64,
        };
        let data_size = data.len();
        // The entire data and header can fit into the block
        if self.current_block_size + data_size as u32 + CHUNK_HEADER_SIZE <= BLOCK_SIZE {
            self.write_internal(data, ChunkType::Full)?;
            return Ok(position);
        }
        // If the size of the data exceeds the size of the block,
        // the data should be written to the block in batches.
        let mut left_size = data_size;

        while left_size > 0 {
            // The left free size of the current chunk to store the data.
            let mut chunk_size =
                (BLOCK_SIZE - self.current_block_size - CHUNK_HEADER_SIZE) as usize;
            // If the left size is enugh to store the left data
            if chunk_size > left_size {
                chunk_size = left_size;
            }
            let mut chunk = vec![0; chunk_size as usize];
            let mut end = data_size - left_size + chunk_size as usize;
            if end > data_size {
                end = data_size
            }

            chunk.copy_from_slice(&data[data_size - left_size..end]);

            // Write the chunks
            if left_size == data_size {
                // First chunk
                self.write_internal(chunk, ChunkType::First)?;
            } else if left_size == chunk_size {
                // Last chunk
                self.write_internal(chunk, ChunkType::Last)?;
            } else {
                self.write_internal(chunk, ChunkType::Middle)?;
            }
            left_size -= chunk_size;
        }

        Ok(position)
    }
}
