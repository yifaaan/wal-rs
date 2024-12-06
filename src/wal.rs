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
        let mut data_to_write_size = data_size;

        while data_to_write_size > 0 {
            // Calculate how much can fit in this block.
            let mut chunk_size =
                (BLOCK_SIZE - self.current_block_size - CHUNK_HEADER_SIZE) as usize;
            // 确保不写入多余的数据
            if chunk_size > data_to_write_size {
                chunk_size = data_to_write_size;
            }
            let mut chunk = vec![0; chunk_size];

            // data_size-data_to_write_size: 已经写入的数据量，即data当前的偏移
            let cur_write_idx = data_size - data_to_write_size;
            // chunk_size: 当前即将写入的数据量
            let mut end = cur_write_idx + chunk_size as usize;
            // In fact, this is not to be happend.
            if end > data_size {
                end = data_size
            }
            chunk.copy_from_slice(&data[cur_write_idx..end]);
            // Write the chunks
            if data_to_write_size == data_size {
                // First chunk: when data_to_write_size == data_size
                self.write_internal(chunk, ChunkType::First)?;
            } else if data_to_write_size == chunk_size {
                // Last chunk
                self.write_internal(chunk, ChunkType::Last)?;
            } else {
                self.write_internal(chunk, ChunkType::Middle)?;
            }
            // Update the left data size
            data_to_write_size -= chunk_size;
        }
        Ok(position)
    }
}
