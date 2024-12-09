use std::{
    io::{Seek, Write},
    os::unix::fs::{FileExt, PermissionsExt},
    path::Path,
};

use crate::error::WalError;

/// 7 Bytes
///
/// Checksum: 4
///
/// Type: 2
///
/// Lenght: 1
pub(crate) const CHUNK_HEADER_SIZE: u32 = 7;

/// 32 KB
pub(crate) const BLOCK_SIZE: u32 = 32 * 1024;
/// File mod
const FILE_MODE_PERM: u32 = 0o644;
/// File suffix
pub(crate) const SEGMENT_FILE_SUFFIX: &'static str = ".seg";

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

// A disk log file.
pub struct Segment {
    pub(crate) id: u32,
    file: std::sync::RwLock<std::fs::File>,
    pub(crate) current_block_number: u32,
    pub(crate) current_block_size: u32,
    file_path: std::path::PathBuf,
}

#[derive(Debug)]
pub struct ChunkPosition {
    pub segment_id: u32,
    pub block_number: u32,
    pub chunk_offset: u64,
}

impl Segment {
    pub fn open(dir_path: impl AsRef<Path>, id: u32) -> Result<Self, WalError> {
        let file_name = format!("{:09}{}", id, SEGMENT_FILE_SUFFIX);
        let file_name = dir_path.as_ref().join(file_name);
        let file = std::fs::File::options()
            .read(true)
            .create(true)
            .append(true)
            .open(&file_name)?;
        // Set file mod.
        let mut perm = std::fs::metadata(&file_name)?.permissions();
        perm.set_mode(FILE_MODE_PERM);
        std::fs::set_permissions(&file_name, perm)?;
        Ok(Self {
            id,
            file: std::sync::RwLock::new(file),
            current_block_number: 0,
            current_block_size: 0,
            file_path: file_name,
        })
    }

    pub fn sync(&self) -> Result<(), WalError> {
        let file = self.file.write().unwrap();
        file.sync_all()?;
        Ok(())
    }

    /// Remove log file from disk.
    pub fn remove(&self) -> Result<(), WalError> {
        std::fs::remove_file(&self.file_path)?;
        Ok(())
    }

    pub fn size(&self) -> u64 {
        (self.current_block_number * BLOCK_SIZE + self.current_block_size) as u64
    }

    pub fn write(&mut self, data: Vec<u8>) -> Result<ChunkPosition, WalError> {
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
        let position = ChunkPosition {
            segment_id: self.id,
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
            // Calculate how much can fit in this block.(Each chunk has a header)
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

    /// Write a chunk data to file
    fn write_internal(
        &mut self,
        chunk_data: Vec<u8>,
        chunk_type: ChunkType,
    ) -> Result<(), WalError> {
        let data_size = chunk_data.len();
        let mut buf = vec![0; data_size + CHUNK_HEADER_SIZE as usize];
        // Length: 2 Bytes, index:4-5
        buf[4..6].copy_from_slice(&(data_size as u16).to_le_bytes());
        // Type: 1 Byte, index:6
        buf[6] = chunk_type.into();
        // Data: N Bytes, index:7-end
        buf[7..].copy_from_slice(&chunk_data);
        // Checksum: 4 Bytes, index:0-3
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf[4..]);
        let sum = hasher.finalize();
        buf[0..4].copy_from_slice(&sum.to_le_bytes());
        // Append to the file
        // dbg!("begin write chunk to file");
        let mut file = self.file.write().unwrap();
        match file.write(&buf) {
            Ok(_) => dbg!("write successful"),
            Err(e) => dbg!(&format!("write failed: {:?}", e)),
        };
        // dbg!("end write chunk to file");
        drop(file);
        if self.current_block_size > BLOCK_SIZE {
            panic!("Wrong! Can not exceed the block size");
        }
        // Update the corresponding fields
        self.current_block_size += buf.len() as u32;
        // A new block
        if self.current_block_size == BLOCK_SIZE {
            self.current_block_number += 1;
            self.current_block_size = 0;
        }
        Ok(())
    }

    pub fn read(&self, mut block_number: u32, mut chunk_offset: u64) -> Result<Vec<u8>, WalError> {
        let file = self.file.read().unwrap();
        let stat = file.metadata()?;
        let seg_size = stat.len();
        let mut result = Vec::new();
        loop {
            // The size of current block.
            let mut size = BLOCK_SIZE as u64;
            // The start position of the block in the file.
            let offset = (block_number * (BLOCK_SIZE as u32)) as u64;
            // Deal with the last situation.
            if offset + size > seg_size as u64 {
                size = seg_size - offset;
            }
            let mut buf = vec![0; size as usize];
            file.read_exact_at(&mut buf, offset)?;
            // file.read_at(&mut buf, offset)?;
            // dbg!(buf.len());
            // dbg!(block_number, chunk_offset);

            // Header part
            let mut header = vec![0; CHUNK_HEADER_SIZE as usize];
            header.copy_from_slice(
                &buf[chunk_offset as usize..(chunk_offset as usize + CHUNK_HEADER_SIZE as usize)],
            );
            // TODO: checksum

            // Length
            let length = u16::from_le_bytes(header[4..6].try_into().unwrap()) as usize;
            dbg!(length);

            // Copy data
            let start = chunk_offset as usize + CHUNK_HEADER_SIZE as usize;
            result.extend_from_slice(&buf[start..start + length]);

            // Type
            let chunk_type: ChunkType = header[6].into();
            if chunk_type == ChunkType::Full || chunk_type == ChunkType::Last {
                break;
            }
            block_number += 1;
            chunk_offset = 0;
        }
        Ok(result)
    }

    pub fn metadata(&self) -> Result<std::fs::Metadata, WalError> {
        Ok(self.file.read().unwrap().metadata()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn wal_write() {
    //     let path: std::path::PathBuf = std::path::PathBuf::from("/tmp/000001.log");
    //     let mut wal = Segment::open(&path).unwrap();

    //     let s = "A".repeat(2028);
    //     wal.write(s.into_bytes()).unwrap();

    //     let s = "A".repeat(30 * 1024);
    //     wal.write(s.into_bytes()).unwrap();

    //     let s = "A".to_string().into_bytes();
    //     wal.write(s).unwrap();

    //     let s = "A".repeat(33 * 1024);
    //     wal.write(s.into_bytes()).unwrap();

    //     let s = "A".repeat(66 * 1024);
    //     wal.write(s.into_bytes()).unwrap();

    //     dbg!(wal.current_block_size);
    //     // std::fs::remove_file(path).unwrap();
    // }

    // #[test]
    // fn wal_read() {
    //     let path: std::path::PathBuf = std::path::PathBuf::from("/tmp/000001.log");
    //     let mut wal = Segment::open(&path).unwrap();
    //     // One block
    //     let s = "A".repeat(2028);
    //     let pos = wal.write(s.into_bytes()).unwrap();
    //     wal.read(pos.block_number, pos.chunk_offset).unwrap();

    //     // Multiple blocks
    //     let s = "A".repeat(45 * 1024);
    //     let pos = wal.write(s.into_bytes()).unwrap();
    //     wal.read(pos.block_number, pos.chunk_offset).unwrap();
    // }
}
