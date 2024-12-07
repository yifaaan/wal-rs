use std::{
    collections::HashMap,
    rc::Rc,
    sync::{Mutex, RwLock},
};

use crate::{
    error::WalError,
    options::Options,
    segment::{self, ChunkPosition, Segment, BLOCK_SIZE, CHUNK_HEADER_SIZE, SEGMENT_FILE_SUFFIX},
};

const INITIAL_SEGMENT_FILE_ID: u32 = 1;

pub struct Wal {
    active_segment: Rc<RwLock<Option<Segment>>>,
    older_segments: HashMap<u32, Rc<Segment>>,
    options: Options,
}

impl Wal {
    pub fn open(options: Options) -> Result<Self, WalError> {
        // Create the directory if not exists.
        std::fs::create_dir(&options.dir_path)?;
        // Get all segment file id.
        let mut segment_ids = Vec::new();
        for entry in std::fs::read_dir(&options.dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                continue;
            }
            let file_name = match entry.file_name().into_string() {
                Ok(s) => s,
                Err(_) => continue,
            };
            let id: u32 = file_name[0..file_name.find(SEGMENT_FILE_SUFFIX).unwrap()].parse()?;
            segment_ids.push(id);
        }
        // Empty directory, just initialize a new segment file and return.
        if segment_ids.is_empty() {
            let mut seg = segment::Segment::open(&options.dir_path, INITIAL_SEGMENT_FILE_ID)?;
            let offset = seg.metadata()?.len();
            seg.current_block_number = (offset / BLOCK_SIZE as u64) as u32;
            seg.current_block_size = (offset % BLOCK_SIZE as u64) as u32;
            return Ok(Self {
                active_segment: Rc::new(RwLock::new(Some(seg))),
                older_segments: HashMap::new(),
                options,
            });
        } else {
            // Open the segment file in order, get the max one as the active segment file.
            let len = segment_ids.len();
            let mut active_segment = None;
            let mut older_segments = HashMap::new();
            segment_ids.sort();

            for (i, seg_id) in segment_ids.into_iter().enumerate() {
                let mut seg = segment::Segment::open(&options.dir_path, seg_id)?;
                if i == len - 1 {
                    let offset = seg.metadata()?.len();
                    seg.current_block_number = (offset / BLOCK_SIZE as u64) as u32;
                    seg.current_block_size = (offset % BLOCK_SIZE as u64) as u32;
                    active_segment = Some(seg);
                } else {
                    older_segments.insert(seg_id, Rc::new(seg));
                }
            }

            Ok(Self {
                active_segment: Rc::new(RwLock::new(active_segment)),
                older_segments,
                options: options,
            })
        }
    }

    pub fn write(&mut self, data: &[u8]) -> Result<ChunkPosition, WalError> {
        let mut active_seg = self.active_segment.write().unwrap();
        let mut active_seg = active_seg.as_mut().unwrap();
        let id = active_seg.id;
        // If the active segment file is full, close it and create a new one.
        if self.is_full(data.len() as u64) {
            let mut seg = Segment::open(&self.options.dir_path, id + 1)?;
            self.older_segments
                .insert(id, std::rc::Rc::new(std::mem::replace(active_seg, seg)));
        }
        active_seg.write(data.to_vec())
    }

    pub fn read(&self, pos: ChunkPosition) -> Result<Vec<u8>, WalError> {
        let active_seg = self.active_segment.read().unwrap();
        let active_seg = active_seg.as_ref();
        // Find the segment file according to the position
        let seg;
        if pos.segment_id == active_seg.unwrap().id {
            seg = active_seg;
        } else {
            seg = self
                .older_segments
                .get(&pos.segment_id)
                .map(|seg| seg.as_ref());
        }

        if seg.is_none() {
            Err(WalError::SegmentFileNotFound)
        } else {
            seg.unwrap().read(pos.block_number, pos.chunk_offset)
        }
    }

    pub fn is_full(&self, delta: u64) -> bool {
        let seg = self.active_segment.read().unwrap();
        seg.as_ref().unwrap().size() + delta + CHUNK_HEADER_SIZE as u64 > self.options.segment_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work() {
        let name = "000900101.wal".to_string();
        let opts = Options {
            dir_path: "/tmp/wal".into(),
            segment_size: 1024 * 1024 * 1024,
        };
        let mut wal = Wal::open(opts).unwrap();
        let pos = wal.write("amazing lyf is better".as_bytes());
    }
}
