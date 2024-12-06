use std::{collections::HashMap, rc::Rc, sync::Mutex};

use crate::{
    error::WalError,
    options::Options,
    segment::{self, ChunkPosition, Segment, SEGMENT_FILE_SUFFIX},
};

const INITIAL_SEGMENT_FILE_ID: u32 = 1;

pub struct Wal {
    active_segment: Rc<Mutex<Option<Segment>>>,
    older_segments: HashMap<u32, Rc<Segment>>,
}

impl Wal {
    pub fn open(options: Options) -> Result<Self, WalError> {
        // Iterate the dir and open all segment file.
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
            let seg = segment::Segment::open(&options.dir_path, INITIAL_SEGMENT_FILE_ID)?;
            return Ok(Self {
                active_segment: Rc::new(Mutex::new(Some(seg))),
                older_segments: HashMap::new(),
            });
        }
        // Open the segment file in order, get the max one as the active segment file.
        let len = segment_ids.len();
        let mut active_segment = None;
        let mut older_segments = HashMap::new();
        segment_ids.sort();

        for (i, seg_id) in segment_ids.into_iter().enumerate() {
            let seg = segment::Segment::open(&options.dir_path, seg_id)?;
            if i == len - 1 {
                active_segment = Some(seg);
            } else {
                older_segments.insert(seg_id, Rc::new(seg));
            }
        }
        Ok(Self {
            active_segment: Rc::new(Mutex::new(active_segment)),
            older_segments,
        })
    }

    pub fn write(&self, data: &[u8]) -> Result<ChunkPosition, WalError> {
        todo!()
    }

    pub fn read(&self, pos: ChunkPosition) -> Result<Vec<u8>, WalError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work() {
        let name = "000900101.wal".to_string();
    }
}
