use wal_rs::wal::{ChunkStartPosition, Wal};
fn main() {
    let path: std::path::PathBuf = std::path::PathBuf::from("/tmp/000001.log");
    let mut wal = Wal::open(&path).unwrap();
    // One block
    let s = "A".repeat(2028);
    let pos = wal.write(s.into_bytes()).unwrap();
    wal.read(pos.block_number, pos.chunk_offset).unwrap();
}
