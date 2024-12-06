# wal-rs
Write Ahead Log for LSM or bitcask storage.

## Format

**Format of the WAL file:**
```
       +-----+-------------+--+----+----------+------+-- ... ----+
 File  | r0  |        r1    |P | r2 |    r3     |  r4   |            |
       +-----+-------------+--+----+----------+------+-- ... ----+
       <--- BlockSize ------->|<--- BlockSize ------>|
  rn = variable size records(Chunk)
  P = Padding
  BlockSize = 32KB
```

**Format of a single record:**
```
+---------+-------------+-----------+--- ... ---+
| CRC (4B)| Length (2B) | Type (1B)  | Payload   |
+---------+-------------+-----------+--- ... ---+
CRC = 32bit hash computed over the payload using CRC
Length = Length of the payload data
Type = Type of record
       (FullType, FirstType, MiddleType, LastType)
       The type is used to group a bunch of records together to represent
       blocks that are larger than BlockSize
Payload = Byte stream as long as specified by the payload size
```

## Getting Started

```Rust
use wal_rs::wal::{ChunkStartPosition, Wal};
fn main() {
    let path: std::path::PathBuf = std::path::PathBuf::from("/tmp/000001.log");
    let mut wal = Wal::open(&path).unwrap();
    // One block
    let s = "A".repeat(2028);
    let pos = wal.write(s.into_bytes()).unwrap();
    wal.read(pos.block_number, pos.chunk_offset).unwrap();
}

```