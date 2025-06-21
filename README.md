# Key-Value Database

## 📘 Overview 
This is a Lightweight Key-Value Database based on the **LSM-tree (Log-Structured Merge Tree)** architecture. It is a simplified storage engine built in Java for educational purposes, designed to simulate how modern databases like LevelDB, RocksDB, or Cassandra store and retrieve data efficiently at scale, featuring:
- Write-Ahead Logging (WAL)
- In-memory MemTable
- Persistent SSTables with Levels
- Compaction/Merging
- Bloom Filters (per level)

## ✨ Features
this project offers a simplified set of features inspired by modern NoSQL storage engines. Each component plays a role in achieving durability, performance, and scalability.

#### 1. Write-Ahead Log (WAL)
   - **Durability Guarantee**: Every write operation is first appended to a WAL file on disk before being added to memory.
   - **Crash Recovery**: On restart, the system replays the WAL to rebuild the MemTable, ensuring no data is lost.
   - **Binary Format**: Operations are serialized in a compact binary format:
     `[operation_type][key_length][key][value_length][value]`

#### 2. MemTable (In-Memory Store)
- **Sorted Data**: Implemented as a TreeMap<String, String> for sorted keys.
- **Fast Access:** Read/write/delete operations are served directly from memory.
- **Tombstones**: Delete operations are stored as tombstones (null values) and processed during compaction.
- **Auto Flush**: When the MemTable exceeds a configurable size, it is flushed to disk as an SSTable.

#### 3. SSTables (Sorted String Tables)
- **Immutable and Persistent**: SSTables are immutable files stored on disk containing sorted key-value pairs.
- **Efficient Search**: Metadata (`minKey`, `maxKey`, `recordCount`) is stored for quick range checks before scanning.
- **Levels**: SSTables are organized into multiple levels (`level_0`, `level_1`, etc.).

#### 4. Compaction
- **Duplicate Resolution**: Older keys are discarded, keeping only the most recent versions.
- **Tombstone Cleanup**: Deleted keys (tombstones) are removed during compaction.
- **Level Merging**: Multiple SSTables in a level are merged and written to the next level in sorted order.

#### 5. Bloom Filters
- **Fast Negative Lookup**: Avoid unnecessary disk I/O by checking if a key might exist in an SSTable.
- **Per-Level Filtering**: One Bloom filter per level tracks all keys in that level.


## 🏗️ Structure
![Image](https://github.com/user-attachments/assets/feeec074-462b-4ad1-a2a3-2dd2a0ca1f6d)

## ⚙️ How It Works
- **✅ Write (PUT)**
  - Write to WAL (disk)
  - Add to MemTable (RAM)
  - If MemTable exceeds threshold, flush to SSTable and clear WAL.
- **🔍 Read (GET)**
  - Search MemTable
  - If not found, check Bloom filters (per level)
  - If possible match, search SSTables using metadata + scan
- **❌ Delete**
  - Add tombstone in MemTable
  - WAL logs to delete
  - Compaction removes tombstone keys from disk

## 🧪 Testing REST API Examples
- **POST** : put (key,value)
  - `curl -X POST "http://localhost:7070/?key=user1&value=Ahmed"`
- **Get** : get value by key
  - `curl -X GET "http://localhost:7070/user1"`
- **Delete** : delete key
  - `curl -X DELETE "http://localhost:7070/test2"`


