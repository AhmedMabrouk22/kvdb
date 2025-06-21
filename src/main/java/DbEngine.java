import memTable.MemTable;
import storage_manager.StorageService;
import utils.Utils;
import wal.WALService;

import java.io.File;
import java.io.IOException;

public class DbEngine {

    private final WALService walService;
    private final MemTable memTable;
    private final StorageService storageService;

    private final File DATA_DIR;
    private final File WAL_FILE;

    public DbEngine() throws IOException {
        this.DATA_DIR = new File("data");
        if(!DATA_DIR.exists()) {
            DATA_DIR.mkdir();
        }

        this.WAL_FILE = new File(DATA_DIR,"wal.log");
        walService = new WALService(WAL_FILE);
        memTable = new MemTable();
        walService.build(memTable);

        this.storageService = new StorageService(5,"sstable_","level_",DATA_DIR);

    }

    /**
     * Add key and value in db
     * First put data into write-ahead log (WAL) and then put it in MemTable
     * If MemTable exceeds size, flush data into SSTable, then clear MemTable and WAL
     * @param key
     * @param value
     * @throws IOException
     */
    public void put(String key, String value) throws IOException {
        walService.logPut(key,value);
        memTable.put(key,value);

        if (memTable.size() >= storageService.getMAX_MEMTABLE_SIZE()) {
            storageService.flush(memTable);
            walService.reset();
            memTable.clear();
            System.out.printf("[WARNING] Flush data into sstable%n");
        }

    }

    /**
     * Get value
     * First check value is existing in MemTable or not
     * If not search on SSTables
     * @param key
     */
    public String get(String key) throws IOException, ClassNotFoundException {
        String value = null;
        if (memTable.contains(key))
            value = memTable.get(key);
        else
            value = storageService.search(key);

        return value;
    }

    /**
     * Delete key from db
     * First log delete operation in Write-Ahade Log, Then add tombstone in MemTable
     * @param key
     */
    public void delete(String key) throws IOException {
        walService.logDelete(key);
        memTable.delete(key);
    }

}
