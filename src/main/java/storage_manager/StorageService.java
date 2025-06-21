package storage_manager;

import bloom_filters.BloomFilter;
import memTable.MemTable;
import ssTable.SSTable;
import ssTable.SSTableService;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;



public class StorageService {
    private final int MAX_MEMTABLE_SIZE;
    private final String SSTABLE_NAME;
    private final String LEVEL_NAME;
    private final File DIR;
    private final SSTableService SSTABLE_SERVICE;
    private final int MAX_SSTABLES_PER_LEVEL;
    private final LevelCompactor LEVEL_COMPACTOR;

    public StorageService(int maxMemTableSize, String SSTABLE_NAME, String levelName, File dir) {
        this.MAX_MEMTABLE_SIZE = maxMemTableSize;
        this.SSTABLE_NAME = SSTABLE_NAME;
        this.LEVEL_NAME = levelName;
        this.DIR = dir;
        this.SSTABLE_SERVICE = new SSTableService();
        this.MAX_SSTABLES_PER_LEVEL = 3;
        this.LEVEL_COMPACTOR = new LevelCompactor(MAX_SSTABLES_PER_LEVEL,SSTABLE_NAME,SSTABLE_SERVICE,LEVEL_NAME,DIR);
        LEVEL_COMPACTOR.createLevel(0);
    }

    public int getMAX_MEMTABLE_SIZE() {
        return MAX_MEMTABLE_SIZE;
    }

    public String getSSTABLE_NAME() {
        return SSTABLE_NAME;
    }

    public String getLEVEL_NAME() {
        return LEVEL_NAME;
    }

    public File getDIR() {
        return DIR;
    }



    public SSTableService getSSTABLE_SERVICE() {
        return SSTABLE_SERVICE;
    }



    /**
     * Flush MemTable data into new SSTable file in level 0
     * Before Flush data first Check if level exceeds the max number of ssTable or not
     * If exceeds compact and merge sstables
     * @param memTable
     * @throws IOException
     */
    public void flush(MemTable memTable) throws IOException {

        File[] levels = DIR.listFiles((file, name) -> name.startsWith(LEVEL_NAME));
        if (levels == null) {
            throw new RuntimeException("Error: level 0 not created, something went wrong");
        }
        Arrays.sort(levels, Comparator.comparing(File::getName));

        LEVEL_COMPACTOR.checkAndCompactLevels(levels);

        int ssTableSerial = SSTABLE_SERVICE.getNextFileSerial(levels[0]);
        SSTABLE_SERVICE.writeSSTable(memTable,new SSTable(levels[0].getPath(),ssTableSerial));
        BloomFilter filter = new BloomFilter(memTable.size());
        memTable.getAll().forEach((key,value) -> {
            filter.add(key);
        });
        filter.save(levels[0]);
    }

    /**
     * Search about key in SSTables
     * For each level check bloom filter first and if the value maybe exist scan SSTables in this level
     * @param key
     * @return
     * @throws IOException
     */
    public String search(String key) throws IOException, ClassNotFoundException {

        File[] levels = DIR.listFiles((file, name) -> name.startsWith(LEVEL_NAME));
        if (levels == null) {
            throw new RuntimeException("Error: level 0 not created, something went wrong");
        }
        Arrays.sort(levels, Comparator.comparing(File::getName));

        for(File level : levels) {
            BloomFilter filter = BloomFilter.load(level);
            String value = null;
            if (filter != null && filter.mightContain(key)) {
                value = SSTABLE_SERVICE.search(key, level);
            }
            if (value != null) return value;
        }

        return null;
    }



}
