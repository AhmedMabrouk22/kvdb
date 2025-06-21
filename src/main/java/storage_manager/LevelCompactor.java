package storage_manager;

import bloom_filters.BloomFilter;
import memTable.MemTable;
import ssTable.KVPair;
import ssTable.Metadata;
import ssTable.SSTable;
import ssTable.SSTableService;

import java.io.*;
import java.util.*;

public class LevelCompactor {

    private final int MAX_SSTABLES_PER_LEVEL;
    private final String SSTABLE_NAME;
    private final SSTableService SSTABLE_SERVICE;
    private final String LEVEL_NAME;
    private final File DIR;

    public LevelCompactor(int MAX_SSTABLES_PER_LEVEL, String sstableName, SSTableService sstableService, String levelName, File dir) {
        this.MAX_SSTABLES_PER_LEVEL = MAX_SSTABLES_PER_LEVEL;
        SSTABLE_NAME = sstableName;
        SSTABLE_SERVICE = sstableService;
        LEVEL_NAME = levelName;
        DIR = dir;
    }

    /**
     * Create new level in storage
     */
    public File createLevel(int levelNumber) {
        String levelName = this.LEVEL_NAME + levelNumber;
        File level = new File(DIR,levelName);
        if (!level.exists()) {
            level.mkdir();
        }
        return level;
    }

    /**
     * Check if level exceeds the max sstables per level or not
     * If yes compact and merge this level
     * @param levels
     * @throws IOException
     */
    public void checkAndCompactLevels(File[] levels) throws IOException {
        for(int i = 0 ; i < levels.length; ++i) {
            if (levels[i] != null && levels[i].listFiles().length == MAX_SSTABLES_PER_LEVEL) {
                mergeSSTablesAtLevel(i,levels[i]);
            }
        }
    }

    /**
     * Merge SStables at level
     * @param levelNumber
     * @param level
     * @throws IOException
     */

    private void mergeSSTablesAtLevel(int levelNumber, File level) throws IOException {
        File[] ssTables = level.listFiles((file,name) -> name.startsWith(SSTABLE_NAME));
        if (ssTables == null || ssTables.length <= 1) return;
        Arrays.sort(ssTables, Comparator.comparing(File::getName).reversed());

        TreeMap<String, String> mergedData = new TreeMap<>();
        for(File file : ssTables) {
            try (DataInputStream input =
                         new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
                Metadata metadata = new Metadata();
                metadata.readMetadata(input);
                for(int i = 0 ; i < metadata.getNumberOfRecords(); ++i) {
                    KVPair kv = new KVPair();
                    kv.readKvPair(input);
                    if (!mergedData.containsKey(kv.getKey())) { // the newest data put first
                        mergedData.put(kv.getKey(),kv.getValue());
                    }
                }
            }
        }

        // remove tombstone keys
        mergedData.entrySet().removeIf(v -> v.getValue().isEmpty());

        int nextLevel = levelNumber + 1;
        File nextLevelDir = createLevel(nextLevel);


        int ssTableSerial = SSTABLE_SERVICE.getNextFileSerial(nextLevelDir);
        SSTable newSSTable = new SSTable(nextLevelDir.getPath(),ssTableSerial);
        SSTABLE_SERVICE.writeSSTable(new MemTable(mergedData),newSSTable);

        BloomFilter filter = new BloomFilter(mergedData.size());
        mergedData.forEach((key,value) -> {
            filter.add(key);
        });
        filter.save(nextLevelDir);

        for (File file : ssTables)
            file.delete();


        System.out.println("[WARNING] Compaction complete: Merged into " + newSSTable.getSsTableFile().getName() + " in level " + nextLevel);

    }

}
