package ssTable;

import memTable.MemTable;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SSTableService {

    public SSTableService() {
    }

    /**
     * Flush MemTable into SSTable file and store it in disk
     * The SSTable file contains metadata block(minKey,maxKey,numberOfRecords) and data block (K,V) pairs
     * @param memTable
     * @param ssTable
     * @throws IOException
     */
    public void writeSSTable(MemTable memTable,SSTable ssTable) throws IOException {
        try (DataOutputStream out =
                     new DataOutputStream(new BufferedOutputStream(new FileOutputStream(ssTable.getSsTableFile())))) {
            Metadata metadata = new Metadata(memTable.getMinKey(), memTable.getMaxKey(), memTable.size());
            metadata.writeMetadata(out);
            for (Map.Entry<String, String> entry : memTable.getAll().entrySet()) {
                KVPair kv = new KVPair(entry.getKey(), entry.getValue());
                kv.writeKvPair(out);
            }
        }
    }

    /**
     * Get the next serial number for SSTable files
     * @return
     */
    public int getNextFileSerial(File levelDir) {
        int maxSerial = 0;
        Pattern pattern = Pattern.compile("sstable_(\\d+)\\.db");
        for (File file : levelDir.listFiles()) {
            Matcher matcher = pattern.matcher(file.getName());
            if (matcher.matches()) {
                int serial = Integer.parseInt(matcher.group(1));
                if (serial > maxSerial) maxSerial = serial;
            }
        }
        return maxSerial + 1;
    }

    /**
     * Search about key in SSTable files
     * Scan from newest to oldest
     * @param key
     * @return
     * @throws IOException
     */
    public String search(String key, File levelDir) throws IOException {
        if (!levelDir.exists() || !levelDir.isDirectory()) {
            throw new IOException("Invalid SSTable directory");
        }

        // Get all SSTable files
        File[] ssTableFiles = getAllSStables(levelDir);
        if (ssTableFiles == null) return null;

        Arrays.sort(ssTableFiles,Comparator.comparing(File::getName).reversed());

        for(File file : ssTableFiles) {
            try (DataInputStream input = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(file)))) {
                Metadata metadata = new Metadata();
                metadata.readMetadata(input);
                if (key.compareTo(metadata.getMinKey()) < 0 || key.compareTo(metadata.getMaxKey()) > 0)
                    continue;

                for(int i = 0 ; i < metadata.getNumberOfRecords(); ++i) {
                    KVPair kv = new KVPair();
                    kv.readKvPair(input);
                    if (key.equals(kv.getKey()))
                        return kv.getValue();
                }
            }
        }

        return null;
    }



    private File[] getAllSStables(File dir) {
        File[] ssTableFiles = dir.listFiles((d,name) -> name.startsWith("sstable_"));
        if (ssTableFiles == null || ssTableFiles.length == 0) return null;
        return ssTableFiles;
    }

}
