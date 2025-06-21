package ssTable;

import java.io.*;

public class SSTable {
    private final File ssTableFile;

    public SSTable(String dirPath, int serial) throws IOException {
        String fileName = String.format("sstable_%d.db",serial);
        this.ssTableFile = new File(dirPath,fileName);
        this.ssTableFile.createNewFile();
    }

    public File getSsTableFile() {
        return ssTableFile;
    }



}
