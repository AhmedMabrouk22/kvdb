package wal;

import memTable.MemTable;
import utils.Utils;

import java.io.*;

public class WALService {

    private final File WAL_FILE;
    private final DataOutputStream WRITER;

    public WALService(File walFile) throws IOException {
        WAL_FILE = walFile;
        if (!WAL_FILE.exists())
            WAL_FILE.createNewFile();

        WRITER = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(WAL_FILE,true)));
    }


    /**
     * Log put operation in Write-Ahead Log File
     * Log with format [operation_type][key length][key][value length][value]
     * If operation is (put) the operation_type = 1
     * @param key
     * @param value
     * @throws IOException
     */
    public void logPut(String key,String value) throws IOException {
        // log format -> [operation_type][key len][key][value len][value]
        WRITER.writeByte(1); // 1 -> put operation
        Utils.writeString(WRITER,key);
        Utils.writeString(WRITER,value);
        WRITER.flush();
    }

    /**
     * Log delete operation in Write-Ahead Log File
     * Log with format [operation_type][key length][key]
     * If operation is (delete) the operation_type = 0
     * @param key
     * @throws IOException
     */
    public void logDelete(String key) throws IOException {
        WRITER.writeByte(0); // 0 -> delete operation
        Utils.writeString(WRITER,key);
        WRITER.flush();
    }

    /**
     * Rebuild the MemTable when application start or crash
     * @param memTable
     * @throws IOException
     */
    public void build(MemTable memTable) {
        try (DataInputStream input = new DataInputStream(new FileInputStream(WAL_FILE))) {
            while (input.available() > 0) {
                byte operation = input.readByte();
                String key = Utils.readString(input);
                if (operation == 1) {
                    String value = Utils.readString(input);
                    memTable.put(key, value);
                    System.out.printf("[WARNING] WAL set %s%n",
                            key);
                } else {
                    System.out.printf("[WARNING] WAL delete %s%n",
                            key);
                    memTable.delete(key);
                }
            }
        } catch (IOException e) {
            System.err.println("Warning: WAL file ended unexpectedly. Some entries may be incomplete.");
        }
    }

    /**
     * Delete Write-Ahead Log file
     */
    public void reset() throws IOException {
        try(RandomAccessFile raf = new RandomAccessFile(WAL_FILE,"rw")) {
            raf.setLength(0);
        }
    }

    public void close() throws IOException {
        WRITER.close();
    }


}
