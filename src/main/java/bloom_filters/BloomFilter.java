package bloom_filters;

import java.io.*;
import java.util.BitSet;

public class BloomFilter implements Serializable{
    private BitSet bitSet;
    private final int bitSetSize;

    public BloomFilter(int bitSetSize) {
        this.bitSetSize = bitSetSize;
        this.bitSet = new BitSet(bitSetSize);
    }

    /**
     * Add key in bloom filter
     * @param key
     */
    public void add(String key) {
        int h1 = hash1(key);
        int h2 = hash2(key);
        bitSet.set(h1,true);
        bitSet.set(h2,true);
    }

    /**
     * Check if bloom filter might contain key or not
     * @param key
     * @return
     */
    public boolean mightContain(String key) {
        int h1 = hash1(key);
        int h2 = hash2(key);
        if (bitSet.get(h1) && bitSet.get(h2)) return true;
        return false;
    }

    private int hash1(String key) {
        int hash = 0;
        for(int i = 0 ; i < key.length(); ++i) {
            hash = hash + (int) key.charAt(i);
            hash %= bitSetSize;
        }

        return hash;
    }

    private int hash2(String key) {
        int hash = 0;
        for(int i = 0 ; i < key.length(); ++i) {
            hash = hash + 31 + (int) key.charAt(i);
            hash %= bitSetSize;
        }
        return hash;
    }


    /**
     * Save and write bloom filter in disk
     * @param level
     * @throws IOException
     */
    public void save(File level) throws IOException {

        File file = new File(level,"bloom.filter");
        if (!file.exists()) {
            file.createNewFile();
        }

        try (ObjectOutputStream output = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
            output.writeInt(bitSetSize);
            output.writeObject(bitSet);
        }
    }

    /**
     * Load bloom filter from disk
     * @param level
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static BloomFilter load(File level) throws IOException, ClassNotFoundException {
        File file = new File(level,"bloom.filter");
        if (!file.exists()) {
            return null;
        }
        try (ObjectInputStream input = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)))){
            int size = input.readInt();
            BitSet bitSet = (BitSet) input.readObject();
            BloomFilter bloomFilter = new BloomFilter(size);
            bloomFilter.bitSet = bitSet;
            return bloomFilter;
        }
    }

}
