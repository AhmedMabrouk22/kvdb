package memTable;

import java.util.Map;
import java.util.TreeMap;

public class MemTable {
    private final TreeMap<String,String> tree;
    public MemTable() {
        this.tree = new TreeMap<>();
    }

    public MemTable(TreeMap<String, String> map) {
        this.tree = new TreeMap<>(map);
    }

    /**
     * Put key and value in MemTable
     * @param key
     * @param value
     */
    public void put(String key, String value) {
        tree.put(key,value);
    }

    /**
     * Delete Key from MemTable
     * It add Tombstone (null) to the key
     * @param key
     */
    public void delete(String key) {
        tree.put(key,null);
    }

    /**
     * Get Key and value from MemTable
     * @param key
     * @return
     */
    public String get(String key) {
        return tree.get(key);
    }

    /**
     * Check if key is exist or not
     * @param key
     * @return
     */
    public boolean contains(String key) {
        return tree.containsKey(key);
    }

    public String getMinKey() {
        return tree.isEmpty() ? null : tree.firstKey();
    }

    public String getMaxKey() {
        return tree.isEmpty() ? null : tree.lastKey();
    }

    public int size() {
        return tree.size();
    }

    public void clear() {
        tree.clear();
    }

    public Map<String, String>  getAll() {
        return new TreeMap<>(tree);
    }

}
