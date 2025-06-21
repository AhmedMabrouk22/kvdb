package ssTable;

import utils.Utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class KVPair {
    private String key;
    private String value;

    public KVPair() {}

    public KVPair(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void writeKvPair(DataOutputStream out) throws IOException {
        Utils.writeString(out,key);
        if (value != null)
            Utils.writeString(out,value);
        else
            Utils.writeString(out,""); // tombstone for deleting
    }

    public void readKvPair(DataInputStream in) throws IOException {
        this.key = Utils.readString(in);
        this.value = Utils.readString(in);
    }


    @Override
    public String toString() {
        return "KVPair{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
