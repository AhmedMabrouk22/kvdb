package ssTable;

import utils.Utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Metadata {
    private String minKey;
    private String maxKey;
    private int numberOfRecords;


    public Metadata() {}

    public Metadata(String minKey, String maxKey, int numberOfRecords) {
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.numberOfRecords = numberOfRecords;
    }

    public String getMinKey() {
        return minKey;
    }

    public void setMinKey(String minKey) {
        this.minKey = minKey;
    }

    public String getMaxKey() {
        return maxKey;
    }

    public void setMaxKey(String maxKey) {
        this.maxKey = maxKey;
    }

    public int getNumberOfRecords() {
        return numberOfRecords;
    }

    public void setNumberOfRecords(int numberOfRecords) {
        this.numberOfRecords = numberOfRecords;
    }

    public void writeMetadata(DataOutputStream out) throws IOException {
        Utils.writeString(out,minKey);
        Utils.writeString(out,maxKey);
        out.writeInt(numberOfRecords);
    }

    public void readMetadata(DataInputStream in) throws IOException {
        this.minKey = Utils.readString(in);
        this.maxKey = Utils.readString(in);
        this.numberOfRecords = in.readInt();
    }


    @Override
    public String toString() {
        return "Metadata{" +
                "minKey='" + minKey + '\'' +
                ", maxKey='" + maxKey + '\'' +
                ", numberOfRecords=" + numberOfRecords +
                '}';
    }
}
