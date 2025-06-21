package utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

public class Utils {
    public static void printError(String message) {
        System.err.println("Error: " + message);
    }

    public static byte[] getStringBytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8); // to make it compatible with all unicode characters
    }

    public static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] data = getStringBytes(value);
        out.writeInt(data.length);
        out.write(data);
    }

    public static String readString(DataInputStream input) throws IOException {
        int len = input.readInt();
        byte[] data = new byte[len];
        input.readFully(data);
        return new String(data, StandardCharsets.UTF_8);
    }

    public static File join(String first, String... others) {
        return Paths.get(first, others).toFile();
    }

    public static File join(File first, String... others) {
        return Paths.get(first.getPath(), others).toFile();
    }

}
