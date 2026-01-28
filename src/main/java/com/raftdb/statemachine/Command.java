package com.raftdb.statemachine;

import com.raftdb.rpc.proto.OperationType;

import java.io.*;
import java.util.Arrays;

/**
 * A command to be applied to the state machine.
 * Represents a KV operation (GET, PUT, DELETE).
 */
public record Command(
        OperationType type,
        byte[] key,
        byte[] value  // null for GET and DELETE
) {

    public static Command get(byte[] key) {
        return new Command(OperationType.GET, key, null);
    }

    public static Command put(byte[] key, byte[] value) {
        return new Command(OperationType.PUT, key, value);
    }

    public static Command delete(byte[] key) {
        return new Command(OperationType.DELETE, key, null);
    }

    // String convenience methods
    public static Command get(String key) {
        return get(key.getBytes());
    }

    public static Command put(String key, String value) {
        return put(key.getBytes(), value.getBytes());
    }

    public static Command delete(String key) {
        return delete(key.getBytes());
    }

    /**
     * Serialize command to bytes for log storage.
     */
    public byte[] toBytes() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(bos)) {

            dos.writeInt(type.getNumber());
            dos.writeInt(key.length);
            dos.write(key);

            if (value != null) {
                dos.writeInt(value.length);
                dos.write(value);
            } else {
                dos.writeInt(-1);
            }

            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize command", e);
        }
    }

    /**
     * Deserialize command from bytes.
     */
    public static Command fromBytes(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             DataInputStream dis = new DataInputStream(bis)) {

            OperationType type = OperationType.forNumber(dis.readInt());

            int keyLen = dis.readInt();
            byte[] key = new byte[keyLen];
            dis.readFully(key);

            int valueLen = dis.readInt();
            byte[] value = null;
            if (valueLen >= 0) {
                value = new byte[valueLen];
                dis.readFully(value);
            }

            return new Command(type, key, value);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize command", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Command command = (Command) o;
        return type == command.type
                && Arrays.equals(key, command.key)
                && Arrays.equals(value, command.value);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        String keyStr = new String(key);
        if (type == OperationType.PUT) {
            return String.format("PUT(%s, %s)", keyStr, new String(value));
        }
        return String.format("%s(%s)", type, keyStr);
    }
}
