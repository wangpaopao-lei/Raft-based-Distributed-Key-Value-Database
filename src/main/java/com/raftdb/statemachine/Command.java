package com.raftdb.statemachine;

import com.raftdb.rpc.proto.OperationType;

import java.io.*;
import java.util.Arrays;

/**
 * Represents a command to be applied to the key-value state machine.
 *
 * <p>Commands are the operations that clients submit to the Raft cluster.
 * After a command is committed to the log, it is applied to the state machine
 * on all nodes in the cluster.
 *
 * <h2>Supported Operations</h2>
 * <ul>
 *   <li>{@link OperationType#GET} - Retrieve the value for a key</li>
 *   <li>{@link OperationType#PUT} - Store a key-value pair</li>
 *   <li>{@link OperationType#DELETE} - Remove a key and its value</li>
 * </ul>
 *
 * <h2>Serialization</h2>
 * <p>Commands are serialized to bytes for storage in the Raft log using
 * {@link #toBytes()} and deserialized with {@link #fromBytes(byte[])}.
 * The binary format is:
 * <pre>
 * [4 bytes: operation type] [4 bytes: key length] [key bytes]
 * [4 bytes: value length or -1 if null] [value bytes if present]
 * </pre>
 *
 * <p>Example usage:
 * <pre>{@code
 * // Create commands
 * Command put = Command.put("name", "Alice");
 * Command get = Command.get("name");
 * Command del = Command.delete("name");
 *
 * // Serialize for log storage
 * byte[] bytes = put.toBytes();
 *
 * // Deserialize from log
 * Command restored = Command.fromBytes(bytes);
 * }</pre>
 *
 * @param type the operation type (GET, PUT, or DELETE)
 * @param key the key to operate on
 * @param value the value for PUT operations (null for GET and DELETE)
 * @author raft-kv
 * @see StateMachine
 * @see OperationType
 */
public record Command(
        OperationType type,
        byte[] key,
        byte[] value  // null for GET and DELETE
) {

    /**
     * Creates a GET command to retrieve a value.
     *
     * @param key the key to look up
     * @return a new GET command
     */
    public static Command get(byte[] key) {
        return new Command(OperationType.GET, key, null);
    }

    /**
     * Creates a PUT command to store a key-value pair.
     *
     * @param key the key to store
     * @param value the value to associate with the key
     * @return a new PUT command
     */
    public static Command put(byte[] key, byte[] value) {
        return new Command(OperationType.PUT, key, value);
    }

    /**
     * Creates a DELETE command to remove a key.
     *
     * @param key the key to delete
     * @return a new DELETE command
     */
    public static Command delete(byte[] key) {
        return new Command(OperationType.DELETE, key, null);
    }

    /**
     * Creates a GET command using a string key.
     *
     * @param key the key to look up
     * @return a new GET command
     */
    public static Command get(String key) {
        return get(key.getBytes());
    }

    /**
     * Creates a PUT command using string key and value.
     *
     * @param key the key to store
     * @param value the value to associate with the key
     * @return a new PUT command
     */
    public static Command put(String key, String value) {
        return put(key.getBytes(), value.getBytes());
    }

    /**
     * Creates a DELETE command using a string key.
     *
     * @param key the key to delete
     * @return a new DELETE command
     */
    public static Command delete(String key) {
        return delete(key.getBytes());
    }

    /**
     * Serializes this command to a byte array for storage in the Raft log.
     *
     * @return the serialized command bytes
     * @throws RuntimeException if serialization fails
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
     * Deserializes a command from a byte array.
     *
     * @param bytes the serialized command bytes
     * @return the deserialized command
     * @throws RuntimeException if deserialization fails
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

    /**
     * Compares this command with another for equality.
     *
     * @param o the object to compare with
     * @return {@code true} if the commands are equal, {@code false} otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Command command = (Command) o;
        return type == command.type
                && Arrays.equals(key, command.key)
                && Arrays.equals(value, command.value);
    }

    /**
     * Returns a hash code for this command.
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    /**
     * Returns a human-readable string representation of this command.
     *
     * @return a string in the format "TYPE(key)" or "PUT(key, value)"
     */
    @Override
    public String toString() {
        String keyStr = new String(key);
        if (type == OperationType.PUT) {
            return String.format("PUT(%s, %s)", keyStr, new String(value));
        }
        return String.format("%s(%s)", type, keyStr);
    }
}