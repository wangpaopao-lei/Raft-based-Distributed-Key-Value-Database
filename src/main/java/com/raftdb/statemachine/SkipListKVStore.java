package com.raftdb.statemachine;

import com.raftdb.rpc.proto.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Key-value store implementation using {@link ConcurrentSkipListMap}.
 *
 * <p>This class provides the {@link StateMachine} implementation for the Raft-KV
 * database. It stores key-value pairs in memory with support for concurrent access
 * and snapshot operations.
 *
 * <h2>Performance Characteristics</h2>
 * <ul>
 *   <li>GET, PUT, DELETE: O(log n) time complexity</li>
 *   <li>Ordered key iteration (useful for range queries)</li>
 *   <li>Lock-free concurrent reads</li>
 *   <li>Thread-safe writes without external synchronization</li>
 * </ul>
 *
 * <h2>Snapshot Format</h2>
 * <p>Snapshots are serialized in the following binary format:
 * <pre>
 * [4 bytes: number of entries]
 * For each entry:
 *   [4 bytes: key length][key bytes][4 bytes: value length][value bytes]
 * </pre>
 *
 * <h2>Implementation Notes</h2>
 * <p>Since Java's {@code byte[]} does not implement {@code equals()} and
 * {@code hashCode()} properly for use as map keys, this class uses an internal
 * {@code ByteArrayWrapper} class that provides correct equality semantics and
 * lexicographic ordering.
 *
 * @author raft-kv
 * @see StateMachine
 * @see Command
 */
public class SkipListKVStore implements StateMachine {

    private static final Logger logger = LoggerFactory.getLogger(SkipListKVStore.class);

    // Using ByteArrayWrapper as key because byte[] doesn't implement equals/hashCode properly
    private final ConcurrentSkipListMap<ByteArrayWrapper, byte[]> store = new ConcurrentSkipListMap<>();

    /**
     * Applies a command to the key-value store.
     *
     * @param command the command to apply (GET, PUT, or DELETE)
     * @return for GET, the value associated with the key (or null if not found);
     *         for PUT and DELETE, always returns null
     * @throws IllegalArgumentException if the command type is unknown
     */
    @Override
    public byte[] apply(Command command) {
        return switch (command.type()) {
            case GET -> get(command.key());
            case PUT -> {
                put(command.key(), command.value());
                yield null;
            }
            case DELETE -> {
                delete(command.key());
                yield null;
            }
            default -> throw new IllegalArgumentException("Unknown command type: " + command.type());
        };
    }

    /**
     * Retrieves the value associated with the specified key.
     *
     * @param key the key to look up
     * @return the value associated with the key, or null if the key does not exist
     */
    @Override
    public byte[] get(byte[] key) {
        byte[] value = store.get(new ByteArrayWrapper(key));
        logger.trace("GET {} -> {}", new String(key), value != null ? new String(value) : "null");
        return value;
    }

    /**
     * Stores a key-value pair in the store.
     *
     * <p>If the key already exists, its value is replaced.
     *
     * @param key the key to store
     * @param value the value to associate with the key
     */
    public void put(byte[] key, byte[] value) {
        logger.trace("PUT {} = {}", new String(key), new String(value));
        store.put(new ByteArrayWrapper(key), value);
    }

    /**
     * Removes the key-value pair with the specified key.
     *
     * <p>If the key does not exist, this method has no effect.
     *
     * @param key the key to delete
     */
    public void delete(byte[] key) {
        logger.trace("DELETE {}", new String(key));
        store.remove(new ByteArrayWrapper(key));
    }

    /**
     * Returns the number of key-value pairs in the store.
     *
     * @return the number of entries
     */
    public int size() {
        return store.size();
    }

    /**
     * Checks if the store is empty.
     *
     * @return {@code true} if the store contains no entries, {@code false} otherwise
     */
    public boolean isEmpty() {
        return store.isEmpty();
    }

    /**
     * Removes all key-value pairs from the store.
     */
    public void clear() {
        store.clear();
    }

    /**
     * Creates a snapshot of the current state.
     *
     * <p>The snapshot captures all key-value pairs in a binary format that can
     * be restored via {@link #restoreSnapshot(byte[])}.
     *
     * @return the serialized snapshot data
     * @throws RuntimeException if serialization fails
     */
    @Override
    public byte[] takeSnapshot() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeInt(store.size());

            for (Map.Entry<ByteArrayWrapper, byte[]> entry : store.entrySet()) {
                byte[] key = entry.getKey().data;
                byte[] value = entry.getValue();

                dos.writeInt(key.length);
                dos.write(key);
                dos.writeInt(value.length);
                dos.write(value);
            }

            dos.flush();
            byte[] snapshot = baos.toByteArray();
            logger.debug("Created snapshot with {} entries, {} bytes", store.size(), snapshot.length);
            return snapshot;

        } catch (IOException e) {
            throw new RuntimeException("Failed to create snapshot", e);
        }
    }

    /**
     * Restores the state from a previously created snapshot.
     *
     * <p>This method clears all existing data before restoring the snapshot.
     *
     * @param data the serialized snapshot data (may be null or empty for an empty state)
     * @throws RuntimeException if deserialization fails
     */
    @Override
    public void restoreSnapshot(byte[] data) {
        store.clear();

        if (data == null || data.length == 0) {
            logger.debug("Restored empty snapshot");
            return;
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             DataInputStream dis = new DataInputStream(bais)) {

            int numEntries = dis.readInt();

            for (int i = 0; i < numEntries; i++) {
                int keyLen = dis.readInt();
                byte[] key = new byte[keyLen];
                dis.readFully(key);

                int valueLen = dis.readInt();
                byte[] value = new byte[valueLen];
                dis.readFully(value);

                store.put(new ByteArrayWrapper(key), value);
            }

            logger.debug("Restored snapshot with {} entries", numEntries);

        } catch (IOException e) {
            throw new RuntimeException("Failed to restore snapshot", e);
        }
    }

    /**
     * Wrapper class for byte arrays to enable use as map keys.
     *
     * <p>This class provides proper {@code equals()}, {@code hashCode()}, and
     * {@code compareTo()} implementations for byte arrays, enabling them to be
     * used as keys in sorted maps.
     */
    private static class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {
        private final byte[] data;
        private final int hashCode;

        /**
         * Constructs a wrapper around the given byte array.
         *
         * @param data the byte array to wrap
         */
        ByteArrayWrapper(byte[] data) {
            this.data = data;
            this.hashCode = Arrays.hashCode(data);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ByteArrayWrapper that = (ByteArrayWrapper) o;
            return Arrays.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        /**
         * Compares this byte array with another lexicographically.
         *
         * @param other the other byte array wrapper
         * @return negative if this &lt; other, zero if equal, positive if this &gt; other
         */
        @Override
        public int compareTo(ByteArrayWrapper other) {
            return Arrays.compare(this.data, other.data);
        }
    }
}
