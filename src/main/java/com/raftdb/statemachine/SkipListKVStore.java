package com.raftdb.statemachine;

import com.raftdb.rpc.proto.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * KV store implementation using ConcurrentSkipListMap.
 *
 * Skip List provides:
 * - O(log n) get, put, delete
 * - Ordered iteration (useful for range queries)
 * - Lock-free concurrent reads
 */
public class SkipListKVStore implements StateMachine {

    private static final Logger logger = LoggerFactory.getLogger(SkipListKVStore.class);

    // Using ByteArrayWrapper as key because byte[] doesn't implement equals/hashCode properly
    private final ConcurrentSkipListMap<ByteArrayWrapper, byte[]> store = new ConcurrentSkipListMap<>();

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

    @Override
    public byte[] get(byte[] key) {
        byte[] value = store.get(new ByteArrayWrapper(key));
        logger.trace("GET {} -> {}", new String(key), value != null ? new String(value) : "null");
        return value;
    }

    public void put(byte[] key, byte[] value) {
        logger.trace("PUT {} = {}", new String(key), new String(value));
        store.put(new ByteArrayWrapper(key), value);
    }

    public void delete(byte[] key) {
        logger.trace("DELETE {}", new String(key));
        store.remove(new ByteArrayWrapper(key));
    }

    /**
     * Get the number of entries in the store.
     */
    public int size() {
        return store.size();
    }

    /**
     * Check if store is empty.
     */
    public boolean isEmpty() {
        return store.isEmpty();
    }

    /**
     * Clear all entries.
     */
    public void clear() {
        store.clear();
    }

    /**
     * Take a snapshot of current state.
     * Format: [numEntries][keyLen][key][valueLen][value]...
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
     * Restore state from a snapshot.
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
     * Wrapper for byte[] to use as map key.
     */
    private static class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {
        private final byte[] data;
        private final int hashCode;

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

        @Override
        public int compareTo(ByteArrayWrapper other) {
            return Arrays.compare(this.data, other.data);
        }
    }
}
