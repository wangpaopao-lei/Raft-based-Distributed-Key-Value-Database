package com.raftdb.statemachine;

import com.raftdb.rpc.proto.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
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
