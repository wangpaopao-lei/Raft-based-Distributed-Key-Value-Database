package com.raftdb.snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;

/**
 * Manages snapshot persistence.
 *
 * Files:
 * - snapshot.dat: Raw snapshot data (KV state)
 * - snapshot.meta: Metadata (lastIncludedIndex, lastIncludedTerm)
 */
public class SnapshotManager {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    private static final String SNAPSHOT_FILE = "snapshot.dat";
    private static final String META_FILE = "snapshot.meta";

    private final Path dataDir;
    private final Path snapshotFile;
    private final Path metaFile;

    // Cached metadata
    private volatile long lastIncludedIndex = 0;
    private volatile long lastIncludedTerm = 0;

    public SnapshotManager(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        this.snapshotFile = dataDir.resolve(SNAPSHOT_FILE);
        this.metaFile = dataDir.resolve(META_FILE);

        Files.createDirectories(dataDir);
        loadMetadata();

        logger.info("SnapshotManager initialized at {}", dataDir);
    }

    /**
     * Save a snapshot with its metadata.
     */
    public synchronized void saveSnapshot(byte[] data, long lastIndex, long lastTerm) throws IOException {
        // Write snapshot data to temp file first
        Path tempSnapshot = dataDir.resolve(SNAPSHOT_FILE + ".tmp");
        Path tempMeta = dataDir.resolve(META_FILE + ".tmp");

        try {
            // Write snapshot data
            try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(tempSnapshot))) {
                os.write(data);
                os.flush();
                // fsync
                if (os instanceof FileOutputStream fos) {
                    fos.getFD().sync();
                }
            }

            // Write metadata
            try (DataOutputStream dos = new DataOutputStream(
                    new BufferedOutputStream(Files.newOutputStream(tempMeta)))) {
                dos.writeLong(lastIndex);
                dos.writeLong(lastTerm);
                dos.flush();
            }

            // Atomic rename
            Files.move(tempSnapshot, snapshotFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            Files.move(tempMeta, metaFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            // Update cached values
            this.lastIncludedIndex = lastIndex;
            this.lastIncludedTerm = lastTerm;

            logger.info("Saved snapshot: lastIndex={}, lastTerm={}, size={} bytes",
                    lastIndex, lastTerm, data.length);

        } finally {
            // Cleanup temp files if they exist
            Files.deleteIfExists(tempSnapshot);
            Files.deleteIfExists(tempMeta);
        }
    }

    /**
     * Load snapshot data.
     * @return snapshot data, or null if no snapshot exists
     */
    public byte[] loadSnapshot() throws IOException {
        if (!Files.exists(snapshotFile)) {
            return null;
        }

        byte[] data = Files.readAllBytes(snapshotFile);
        logger.debug("Loaded snapshot: {} bytes", data.length);
        return data;
    }

    /**
     * Load metadata from disk.
     */
    private void loadMetadata() throws IOException {
        if (!Files.exists(metaFile)) {
            lastIncludedIndex = 0;
            lastIncludedTerm = 0;
            return;
        }

        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(metaFile)))) {
            lastIncludedIndex = dis.readLong();
            lastIncludedTerm = dis.readLong();
        }

        logger.debug("Loaded snapshot metadata: lastIndex={}, lastTerm={}",
                lastIncludedIndex, lastIncludedTerm);
    }

    /**
     * Check if a snapshot exists.
     */
    public boolean hasSnapshot() {
        return Files.exists(snapshotFile) && Files.exists(metaFile) && lastIncludedIndex > 0;
    }

    /**
     * Get the last included index in the snapshot.
     */
    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    /**
     * Get the last included term in the snapshot.
     */
    public long getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    /**
     * Delete snapshot files.
     */
    public void deleteSnapshot() throws IOException {
        Files.deleteIfExists(snapshotFile);
        Files.deleteIfExists(metaFile);
        lastIncludedIndex = 0;
        lastIncludedTerm = 0;
        logger.info("Deleted snapshot");
    }

    /**
     * Get snapshot file size (for monitoring).
     */
    public long getSnapshotSize() {
        try {
            return Files.exists(snapshotFile) ? Files.size(snapshotFile) : 0;
        } catch (IOException e) {
            return 0;
        }
    }

    public void close() {
        // Nothing to close, but keep for consistency
    }
}
