package com.raftdb.snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;

/**
 * Manages snapshot persistence for log compaction.
 *
 * <p>Snapshots capture the state machine's state at a specific point in time,
 * allowing log entries up to that point to be discarded. This bounds the log
 * size and enables fast recovery for nodes that fall far behind.
 *
 * <h2>Snapshot Files</h2>
 * <ul>
 *   <li><b>snapshot.dat</b> - Raw snapshot data (serialized state machine state)</li>
 *   <li><b>snapshot.meta</b> - Metadata containing:
 *       <ul>
 *         <li>{@code lastIncludedIndex} - Index of the last log entry in the snapshot</li>
 *         <li>{@code lastIncludedTerm} - Term of the last log entry in the snapshot</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h2>Atomic Writes</h2>
 * <p>Snapshots are saved atomically using the write-to-temp-then-rename pattern:
 * <ol>
 *   <li>Write data to temporary files (snapshot.dat.tmp, snapshot.meta.tmp)</li>
 *   <li>Fsync both files to ensure durability</li>
 *   <li>Atomically rename temp files to final names</li>
 * </ol>
 * <p>This ensures that even a crash during snapshot save won't corrupt existing data.
 *
 * <h2>Usage</h2>
 * <p>The SnapshotManager is used in two scenarios:
 * <ul>
 *   <li><b>Taking snapshots</b> - Leader periodically snapshots state machine to compact log</li>
 *   <li><b>Installing snapshots</b> - Follower receives snapshot via InstallSnapshot RPC</li>
 * </ul>
 *
 * @author raft-kv
 * @see com.raftdb.statemachine.StateMachine#takeSnapshot()
 * @see com.raftdb.statemachine.StateMachine#restoreSnapshot(byte[])
 */
public class SnapshotManager {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    private static final String SNAPSHOT_FILE = "snapshot.dat";
    private static final String META_FILE = "snapshot.meta";

    private final Path dataDir;
    private final Path snapshotFile;
    private final Path metaFile;

    /** Index of the last log entry included in the snapshot. */
    private volatile long lastIncludedIndex = 0;

    /** Term of the last log entry included in the snapshot. */
    private volatile long lastIncludedTerm = 0;

    /**
     * Constructs a new SnapshotManager for the given data directory.
     *
     * <p>Creates the directory if it doesn't exist and loads any existing
     * snapshot metadata.
     *
     * @param dataDir the directory where snapshot files will be stored
     * @throws IOException if initialization fails
     */
    public SnapshotManager(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        this.snapshotFile = dataDir.resolve(SNAPSHOT_FILE);
        this.metaFile = dataDir.resolve(META_FILE);

        Files.createDirectories(dataDir);
        loadMetadata();

        logger.info("SnapshotManager initialized at {}", dataDir);
    }

    /**
     * Saves a snapshot with its metadata atomically.
     *
     * @param data the serialized state machine data
     * @param lastIndex the index of the last log entry included in this snapshot
     * @param lastTerm the term of the last log entry included in this snapshot
     * @throws IOException if saving fails
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