package com.raftdb.persistence;

import com.raftdb.core.NodeId;
import com.raftdb.log.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Manages persistence of Raft state to disk.
 *
 * Files:
 * - meta.dat: currentTerm, votedFor (must be fsync'd on every update)
 * - log.dat: append-only log entries
 */
public class PersistenceManager {

    private static final Logger logger = LoggerFactory.getLogger(PersistenceManager.class);

    private static final String META_FILE = "meta.dat";
    private static final String LOG_FILE = "log.dat";

    private final Path dataDir;
    private final Path metaFile;
    private final Path logFile;

    // Cached state
    private long currentTerm = 0;
    private NodeId votedFor = null;

    // Log file handle for appending
    private RandomAccessFile logRaf;

    public PersistenceManager(Path dataDir) {
        this.dataDir = dataDir;
        this.metaFile = dataDir.resolve(META_FILE);
        this.logFile = dataDir.resolve(LOG_FILE);
    }

    /**
     * Initialize persistence. Creates directory if needed, recovers state from disk.
     */
    public void init() throws IOException {
        // Create data directory
        Files.createDirectories(dataDir);

        // Load meta if exists
        if (Files.exists(metaFile)) {
            loadMeta();
        }

        // Open log file for appending
        logRaf = new RandomAccessFile(logFile.toFile(), "rw");

        logger.info("PersistenceManager initialized at {}", dataDir);
    }

    /**
     * Close resources.
     */
    public void close() {
        try {
            if (logRaf != null) {
                logRaf.close();
            }
        } catch (IOException e) {
            logger.warn("Error closing log file", e);
        }
    }

    // ==================== Meta Persistence ====================

    /**
     * Save term and votedFor atomically.
     * This must be called before responding to any RPC.
     */
    public void saveMeta(long term, NodeId votedFor) throws IOException {
        this.currentTerm = term;
        this.votedFor = votedFor;

        // Write to temp file first
        Path tempFile = dataDir.resolve(META_FILE + ".tmp");

        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(tempFile)))) {
            dos.writeLong(term);
            dos.writeBoolean(votedFor != null);
            if (votedFor != null) {
                dos.writeUTF(votedFor.id());
            }
        }

        // Atomic rename
        Files.move(tempFile, metaFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        logger.debug("Saved meta: term={}, votedFor={}", term, votedFor);
    }

    private void loadMeta() throws IOException {
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(metaFile)))) {
            currentTerm = dis.readLong();
            boolean hasVotedFor = dis.readBoolean();
            if (hasVotedFor) {
                votedFor = NodeId.of(dis.readUTF());
            }
        }

        logger.info("Loaded meta: term={}, votedFor={}", currentTerm, votedFor);
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public NodeId getVotedFor() {
        return votedFor;
    }

    // ==================== Log Persistence ====================

    /**
     * Append a log entry to disk.
     */
    public void appendLog(LogEntry entry) throws IOException {
        byte[] data = serializeEntry(entry);

        synchronized (logRaf) {
            logRaf.seek(logRaf.length());
            logRaf.writeInt(data.length);
            logRaf.write(data);
            logRaf.getFD().sync();  // Ensure durability
        }

        logger.trace("Appended log entry: index={}", entry.index());
    }

    /**
     * Append multiple log entries to disk.
     */
    public void appendLogs(List<LogEntry> entries) throws IOException {
        if (entries.isEmpty()) {
            return;
        }

        synchronized (logRaf) {
            logRaf.seek(logRaf.length());

            for (LogEntry entry : entries) {
                byte[] data = serializeEntry(entry);
                logRaf.writeInt(data.length);
                logRaf.write(data);
            }

            logRaf.getFD().sync();
        }

        logger.debug("Appended {} log entries", entries.size());
    }

    /**
     * Load all log entries from disk.
     */
    public List<LogEntry> loadLogs() throws IOException {
        List<LogEntry> entries = new ArrayList<>();

        if (!Files.exists(logFile) || Files.size(logFile) == 0) {
            return entries;
        }

        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(logFile)))) {

            while (dis.available() > 0) {
                int length = dis.readInt();
                byte[] data = new byte[length];
                dis.readFully(data);
                entries.add(deserializeEntry(data));
            }
        }

        logger.info("Loaded {} log entries from disk", entries.size());
        return entries;
    }

    /**
     * Truncate log from the given index (inclusive).
     * Rewrites the log file without entries >= fromIndex.
     */
    public void truncateLog(long fromIndex, List<LogEntry> remainingEntries) throws IOException {
        synchronized (logRaf) {
            logRaf.close();

            // Rewrite the file with only remaining entries
            try (DataOutputStream dos = new DataOutputStream(
                    new BufferedOutputStream(Files.newOutputStream(logFile)))) {

                for (LogEntry entry : remainingEntries) {
                    if (entry.index() < fromIndex) {
                        byte[] data = serializeEntry(entry);
                        dos.writeInt(data.length);
                        dos.write(data);
                    }
                }
            }

            // Reopen for appending
            logRaf = new RandomAccessFile(logFile.toFile(), "rw");
        }

        logger.info("Truncated log from index {}", fromIndex);
    }

    /**
     * Rewrite the log file with the given entries.
     * Used for log compaction after snapshot.
     */
    public void rewriteLog(List<LogEntry> entries) throws IOException {
        synchronized (logRaf) {
            logRaf.close();

            // Rewrite the file
            try (DataOutputStream dos = new DataOutputStream(
                    new BufferedOutputStream(Files.newOutputStream(logFile)))) {

                for (LogEntry entry : entries) {
                    byte[] data = serializeEntry(entry);
                    dos.writeInt(data.length);
                    dos.write(data);
                }
            }

            // Reopen for appending
            logRaf = new RandomAccessFile(logFile.toFile(), "rw");
        }

        logger.info("Rewrote log with {} entries", entries.size());
    }

    // ==================== Serialization ====================

    private byte[] serializeEntry(LogEntry entry) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(bos)) {

            dos.writeLong(entry.index());
            dos.writeLong(entry.term());
            dos.writeInt(entry.command().length);
            dos.write(entry.command());

            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize log entry", e);
        }
    }

    private LogEntry deserializeEntry(byte[] data) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             DataInputStream dis = new DataInputStream(bis)) {

            long index = dis.readLong();
            long term = dis.readLong();
            int cmdLength = dis.readInt();
            byte[] command = new byte[cmdLength];
            dis.readFully(command);

            return LogEntry.of(index, term, command);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize log entry", e);
        }
    }

    /**
     * Get the data directory path.
     */
    public Path getDataDir() {
        return dataDir;
    }
}
