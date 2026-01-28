package com.raftdb.log;

import com.raftdb.persistence.PersistenceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages the Raft log.
 *
 * Thread-safe implementation using read-write locks.
 * Supports optional persistence via PersistenceManager.
 * Supports log compaction via snapshots.
 */
public class LogManager {

    private static final Logger logger = LoggerFactory.getLogger(LogManager.class);

    // Log entries (1-indexed, position 0 is unused)
    private final List<LogEntry> entries = new ArrayList<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // Optional persistence
    private PersistenceManager persistence;

    // Snapshot info: all entries up to and including snapshotIndex have been compacted
    private volatile long snapshotIndex = 0;
    private volatile long snapshotTerm = 0;

    public LogManager() {
        // Add dummy entry at index 0 to make log 1-indexed
        entries.add(null);
    }

    /**
     * Set persistence manager and load existing logs.
     */
    public void setPersistence(PersistenceManager persistence) throws IOException {
        this.persistence = persistence;

        // Load existing logs from disk
        List<LogEntry> loaded = persistence.loadLogs();
        if (!loaded.isEmpty()) {
            lock.writeLock().lock();
            try {
                entries.addAll(loaded);
                logger.info("Recovered {} log entries, lastIndex={}", loaded.size(), getLastIndex());
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     * Set snapshot info after loading/installing a snapshot.
     * Also clears any log entries before or at the snapshot index.
     */
    public void setSnapshot(long index, long term) {
        lock.writeLock().lock();
        try {
            this.snapshotIndex = index;
            this.snapshotTerm = term;

            // Remove entries that are covered by the snapshot
            // Keep only entries after snapshotIndex
            if (!entries.isEmpty()) {
                int removeUpTo = (int) (index - getLogOffset());
                if (removeUpTo > 0 && removeUpTo < entries.size()) {
                    // Keep entries after snapshot
                    List<LogEntry> remaining = new ArrayList<>(entries.subList(removeUpTo + 1, entries.size()));
                    entries.clear();
                    entries.add(null); // dummy at index 0
                    entries.addAll(remaining);
                } else if (removeUpTo >= entries.size() - 1) {
                    // Snapshot covers all entries
                    entries.clear();
                    entries.add(null);
                }
            }

            logger.info("Set snapshot info: index={}, term={}", index, term);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get the log offset (entries before this are compacted into snapshot).
     */
    private long getLogOffset() {
        return snapshotIndex;
    }

    /**
     * Convert absolute log index to internal list index.
     */
    private int toInternalIndex(long absoluteIndex) {
        return (int) (absoluteIndex - snapshotIndex);
    }

    /**
     * Get the last log index (0 if log is empty).
     */
    public long getLastIndex() {
        lock.readLock().lock();
        try {
            return snapshotIndex + entries.size() - 1;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the term of the last log entry (0 if log is empty).
     */
    public long getLastTerm() {
        lock.readLock().lock();
        try {
            if (entries.size() <= 1) {
                return snapshotTerm;
            }
            return entries.get(entries.size() - 1).term();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the term at a specific index (0 if index is 0 or out of bounds).
     */
    public long getTerm(long index) {
        lock.readLock().lock();
        try {
            if (index <= 0) {
                return 0;
            }
            if (index == snapshotIndex) {
                return snapshotTerm;
            }
            if (index < snapshotIndex) {
                // Entry is compacted, we don't have it
                return 0;
            }
            int internalIndex = toInternalIndex(index);
            if (internalIndex <= 0 || internalIndex >= entries.size()) {
                return 0;
            }
            return entries.get(internalIndex).term();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get entry at a specific index (null if out of bounds or compacted).
     */
    public LogEntry getEntry(long index) {
        lock.readLock().lock();
        try {
            if (index <= snapshotIndex) {
                return null; // Compacted
            }
            int internalIndex = toInternalIndex(index);
            if (internalIndex <= 0 || internalIndex >= entries.size()) {
                return null;
            }
            return entries.get(internalIndex);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get entries from startIndex to the end of the log.
     */
    public List<LogEntry> getEntriesFrom(long startIndex) {
        lock.readLock().lock();
        try {
            long lastIdx = getLastIndex();
            if (startIndex > lastIdx) {
                return Collections.emptyList();
            }
            // If startIndex is compacted, we can't provide those entries
            if (startIndex <= snapshotIndex) {
                startIndex = snapshotIndex + 1;
            }
            int internalStart = toInternalIndex(startIndex);
            if (internalStart < 1) {
                internalStart = 1;
            }
            if (internalStart >= entries.size()) {
                return Collections.emptyList();
            }
            return new ArrayList<>(entries.subList(internalStart, entries.size()));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get entries in a range [startIndex, endIndex].
     */
    public List<LogEntry> getEntries(long startIndex, long endIndex) {
        lock.readLock().lock();
        try {
            if (startIndex <= snapshotIndex) {
                startIndex = snapshotIndex + 1;
            }
            int internalStart = toInternalIndex(startIndex);
            int internalEnd = toInternalIndex(endIndex) + 1;

            internalStart = Math.max(1, internalStart);
            internalEnd = Math.min(entries.size(), internalEnd);

            if (internalStart >= internalEnd) {
                return Collections.emptyList();
            }
            return new ArrayList<>(entries.subList(internalStart, internalEnd));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Check if the log contains an entry at the given index (not compacted).
     */
    public boolean containsEntry(long index) {
        lock.readLock().lock();
        try {
            if (index <= snapshotIndex) {
                return false;
            }
            int internalIndex = toInternalIndex(index);
            return internalIndex > 0 && internalIndex < entries.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the snapshot index.
     */
    public long getSnapshotIndex() {
        return snapshotIndex;
    }

    /**
     * Get the snapshot term.
     */
    public long getSnapshotTerm() {
        return snapshotTerm;
    }

    /**
     * Append a new entry to the log.
     *
     * @return the index of the appended entry
     */
    public long append(long term, byte[] command) {
        lock.writeLock().lock();
        try {
            long index = snapshotIndex + entries.size();
            LogEntry entry = LogEntry.of(index, term, command);
            entries.add(entry);

            // Persist
            if (persistence != null) {
                try {
                    persistence.appendLog(entry);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to persist log entry", e);
                }
            }

            logger.debug("Appended entry at index {} term {}", index, term);
            return index;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Append entries received from leader.
     * Handles conflict detection and truncation.
     *
     * @param prevLogIndex index of entry immediately preceding new entries
     * @param prevLogTerm term of prevLogIndex entry
     * @param newEntries entries to append
     * @return true if successful, false if log doesn't contain prevLogIndex/prevLogTerm
     */
    public boolean appendEntries(long prevLogIndex, long prevLogTerm, List<LogEntry> newEntries) {
        lock.writeLock().lock();
        try {
            // Check if log contains prevLogIndex with matching term
            if (prevLogIndex > 0) {
                // If prevLogIndex is at snapshot boundary, check snapshotTerm
                if (prevLogIndex == snapshotIndex) {
                    if (snapshotTerm != prevLogTerm) {
                        logger.debug("Term mismatch at snapshot index {}: {} != {}",
                                prevLogIndex, snapshotTerm, prevLogTerm);
                        return false;
                    }
                } else if (prevLogIndex < snapshotIndex) {
                    // Entry is compacted, assume it matches (leader wouldn't send otherwise)
                    // This is ok because snapshot installation guarantees consistency
                } else {
                    int internalPrev = toInternalIndex(prevLogIndex);
                    if (internalPrev >= entries.size()) {
                        logger.debug("Log doesn't contain prevLogIndex {}", prevLogIndex);
                        return false;
                    }
                    long termAtPrev = entries.get(internalPrev).term();
                    if (termAtPrev != prevLogTerm) {
                        logger.debug("Term mismatch at index {}: {} != {}", prevLogIndex, termAtPrev, prevLogTerm);
                        return false;
                    }
                }
            }

            // Track new entries to persist
            List<LogEntry> entriesToPersist = new ArrayList<>();

            // Append new entries, handling conflicts
            long insertIndex = prevLogIndex + 1;
            boolean truncated = false;

            for (LogEntry newEntry : newEntries) {
                int internalInsert = toInternalIndex(insertIndex);

                if (internalInsert > 0 && internalInsert < entries.size()) {
                    LogEntry existing = entries.get(internalInsert);
                    if (existing.term() != newEntry.term()) {
                        // Conflict: truncate from here
                        logger.info("Conflict at index {}, truncating", insertIndex);
                        truncateFromInternal(insertIndex);
                        truncated = true;
                        entries.add(newEntry);
                        entriesToPersist.add(newEntry);
                    }
                    // If terms match, entry is already there, skip
                } else if (internalInsert >= entries.size()) {
                    // New entry beyond current log
                    entries.add(newEntry);
                    entriesToPersist.add(newEntry);
                }
                // If internalInsert <= 0, entry is covered by snapshot, skip
                insertIndex++;
            }

            // Persist new entries
            if (persistence != null && !entriesToPersist.isEmpty()) {
                try {
                    if (truncated) {
                        // Need to rewrite the log file
                        List<LogEntry> remaining = new ArrayList<>();
                        for (int i = 1; i < entries.size() - entriesToPersist.size(); i++) {
                            remaining.add(entries.get(i));
                        }
                        persistence.truncateLog(insertIndex - entriesToPersist.size(), remaining);
                    }
                    persistence.appendLogs(entriesToPersist);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to persist log entries", e);
                }
            }

            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Truncate log from the given index (inclusive).
     */
    public void truncateFrom(long fromIndex) {
        lock.writeLock().lock();
        try {
            truncateFromInternal(fromIndex);

            // Persist truncation
            if (persistence != null) {
                try {
                    List<LogEntry> remaining = new ArrayList<>();
                    for (int i = 1; i < entries.size(); i++) {
                        remaining.add(entries.get(i));
                    }
                    persistence.truncateLog(fromIndex, remaining);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to persist log truncation", e);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void truncateFromInternal(long fromIndex) {
        if (fromIndex <= snapshotIndex) {
            // Can't truncate compacted entries
            return;
        }
        int internalFrom = toInternalIndex(fromIndex);
        if (internalFrom >= entries.size()) {
            return;
        }
        internalFrom = Math.max(1, internalFrom);
        logger.info("Truncating log from index {} (internal {})", fromIndex, internalFrom);
        entries.subList(internalFrom, entries.size()).clear();
    }

    /**
     * Compact log up to the given index (for snapshot).
     * Removes entries from 1 to compactIndex (inclusive).
     */
    public void compactTo(long compactIndex, long compactTerm) {
        lock.writeLock().lock();
        try {
            if (compactIndex <= snapshotIndex) {
                logger.debug("Nothing to compact, compactIndex {} <= snapshotIndex {}",
                        compactIndex, snapshotIndex);
                return;
            }

            int internalCompact = toInternalIndex(compactIndex);
            if (internalCompact <= 0 || internalCompact >= entries.size()) {
                logger.warn("Invalid compact index {}", compactIndex);
                return;
            }

            // Keep entries after compactIndex
            List<LogEntry> remaining = new ArrayList<>(
                    entries.subList(internalCompact + 1, entries.size()));

            entries.clear();
            entries.add(null); // dummy
            entries.addAll(remaining);

            snapshotIndex = compactIndex;
            snapshotTerm = compactTerm;

            logger.info("Compacted log to index {}, {} entries remaining", compactIndex, remaining.size());

            // Persist remaining entries (rewrite log file)
            if (persistence != null) {
                try {
                    persistence.rewriteLog(remaining);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to persist log compaction", e);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Reset log after installing a snapshot.
     * Clears all entries and sets new snapshot baseline.
     */
    public void resetToSnapshot(long index, long term) {
        lock.writeLock().lock();
        try {
            entries.clear();
            entries.add(null); // dummy
            snapshotIndex = index;
            snapshotTerm = term;

            logger.info("Reset log to snapshot: index={}, term={}", index, term);

            // Clear persisted log
            if (persistence != null) {
                try {
                    persistence.rewriteLog(Collections.emptyList());
                } catch (IOException e) {
                    throw new RuntimeException("Failed to clear persisted log", e);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get the size of the log (number of entries, excluding compacted).
     */
    public int size() {
        lock.readLock().lock();
        try {
            return entries.size() - 1;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Check if log is empty.
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Get all entries (for recovery/debugging).
     */
    public List<LogEntry> getAllEntries() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(entries.subList(1, entries.size()));
        } finally {
            lock.readLock().unlock();
        }
    }
}
