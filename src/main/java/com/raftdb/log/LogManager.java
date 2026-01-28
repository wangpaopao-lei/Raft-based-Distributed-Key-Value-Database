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
 */
public class LogManager {

    private static final Logger logger = LoggerFactory.getLogger(LogManager.class);

    // Log entries (1-indexed, position 0 is unused)
    private final List<LogEntry> entries = new ArrayList<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // Optional persistence
    private PersistenceManager persistence;

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
     * Get the last log index (0 if log is empty).
     */
    public long getLastIndex() {
        lock.readLock().lock();
        try {
            return entries.size() - 1;
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
                return 0;
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
            if (index <= 0 || index >= entries.size()) {
                return 0;
            }
            return entries.get((int) index).term();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get entry at a specific index (null if out of bounds).
     */
    public LogEntry getEntry(long index) {
        lock.readLock().lock();
        try {
            if (index <= 0 || index >= entries.size()) {
                return null;
            }
            return entries.get((int) index);
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
            if (startIndex >= entries.size()) {
                return Collections.emptyList();
            }
            int start = Math.max(1, (int) startIndex);
            return new ArrayList<>(entries.subList(start, entries.size()));
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
            int start = Math.max(1, (int) startIndex);
            int end = Math.min(entries.size(), (int) endIndex + 1);
            if (start >= end) {
                return Collections.emptyList();
            }
            return new ArrayList<>(entries.subList(start, end));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Append a new entry to the log.
     *
     * @return the index of the appended entry
     */
    public long append(long term, byte[] command) {
        lock.writeLock().lock();
        try {
            long index = entries.size();
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
                if (prevLogIndex >= entries.size()) {
                    logger.debug("Log doesn't contain prevLogIndex {}", prevLogIndex);
                    return false;
                }
                long termAtPrev = entries.get((int) prevLogIndex).term();
                if (termAtPrev != prevLogTerm) {
                    logger.debug("Term mismatch at index {}: {} != {}", prevLogIndex, termAtPrev, prevLogTerm);
                    return false;
                }
            }

            // Track new entries to persist
            List<LogEntry> entriesToPersist = new ArrayList<>();

            // Append new entries, handling conflicts
            long insertIndex = prevLogIndex + 1;
            boolean truncated = false;

            for (LogEntry newEntry : newEntries) {
                if (insertIndex < entries.size()) {
                    LogEntry existing = entries.get((int) insertIndex);
                    if (existing.term() != newEntry.term()) {
                        // Conflict: truncate from here
                        logger.info("Conflict at index {}, truncating", insertIndex);
                        truncateFromInternal(insertIndex);
                        truncated = true;
                        entries.add(newEntry);
                        entriesToPersist.add(newEntry);
                    }
                    // If terms match, entry is already there, skip
                } else {
                    // New entry beyond current log
                    entries.add(newEntry);
                    entriesToPersist.add(newEntry);
                }
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
        if (fromIndex >= entries.size()) {
            return;
        }
        int from = Math.max(1, (int) fromIndex);
        logger.info("Truncating log from index {}", from);
        entries.subList(from, entries.size()).clear();
    }

    /**
     * Get the size of the log (number of entries, excluding dummy at index 0).
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
