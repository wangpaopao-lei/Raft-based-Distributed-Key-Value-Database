package com.raftdb.log;

import java.util.Arrays;

/**
 * Represents a single entry in the Raft replicated log.
 *
 * <p>Each log entry contains:
 * <ul>
 *   <li>{@code index} - The position of this entry in the log (1-indexed)</li>
 *   <li>{@code term} - The term when the entry was received by the leader</li>
 *   <li>{@code command} - The state machine command to be applied</li>
 * </ul>
 *
 * <p>Log entries are the fundamental unit of replication in Raft. The leader
 * appends client commands as new log entries and replicates them to followers.
 * Once an entry is stored on a majority of servers, it is considered committed
 * and can be applied to the state machine.
 *
 * <p>This class is implemented as a Java record for immutability. The command
 * byte array is defensively copied to ensure true immutability.
 *
 * <p>Example usage:
 * <pre>{@code
 * LogEntry entry = LogEntry.of(1, 1, "SET key value".getBytes());
 * System.out.println(entry.index()); // 1
 * System.out.println(entry.term());  // 1
 * }</pre>
 *
 * @author raft-kv
 * @see LogManager
 */
public record LogEntry(
        long index,
        long term,
        byte[] command
) {

    /**
     * Factory method to create a new log entry with defensive copying.
     *
     * @param index the log index (1-indexed position in the log)
     * @param term the term when this entry was created
     * @param command the state machine command (will be copied)
     * @return a new {@code LogEntry} instance
     */
    public static LogEntry of(long index, long term, byte[] command) {
        return new LogEntry(index, term, command != null ? command.clone() : new byte[0]);
    }

    /**
     * Converts this log entry to its Protocol Buffers representation.
     *
     * <p>This is used for serializing log entries in RPC messages.
     *
     * @return the protobuf representation of this log entry
     */
    public com.raftdb.rpc.proto.LogEntry toProto() {
        return com.raftdb.rpc.proto.LogEntry.newBuilder()
                .setIndex(index)
                .setTerm(term)
                .setCommand(com.google.protobuf.ByteString.copyFrom(command))
                .build();
    }

    /**
     * Creates a log entry from its Protocol Buffers representation.
     *
     * @param proto the protobuf log entry
     * @return a new {@code LogEntry} instance
     */
    public static LogEntry fromProto(com.raftdb.rpc.proto.LogEntry proto) {
        return new LogEntry(
                proto.getIndex(),
                proto.getTerm(),
                proto.getCommand().toByteArray()
        );
    }

    /**
     * Compares this log entry with another for equality.
     *
     * <p>Two log entries are equal if they have the same index, term, and command bytes.
     *
     * @param o the object to compare with
     * @return {@code true} if the entries are equal, {@code false} otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogEntry logEntry = (LogEntry) o;
        return index == logEntry.index
                && term == logEntry.term
                && Arrays.equals(command, logEntry.command);
    }

    /**
     * Returns a hash code for this log entry.
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        int result = Long.hashCode(index);
        result = 31 * result + Long.hashCode(term);
        result = 31 * result + Arrays.hashCode(command);
        return result;
    }

    /**
     * Returns a string representation of this log entry for debugging.
     *
     * @return a string containing the index, term, and command length
     */
    @Override
    public String toString() {
        return String.format("LogEntry{index=%d, term=%d, cmdLen=%d}", index, term, command.length);
    }
}
