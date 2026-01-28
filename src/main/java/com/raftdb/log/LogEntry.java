package com.raftdb.log;

import java.util.Arrays;

/**
 * A single entry in the Raft log.
 */
public record LogEntry(
        long index,
        long term,
        byte[] command
) {

    public static LogEntry of(long index, long term, byte[] command) {
        return new LogEntry(index, term, command != null ? command.clone() : new byte[0]);
    }

    /**
     * Convert to protobuf LogEntry.
     */
    public com.raftdb.rpc.proto.LogEntry toProto() {
        return com.raftdb.rpc.proto.LogEntry.newBuilder()
                .setIndex(index)
                .setTerm(term)
                .setCommand(com.google.protobuf.ByteString.copyFrom(command))
                .build();
    }

    /**
     * Create from protobuf LogEntry.
     */
    public static LogEntry fromProto(com.raftdb.rpc.proto.LogEntry proto) {
        return new LogEntry(
                proto.getIndex(),
                proto.getTerm(),
                proto.getCommand().toByteArray()
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogEntry logEntry = (LogEntry) o;
        return index == logEntry.index
                && term == logEntry.term
                && Arrays.equals(command, logEntry.command);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(index);
        result = 31 * result + Long.hashCode(term);
        result = 31 * result + Arrays.hashCode(command);
        return result;
    }

    @Override
    public String toString() {
        return String.format("LogEntry{index=%d, term=%d, cmdLen=%d}", index, term, command.length);
    }
}
