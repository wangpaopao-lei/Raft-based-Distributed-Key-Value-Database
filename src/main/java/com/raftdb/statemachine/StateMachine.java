package com.raftdb.statemachine;

/**
 * State machine interface for applying committed log entries.
 *
 * The state machine is deterministic: given the same sequence of commands,
 * all nodes will arrive at the same state.
 */
public interface StateMachine {

    /**
     * Apply a command to the state machine.
     *
     * @param command the command to apply
     * @return result of the command (value for GET, null for PUT/DELETE)
     */
    byte[] apply(Command command);

    /**
     * Get a value by key (direct read, bypasses Raft log).
     * Use only for leader reads or stale reads.
     */
    byte[] get(byte[] key);

    /**
     * Take a snapshot of current state (for Phase 4).
     */
    default byte[] takeSnapshot() {
        throw new UnsupportedOperationException("Snapshot not implemented");
    }

    /**
     * Restore state from a snapshot (for Phase 4).
     */
    default void restoreSnapshot(byte[] data) {
        throw new UnsupportedOperationException("Snapshot not implemented");
    }
}
