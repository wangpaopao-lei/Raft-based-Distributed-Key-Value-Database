package com.raftdb.statemachine;

/**
 * Interface for the replicated state machine in a Raft cluster.
 *
 * <p>The state machine is the application-specific component that processes
 * commands after they have been committed to the Raft log. In this key-value
 * database implementation, the state machine stores and retrieves key-value pairs.
 *
 * <h2>Determinism Requirement</h2>
 * <p>The state machine must be deterministic: given the same initial state and
 * the same sequence of commands, all nodes in the cluster must arrive at exactly
 * the same final state. This is essential for maintaining consistency across replicas.
 *
 * <h2>Command Processing</h2>
 * <p>Commands are applied via {@link #apply(Command)} after being committed.
 * The order of command application matches the log order, ensuring all replicas
 * process commands in the same sequence.
 *
 * <h2>Snapshot Support</h2>
 * <p>To enable log compaction, the state machine must support:
 * <ul>
 *   <li>{@link #takeSnapshot()} - Serialize the current state to a byte array</li>
 *   <li>{@link #restoreSnapshot(byte[])} - Restore state from a serialized snapshot</li>
 * </ul>
 *
 * @author raft-kv
 * @see Command
 * @see SkipListKVStore
 */
public interface StateMachine {

    /**
     * Applies a command to the state machine.
     *
     * <p>This method is called when a log entry has been committed and should
     * be applied to update the state machine. The command must be processed
     * deterministically.
     *
     * @param command the command to apply
     * @return the result of the command execution:
     *         <ul>
     *           <li>For GET: the value associated with the key, or null if not found</li>
     *           <li>For PUT: null (or optionally the previous value)</li>
     *           <li>For DELETE: null (or optionally the deleted value)</li>
     *         </ul>
     */
    byte[] apply(Command command);

    /**
     * Retrieves a value by key directly from the state machine.
     *
     * <p><b>Warning:</b> This is a direct read that bypasses the Raft log.
     * It may return stale data if:
     * <ul>
     *   <li>Called on a follower that hasn't applied recent commits</li>
     *   <li>Called on a leader that has been partitioned from the cluster</li>
     * </ul>
     *
     * <p>For linearizable reads, use the Raft read protocol instead.
     *
     * @param key the key to look up
     * @return the value associated with the key, or null if not found
     */
    byte[] get(byte[] key);

    /**
     * Creates a snapshot of the current state machine state.
     *
     * <p>The snapshot captures the complete state at a point in time, allowing
     * log entries up to that point to be discarded. The returned byte array
     * must contain all information needed to restore the state via
     * {@link #restoreSnapshot(byte[])}.
     *
     * @return a serialized representation of the current state
     * @throws UnsupportedOperationException if snapshots are not supported
     */
    default byte[] takeSnapshot() {
        throw new UnsupportedOperationException("Snapshot not implemented");
    }

    /**
     * Restores the state machine from a snapshot.
     *
     * <p>This method replaces the current state with the state captured in
     * the snapshot. It is called when:
     * <ul>
     *   <li>A node recovers from a crash and loads a saved snapshot</li>
     *   <li>A follower receives a snapshot from the leader via InstallSnapshot RPC</li>
     * </ul>
     *
     * @param data the serialized snapshot data
     * @throws UnsupportedOperationException if snapshots are not supported
     */
    default void restoreSnapshot(byte[] data) {
        throw new UnsupportedOperationException("Snapshot not implemented");
    }
}
