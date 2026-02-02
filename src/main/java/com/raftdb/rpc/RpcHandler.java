package com.raftdb.rpc;

import com.raftdb.rpc.proto.AppendEntriesRequest;
import com.raftdb.rpc.proto.AppendEntriesResponse;
import com.raftdb.rpc.proto.InstallSnapshotRequest;
import com.raftdb.rpc.proto.InstallSnapshotResponse;
import com.raftdb.rpc.proto.VoteRequest;
import com.raftdb.rpc.proto.VoteResponse;

/**
 * Handler interface for processing incoming Raft RPC requests.
 *
 * <p>This interface is implemented by {@link com.raftdb.core.RaftNode} to handle
 * the three types of Raft RPCs:
 * <ul>
 *   <li><b>RequestVote</b> - Process vote requests during leader election</li>
 *   <li><b>AppendEntries</b> - Process log replication and heartbeat messages</li>
 *   <li><b>InstallSnapshot</b> - Process snapshot transfers from the leader</li>
 * </ul>
 *
 * <p>The transport layer ({@link RpcTransport}) receives incoming requests and
 * delegates them to this handler for processing. All handler methods should
 * be thread-safe as they may be called concurrently from multiple transport threads.
 *
 * @author raft-kv
 * @see RpcTransport
 * @see com.raftdb.core.RaftNode
 */
public interface RpcHandler {

    /**
     * Handles an incoming vote request from a candidate.
     *
     * <p>This method implements the RequestVote RPC receiver logic:
     * <ol>
     *   <li>Update term if request term is higher</li>
     *   <li>Grant vote if: haven't voted OR already voted for this candidate,
     *       AND candidate's log is at least as up-to-date as ours</li>
     *   <li>Reset election timer if vote is granted</li>
     * </ol>
     *
     * @param request the vote request from a candidate
     * @return the vote response indicating whether the vote was granted
     */
    VoteResponse handleVoteRequest(VoteRequest request);

    /**
     * Handles an incoming append entries request from the leader.
     *
     * <p>This RPC serves dual purposes:
     * <ul>
     *   <li><b>Heartbeat</b> - When entries list is empty, acknowledges leader and resets election timer</li>
     *   <li><b>Log Replication</b> - When entries list is non-empty, appends entries to local log</li>
     * </ul>
     *
     * <p>Processing steps:
     * <ol>
     *   <li>Update term if request term is higher</li>
     *   <li>Reject if request term is lower than current term</li>
     *   <li>Reset election timer (valid message from leader)</li>
     *   <li>Check log consistency at prevLogIndex/prevLogTerm</li>
     *   <li>Append new entries, resolving conflicts</li>
     *   <li>Update commit index if leader's commit is higher</li>
     * </ol>
     *
     * @param request the append entries request from the leader
     * @return the response indicating success/failure and match information
     */
    AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request);

    /**
     * Handles an incoming install snapshot request from the leader.
     *
     * <p>This RPC is invoked when the leader needs to bring a far-behind follower
     * up to date by sending a snapshot instead of individual log entries.
     *
     * <p>Processing steps:
     * <ol>
     *   <li>Update term if request term is higher</li>
     *   <li>Reject if request term is lower than current term</li>
     *   <li>Reset election timer (valid message from leader)</li>
     *   <li>Restore state machine from snapshot data</li>
     *   <li>Update log to reflect snapshot (discard covered entries)</li>
     *   <li>Update commit index and last applied index</li>
     * </ol>
     *
     * @param request the install snapshot request from the leader
     * @return the response containing the current term
     */
    InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request);
}
