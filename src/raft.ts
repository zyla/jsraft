import delay from 'delay';
import { OVar, waitFor } from "./observable";
import { randomInRange } from "./utils";

export type Address = string;
export type Json = any;
export type Term = number;
export type LogIndex = number;
export type Payload = string;

// Transport doesn't know about message types, operates on opaque JSON objects.
export interface Transport {
  rpc(to: Address, request: Json): Promise<Json>;
  setReceiver(receiver: Receiver): void;
}

export interface Receiver {
  // Handle incoming RPC. Returns the response.
  handleMessage(from: Address, message: Json): Json;
}

export class NullReceiver {
  handleMessage() {}
}

export class TransportError extends Error {}

/// Main raft code

export interface Logger {
  (formatter: any, ...args: any[]): void;
  extend: (namespace: string) => Logger;
}

export type Config = {
  transport: Transport;

  // Servers in the cluster, including ourselves
  servers: Address[];
  myAddress: Address;

  heartbeatInterval: number;
  minElectionTimeout: number;
  maxElectionTimeout: number;

  logger: Logger;
};

export type LogEntry = [Term, Payload];

export const TERM = 0;
export const PAYLOAD = 1;

export type RequestVote = {
  type: "RequestVote";
  term: Term;

  lastLogIndex: LogIndex;
  lastLogTerm: Term;
};

export type AppendEntries = {
  type: "AppendEntries";
  term: Term;
  prevLogIndex: LogIndex;
  prevLogTerm: Term;
  entries: LogEntry[];
  leaderCommit: LogIndex;
};

export type Request = RequestVote | AppendEntries;

export type Response<Request> = Request extends RequestVote
  ? {
      term: Term;
      granted: boolean;
    }
  : Request extends AppendEntries
  ? {
      term: Term;
      success: boolean;
    }
  : never;

// A raft instance.
export class Raft {
  private _leader = new OVar<Address | null>(null);
  private logSize = new OVar(0);
  /** Current leader contact number. Incremented on every contact from the
   * leader; can be used to wait for the next one. */
  private leaderContact = new OVar(0);
  private matchIndex = new Map<Address, LogIndex>();
  private currentTerm: Term = 0;
  private log: LogEntry[] = [];
  private commitIndex: LogIndex = 0;
  private votedFor: Address | null = null;
  private _stopped = new OVar(false);

  constructor(private config: Config) {
    this.transport.setReceiver(this);

    this._leader.name = `${this.me}.leader`;
    this.leaderContact.name = `${this.me}.leaderContact`;
    this._stopped.name = `${this.me}.stopped`;
    this.logSize.name = `${this.me}.logSize`;
  }

  get term() {
    return this.currentTerm;
  }

  private get debug() {
    return this.config.logger;
  }

  private get transport() {
    return this.config.transport;
  }

  private get me() {
    return this.config.myAddress;
  }

  get address() {
    return this.config.myAddress;
  }

  private get peers() {
    return this.config.servers.filter((s) => s !== this.me);
  }

  get stopped() {
    return this._stopped.get();
  }

  get leader() {
    return this._leader.get();
  }

  get isLeader() {
    return this.leader === this.me;
  }

  // Returns current matchIndex for the given peer, or -1 if there's none.
  private getMatchIndex(peer: Address): LogIndex {
    const index = this.matchIndex.get(peer);
    return index !== undefined ? index : -1;
  }

  // Returns log term for the given log index, or -1 if there's none.
  private getLogTerm(index: LogIndex): Term {
    return index >= 0 && index < this.log.length ? this.log[index][TERM] : -1;
  }

  private rpc<Req extends Request>(
    to: Address,
    request: Req
  ): Promise<Response<Req>> {
    return this.transport.rpc(to, request);
  }

  // Receiver implementation.
  handleMessage(from: Address, message: Json): Promise<Json> {
    return this.handleRequest(from, message);
  }

  /// Logic

  start(): void {
    for (const peer of this.peers) {
      this.replicationTask(peer);
    }
    this.electionTask();
  }

  stop(): void {
    this._stopped.set(true);
  }

  private async replicationTask(peer: Address) {
    while (true) {
      await waitFor(() => this.isLeader || this.stopped);
      if(this.stopped) {
        return;
      }
      while (this.isLeader && !this.stopped) {
        await this.sendEntries(peer);
        await waitFor(
          () => !this.isLeader || (this.logSize.get() > 0 && this.logSize.get() > this.getMatchIndex(peer)) || this.stopped,
          this.config.heartbeatInterval
        );
      }
    }
  }

  private async sendEntries(peer: Address) {
    const debug = this.debug.extend("replication:" + peer);
    const term = this.currentTerm;
    let nextIndex = this.matchIndex.has(peer)
      ? this.matchIndex.get(peer)! + 1
      : this.log.length - 1;

    while (this.isLeader && !this.stopped) {
      const targetIndex = this.log.length - 1;
      const numEntries = targetIndex - nextIndex;

      debug(
        "attempting to replicate %d entries from index %d",
        numEntries,
        nextIndex
      );

      const reply = await this.rpc(peer, {
        type: "AppendEntries",
        term,
        prevLogIndex: nextIndex - 1,
        prevLogTerm: this.getLogTerm(nextIndex - 1),
        entries: this.log.slice(nextIndex, numEntries),
        leaderCommit: this.commitIndex,
      });

      if (reply.term > this.currentTerm) {
        this.updateTerm(reply.term);
        return;
      }

      if (reply.success) {
        debug("replicated up to %d", targetIndex);
        this.matchIndex.set(peer, targetIndex);
        return;
      } else {
        // TODO: implement actual binary search
        debug("peer log does not match, backtracking by 1");
        nextIndex--;
      }
    }
  }

  private updateTerm(newTerm: Term) {
    if (newTerm > this.currentTerm) {
      this.debug("our term is stale (%d > %d)", newTerm, this.currentTerm);
      if (this.isLeader) {
        this.debug("stepping down");
      }

      // Warning: updating currentTerm, votedFor and leader has to be atomic!
      this.currentTerm = newTerm;
      this.votedFor = null;
      this._leader.set(null);
    }
  }

  private updateCommitIndex(commitIndex: LogIndex) {
    if (commitIndex > this.commitIndex) {
      this.commitIndex = commitIndex;

      // TODO: notify commit listeners
    }
  }

  private async electionTask() {
    const debug = this.debug.extend("electionTask");

    while (true) {
      await waitFor(() => !this.isLeader || this.stopped);
      if(this.stopped) {
        return;
      }
      const lastLeaderContact = this.leaderContact.get();
      const result = await waitFor(() => {
        if(this.stopped) {
          return "stop";
        }
        if (this.isLeader || this.leaderContact.get() > lastLeaderContact) {
          return "continue";
        }
      }, randomInRange(this.config.minElectionTimeout, this.config.maxElectionTimeout));
      if (result === "stop") {
        return;
      }
      if (result === "continue") {
        continue;
      }

      while (!this.leader) {
        if(this.stopped) {
          return;
        }

        const term = ++this.currentTerm;
        this.votedFor = this.me;
        const neededVotes = Math.floor(this.config.servers.length / 2) + 1;
        const numVotes = new OVar(1, `${this.me}.numVotes[term=${term}]`);
        const aborted = new OVar(false, `${this.me}.aborted[term=${term}]`);

        debug(
          "starting election for term %d, need %d votes",
          term,
          neededVotes
        );

        for (const peer of this.peers) {
          (async () => {
            const response = await this.rpc(peer, {
              type: "RequestVote",
              term,
              lastLogIndex: this.log.length - 1,
              lastLogTerm:
                this.log.length > 0 ? this.log[this.log.length - 1][TERM] : -1,
            });
            if (response.term > term) {
              this.updateTerm(response.term);
              aborted.set(true);
              return;
            }
            if (response.granted) {
              debug("vote granted by %s", peer);
              numVotes.set(numVotes.get() + 1);
            }
          })();
        }

        const lastLeaderContact = this.leaderContact.get();
        await waitFor(
          () =>
            numVotes.get() >= neededVotes ||
            aborted.get() ||
            this.leaderContact.get() > lastLeaderContact ||
          this.stopped,
          randomInRange(
            this.config.minElectionTimeout,
            this.config.maxElectionTimeout
          )
        );

        if (this.currentTerm === term && numVotes.get() >= neededVotes) {
          debug("got needed votes, becoming leader in term %d", term);
          this.becomeLeader();
        }
      }
    }
  }

  private becomeLeader() {
    this.matchIndex.clear();
    this._leader.set(this.me);
  }

  // The real RPC handler, appropriately typed.
  private handleRequest(
    from: Address,
    request: Request
  ): Promise<Response<typeof request>> {
    if (request.type === "RequestVote") {
      return this.handleRequestVote(from, request);
    } else if (request.type === "AppendEntries") {
      return this.handleAppendEntries(from, request);
    } else {
      throw new Error("Invalid request type");
    }
  }

  private async handleRequestVote(
    from: Address,
    request: RequestVote
  ): Promise<Response<RequestVote>> {
    if (request.term > this.currentTerm) {
      this.updateTerm(request.term);
    }

    if (request.term < this.currentTerm) {
      return {
        term: this.currentTerm,
        granted: false,
      };
    }

    if (!this.votedFor || this.votedFor === from) {
      this.debug("voted for %s in term %d", from, this.currentTerm);
      this.votedFor = from;
      // TODO: save to stable storage

      return {
        term: this.currentTerm,
        granted: true,
      };
    } else {
      this.debug("not voting for %s in term %d, already voted for %s", from, this.currentTerm, this.votedFor);
      return {
        term: this.currentTerm,
        granted: false,
      };
    }
  }

  private async handleAppendEntries(
    from: Address,
    request: AppendEntries
  ): Promise<Response<AppendEntries>> {
    if (request.term > this.currentTerm) {
      this.updateTerm(request.term);
    }

    if (request.term < this.currentTerm) {
      return {
        term: this.currentTerm,
        success: false,
      };
    }

    if (!this.leader) {
      this._leader.set(from);
    }

    this.leaderContact.set(this.leaderContact.get() + 1);

    if (
      request.prevLogIndex > 0 &&
      (request.prevLogIndex >= this.log.length ||
        request.prevLogTerm !== this.log[request.prevLogIndex][TERM])
    ) {
      this.debug(
        "our log doesn't match leader %s at index %d",
        from,
        request.prevLogIndex
      );
      return {
        term: this.currentTerm,
        success: false,
      };
    }

    for (let i = 0; i < request.entries.length; i++) {
      const targetIndex = request.prevLogIndex + 1 + i;

      if (
        targetIndex < this.log.length &&
        this.log[targetIndex][TERM] !== request.entries[i][TERM]
      ) {
        this.debug("discarding log entries starting from %d", targetIndex);
        this.log.splice(targetIndex);
        // TODO: notify listeners that entries were discarded
      }

      if (targetIndex >= this.log.length) {
        this.log.push(request.entries[i]);
      }
    }

    this.updateCommitIndex(request.leaderCommit);

    return {
      term: this.currentTerm,
      success: true,
    };
  }
}
