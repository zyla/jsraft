import debug from 'debug';
import OVar from './observable';

type Address = string;
type Json = any;
type Term = number;
type LogIndex = number;
type Payload = string;

// Transport doesn't know about message types, operates on opaque JSON objects.
interface Transport {
  rpc(to: Address, request: Json): Promise<Json>;
  setReceiver(receiver: Receiver): void;
}

interface Receiver {
  // Handle incoming RPC. Returns the response.
  handleMessage(from: Address, message: Json): Json;
}

class NullReceiver {
  handleMessage() {}
}

class TransportError extends Error {}

class MemTransport {
  private receiver: Receiver = new NullReceiver();

  constructor(private net: MemNetwork, private myAddress: Address) {}

  setReceiver(receiver: Receiver) {
    this.receiver = receiver;
  }

  rpc(to: Address, request: Json): Promise<Json> {
    const otherNode = this.net.nodes.get(to);
    if(!otherNode) {
      throw new TransportError('unknown_address');
    }
    return otherNode.receiver.handleMessage(this.myAddress, request);
  }
}

class MemNetwork {
  public nodes: Map<Address, MemTransport> = new Map();

  constructor(addrs: Address[]) {
    for(const addr of addrs) {
      this.nodes.set(addr, new MemTransport(this, addr));
    }
  }
}

/// delay

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/// Main raft code

export type Config = {
  transport: Transport;

  // Servers in the cluster, including ourselves
  servers: Address[];
  myAddress: Address;

  heartbeatInterval: number;
  minElectionTimeout: number;
  maxElectionTimeout: number;

  logger: debug.Debugger;
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
  entries: LogEntry[],
  leaderCommit: LogIndex;
};

export type Request = RequestVote | AppendEntries;

export type Response<Request> =
  Request extends RequestVote ? {
    term: Term;
    granted: boolean;
  } :
  Request extends AppendEntries ? {
    term: Term;
    success: boolean;
  } :
  never;

// A raft instance.
export class Raft {
  private leader = new OVar<Address | null>(null);
  private logSize = new OVar(0);
  private leaderContact = new OVar<null>(null);
  private matchIndex = new Map<Address, LogIndex>();
  private currentTerm: Term = 0;
  private log: LogEntry[] = [];
  private commitIndex: LogIndex = 0;
  private votedFor: Address | null = null;

  constructor(private config: Config) {
    this.transport.setReceiver(this);
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

  private get peers() {
    return this.config.servers.filter(s => s !== this.me);
  }

  private waitUntilBecomingLeader() {
    return this.leader.waitFor(l => l === this.me);
  }

  private waitUntilBecomingNonLeader() {
    return this.leader.waitFor(l => l != this.me);
  }

  private get isLeader() {
    return this.leader.get() === this.me;
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

  private rpc<Req extends Request>(to: Address, request: Req): Promise<Response<Req>> {
    return this.transport.rpc(to, request);
  }

  // Receiver implementation.
  handleMessage(from: Address, message: Json): Promise<Json> {
    return this.handleRequest(from, message);
  }

  /// Logic
  
  start(): void {
    for(const peer of this.peers) {
      this.replicationTask(peer);
    }
    this.electionTask();
  }

  private async replicationTask(peer: Address) {
    while(true) {
      await this.waitUntilBecomingLeader();
      while(this.isLeader) {
        await this.sendEntries(peer);

        await Promise.race([
          delay(this.config.heartbeatInterval),
          this.waitUntilBecomingNonLeader(),
          // FIXME: this leaks subscribers
          this.logSize.waitFor(logSize => logSize > this.getMatchIndex(peer)),
        ]);
      }
    }
  }

  private async sendEntries(peer: Address) {
    const debug = this.debug.extend('replication:' + peer);
    let nextIndex = this.matchIndex.has(peer) ? this.matchIndex.get(peer)! + 1 : this.log.length - 1;

    while(true) {
      const targetIndex = this.log.length - 1;
      const numEntries = targetIndex - nextIndex;

      debug('attempting to replicate %d entries from index %d', numEntries, nextIndex);

      const reply = await this.rpc(peer, {
        type: "AppendEntries",
        term: this.currentTerm,
        prevLogIndex: nextIndex - 1,
        prevLogTerm: this.getLogTerm(nextIndex - 1),
        entries: this.log.slice(nextIndex, numEntries),
        leaderCommit: this.commitIndex,
      });

      if(reply.term > this.currentTerm) {
        this.updateTerm(reply.term);
        return;
      }

      if(reply.success) {
        debug('replicated up to %d', targetIndex);
        this.matchIndex.set(peer, targetIndex);
        return;
      } else {
        // TODO: implement actual binary search
        debug('peer log does not match, backtracking by 1');
        nextIndex--;
      }
    }
  }

  private updateTerm(newTerm: Term) {
    if(newTerm > this.currentTerm) {
      this.debug('our term is stale (%d > %d)', newTerm, this.currentTerm);
      if(this.isLeader) {
        this.debug('stepping down');
      }

      // Warning: updating currentTerm, votedFor and leader has to be atomic!
      this.currentTerm = newTerm;
      this.votedFor = null;
      this.leader.set(null);
    }
  }

  private updateCommitIndex(commitIndex: LogIndex) {
    if(commitIndex > this.commitIndex) {
      this.commitIndex = commitIndex;

      // TODO: notify commit listeners
    }
  }

  private async electionTask() {
    const debug = this.debug.extend('electionTask');

    while(true) {
      await this.waitUntilBecomingNonLeader();
      const result = await Promise.race([
        delay(randomInRange(this.config.minElectionTimeout, this.config.maxElectionTimeout)).then(() => 'timeout'),
        this.leaderContact.wait().then(() => 'continue'),
        this.waitUntilBecomingLeader().then(() => 'continue'),
      ]);
      if(result === 'continue') {
        continue;
      }

      while(!this.leader.get()) {
        const term = ++this.currentTerm;
        this.votedFor = this.me;
        const neededVotes = Math.floor(this.config.servers.length / 2) + 1;
        const numVotes = new OVar(1);
        const abort = new OVar(null);

        debug("starting election for term %d, need %d votes", term, neededVotes);

        for(const peer of this.peers) {
          (async () => {
            const response = await this.rpc(peer, {
              type: "RequestVote",
              term,
              lastLogIndex: this.log.length - 1,
              lastLogTerm: this.log.length > 0 ? this.log[this.log.length - 1][TERM] : -1,
            });
            if(response.term > term) {
              this.updateTerm(response.term);
              abort.set(null);
              return;
            }
            if(response.granted) {
              debug("vote granted by %s", peer);
              numVotes.set(numVotes.get() + 1);
            }
          })();
        }

        await Promise.race([
          numVotes.waitFor(nv => nv >= neededVotes),
          abort.wait(),
          this.leaderContact.wait(),
          delay(randomInRange(this.config.minElectionTimeout, this.config.maxElectionTimeout)),
        ]);

        if(this.currentTerm === term && numVotes.get() >= neededVotes) {
          debug("got needed votes, becoming leader");
          this.becomeLeader();
        } else {
          debug("election failed, retrying");
        }
      }
    }
  }

  private becomeLeader() {
    this.matchIndex.clear();
    this.leader.set(this.me);
  }

  // The real RPC handler, appropriately typed.
  private handleRequest(from: Address, request: Request): Promise<Response<typeof request>> {
    if(request.type === "RequestVote") {
      return this.handleRequestVote(from, request);
    } else if(request.type === "AppendEntries") {
      return this.handleAppendEntries(from, request);
    } else {
      throw new Error("Invalid request type");
    }
  }

  private async handleRequestVote(from: Address, request: RequestVote): Promise<Response<RequestVote>> {
    if(request.term > this.currentTerm) {
      this.updateTerm(request.term);
    }

    if(request.term < this.currentTerm) {
      return {
        term: this.currentTerm,
        granted: false,
      };
    }

    if(!this.votedFor || this.votedFor === from) {
      this.votedFor = from;
      // TODO: save to stable storage

      return {
        term: this.currentTerm,
        granted: true,
      };
    } else {
      return {
        term: this.currentTerm,
        granted: false,
      };
    }
  }

  private async handleAppendEntries(from: Address, request: AppendEntries): Promise<Response<AppendEntries>> {
    if(request.term > this.currentTerm) {
      this.updateTerm(request.term);
    }

    if(request.term < this.currentTerm) {
      return {
        term: this.currentTerm,
        success: false,
      };
    }

    if(!this.leader.get()) {
      this.leader.set(from);
    }

    if(request.prevLogIndex > 0 && (request.prevLogIndex >= this.log.length || request.prevLogTerm !== this.log[request.prevLogIndex][TERM])) {
      this.debug("our log doesn't match leader %s at index %d", from, request.prevLogIndex);
      return {
        term: this.currentTerm,
        success: false,
      };
    }

    this.leaderContact.set(null);

    for(let i = 0; i < request.entries.length; i++) {
      const targetIndex = request.prevLogIndex + 1 + i;

      if(targetIndex < this.log.length && this.log[targetIndex][TERM] !== request.entries[i][TERM]) {
        this.debug("discarding log entries starting from %d", targetIndex);
        this.log.splice(targetIndex);
        // TODO: notify listeners that entries were discarded
      }

      if(targetIndex >= this.log.length) {
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

function randomInRange(lo: number, hi: number): number {
  return lo + (Math.random() * (hi - lo));
}
