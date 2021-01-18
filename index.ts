import debug from 'debug';

type Address = string;
type Json = any;
type Term = number;
type LogIndex = number;
type Payload = string;

const setTimeout = (this as any).setTimeout;

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

/// OVar - "observable variable"

class OVar<T> {
  private listeners: ((v: T) => void)[] = [];

  constructor(private value: T) {}

  addListener(l: (v: T) => void) {
    this.listeners.push(l);
  }

  removeListener(l: (v: T) => void) {
    const i = this.listeners.indexOf(l);
    if(i !== -1) {
      this.listeners.splice(i, 1);
    }
  }

  get() {
    return this.value;
  }

  set(v: T) {
    this.value = v;
    setTimeout(() => {
      for(const l of this.listeners) {
        l(v);
      }
    }, 0);
  }

  waitFor(predicate: (v: T) => boolean): Promise<void> {
    return new Promise(resolve => {
      if(predicate(this.get())) {
        resolve();
        return;
      }
      const l = (v: T) => {
        if(predicate(v)) {
          this.removeListener(l);
          resolve();
        }
      };
      this.addListener(l);
    });
  }
}

/// delay

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/// Main raft code

type Config = {
  transport: Transport;

  // Servers in the cluster, including ourselves
  servers: Address[];
  myAddress: Address;

  heartbeatInterval: number;

  logger: debug.Debugger;
};

type LogEntry = [Term, Payload];

type RequestVote = {
  type: "RequestVote";
  term: Term;

  lastLogIndex: LogIndex;
  lastLogTerm: Term;
};

type AppendEntries = {
  type: "AppendEntries";
  term: Term;
  prevLogIndex: LogIndex;
  prevLogTerm: Term;
  entries: LogEntry[],
  leaderCommit: LogIndex;
};

type Request = RequestVote | AppendEntries;

type Response<Request> =
  Request extends RequestVote ? {
    term: Term;
    granted: boolean;
  } :
  Request extends AppendEntries ? {
    term: Term;
    success: boolean;
  } :
  "lol";

// A raft instance.
class Raft {
  private isLeader = new OVar(false);
  private logSize = new OVar(0);
  private matchIndex = new Map<Address, LogIndex>();
  private currentTerm: Term = 0;
  private log: LogEntry[] = [];
  private commitIndex: LogIndex = 0;

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

  // Returns current matchIndex for the given peer, or -1 if there's none.
  private getMatchIndex(peer: Address): LogIndex {
    const index = this.matchIndex.get(peer);
    return index !== undefined ? index : -1;
  }

  // Returns log term for the given log index, or -1 if there's none.
  private getLogTerm(index: LogIndex): Term {
    return index >= 0 && index < this.log.length ? this.log[index][0] : -1;
  }

  private rpc<Req extends Request>(to: Address, request: Req): Promise<Response<Req>> {
    return this.transport.rpc(to, request);
  }

  // Receiver implementation.
  handleMessage(from: Address, message: Json): Promise<Json> {
    return this.handleRequest(from, message);
  }

  /// Logic
  
  start() {
    for(const server of this.config.servers) {
      if(server === this.me) {
        continue;
      }
      this.replicationTask(server);
    }
    this.electionTask();
  }

  private async replicationTask(peer: Address) {
    while(true) {
      await this.isLeader.waitFor(isLeader => isLeader);
      while(this.isLeader) {
        const steppedDown = this.isLeader.waitFor(isLeader => !isLeader);

        await this.sendEntries(peer);

        await Promise.race([
          delay(this.config.heartbeatInterval),
          steppedDown,

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

      // Warning: updating currentTerm and isLeader has to be atomic!
      this.currentTerm = newTerm;
      if(this.isLeader.get()) {
        this.debug('stepping down');
        this.isLeader.set(false);
      }
    }
  }

  private async electionTask() {
    while(true) {
      await this.isLeader.waitFor(isLeader => !isLeader);
    }
  }

  // The real RPC handler, appropriately typed.
  private handleRequest<Req extends Request>(from: Address, request: Req): Promise<Response<Req>> {
    throw new Error('nope');
  }
}
