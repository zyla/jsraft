import debug from 'debug';
import delay from 'delay';
import { format, inspect } from 'util';
import { randomInRange } from "./utils";
import { OVar, waitFor as observableWaitFor } from "./observable";
import { Raft, Term, Logger, Transport, Receiver, NullReceiver, Json, TransportError, Config, Address } from './raft';

class Cluster {
  private stopped = false;
  public servers: Raft[];
  public logger: Logger;
  private net: MemNetwork;

  constructor(addrs: Address[], config: ClusterConfig) {
    debug.enable('raft:*');
    const logger = debug('raft');
    logger.log = (fmt: any, ...args: any[]) => {
      if(!this.stopped) {
        process.stdout.write(format(fmt, ...args) + '\n');
      }
    }

    this.logger = logger;
//    OVar.logger = logger.extend('ovar');

    this.net = new MemNetwork(addrs, this.logger.extend('net'));
    this.servers = addrs.map(addr => {
      const s = new Raft({
        transport: this.net.nodes.get(addr)!,
        servers: addrs,
        myAddress: addr,
        heartbeatInterval,
        minElectionTimeout,
        maxElectionTimeout,
        logger: this.logger.extend(addr),
        ...config
      });
      s.start();
      return s;
    });
  }

  leader() {
    let term = 0;
    let leader: Address | null = null;
    for(const s of this.servers) {
      if(s.isLeader && s.term > term) {
        term = s.term;
        leader = s.address;
      }
    }
    return leader;
  }

  get term() {
    let term = 0;
    for(const s of this.servers) {
      term = Math.max(term, s.term);
    }
    return term;
  }

  stop() {
    this.logger("stopping the cluster");
    this.stopped = true;
    for(const s of this.servers) {
      s.stop();
    }
  }

  /**
   * Check if all nodes know a leader.
   * Returns true if so.
   * Throws an error if any node knows a different leader.
   */
  checkAllLeaders() {
    const leader = this.leader();
    const term = this.term;
    if(!leader) {
      return false;
    }
    const leaders = this.servers.map(s => [s.term, s.leader]);
    this.logger('leaders: %O', leaders);
    if(this.servers.some(s => s.term === term && s.leader && s.leader !== leader)) {
      throw new Error('Not all nodes know the same leader: ' + inspect(leaders));
    }
    return this.servers.every(s => s.leader);
  }
}

export class MemTransport {
  private receiver: Receiver = new NullReceiver();

  constructor(private net: MemNetwork, private myAddress: Address, private logger: Logger) {}

  setReceiver(receiver: Receiver) {
    this.receiver = receiver;
  }

  async rpc(to: Address, request: Json): Promise<Json> {
    const otherNode = this.net.nodes.get(to);
    if (!otherNode) {
      throw new TransportError("unknown_address");
    }
    await delay(randomInRange(0, 10));
    this.logger("%s->%s %s", this.myAddress, to, JSON.stringify(request));
    const response = await otherNode.receiver.handleMessage(this.myAddress, request);
    await delay(randomInRange(0, 10));
    this.logger("%s->%s %s", to, this.myAddress, JSON.stringify(response));
    return response;
  }
}

export class MemNetwork {
  public nodes: Map<Address, MemTransport> = new Map();

  constructor(addrs: Address[], logger: Logger) {
    for (const addr of addrs) {
      this.nodes.set(addr, new MemTransport(this, addr, logger));
    }
  }
}

const SLOW = false;

const heartbeatInterval = SLOW ? 500 : 50;
const minElectionTimeout = SLOW ? 1000 : 100;
const maxElectionTimeout = SLOW ? 2000 : 200;

type ClusterConfig = {
  numServers?: number;
  heartbeatInterval?: number;
  minElectionTimeout?: number;
  maxElectionTimeout?: number;
};

function setupCluster(config: ClusterConfig = {}) {
  const addrs = Array.from(Array(config.numServers || 3).keys()).map((index) => "s" + (index + 1));
  return new Cluster(addrs, config);
}

async function waitFor<T>(
  fn: () => T,
  timeout?: number
): Promise<T> {
  const result = await observableWaitFor(fn, timeout);
  if(result === 'timeout') {
    throw new Error('waitFor: timeout waiting for ' + fn);
  }
  return result;
}

describe("Leader election", () => {
  it("works without network disruption", async () => {
    const c = setupCluster();
    const debug = c.logger.extend('test');
    try {
      await waitFor(() => c.checkAllLeaders(), 2 * maxElectionTimeout);
    } finally {
      c.stop();
    }
  });

  it("works with many candidates", async () => {
    // Set election timeouts such that they are very likely to fire at the same time
    const c = setupCluster({ minElectionTimeout: 100, maxElectionTimeout: 105 });
    const debug = c.logger.extend('test');
    try {
      const p = waitFor(() => {
        const l = c.leader();
        debug('LEADER: %s', l);
        return l;
      }, 3000);
      waitFor(() => {
        debug('leaders: %O, leader: %s', c.servers.map(s => [s.term, s.leader]), c.leader());
        return false;
      });
      await p;
      await waitFor(() => c.checkAllLeaders(), 200);
    } finally {
      c.stop();
    }
  });
});
