import debug from 'debug';
import delay from 'delay';
import { format, inspect } from 'util';
import { randomInRange } from "./utils";
import { waitFor } from "./observable";
import { Raft, Logger, Transport, Receiver, NullReceiver, Json, TransportError, Config, Address } from './raft';

class Cluster {
  private stopped = false;
  public servers: Raft[];
  public logger: Logger;
  private net: MemNetwork;

  constructor(addrs: Address[]) {
    debug.enable('raft:*');
    const logger = debug('raft');
    logger.log = (fmt: any, ...args: any[]) => {
      if(!this.stopped) {
        process.stdout.write(format(fmt, ...args) + '\n');
      }
    }

    this.logger = logger;

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
      });
      s.start();
      return s;
    });
  }

  get leader() {
    for(const s of this.servers) {
      if(s.isLeader) {
        return s.address;
      }
    }
    return null;
  }

  get leaders() {
    return this.servers.map(s => s.leader);
  }

  stop() {
    this.stopped = true;
    for(const s of this.servers) {
      s.stop();
    }
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

function setupCluster(numServers: number = 3) {
  const addrs = Array.from(Array(numServers).keys()).map((index) => "s" + (index + 1));
  return new Cluster(addrs);
}

describe("Leader election", () => {
  it("works without network disruption", async () => {
    const c = setupCluster();
    const debug = c.logger.extend('test');
    try {
      const leader = await waitFor(() => c.leader);
      expect(leader).toBeTruthy();
      debug('got a leader: %s', leader);
      await waitFor(() => {
        const leaders = c.leaders;
        if(leaders.some(l => !l)) {
          return null;
        }
        if(leaders.some(l => l !== leader)) {
          throw new Error('Not all nodes know the same leader: ' + inspect(leaders));
        }
        return true;
      });
    } finally {
      c.stop();
    }
  });
});
