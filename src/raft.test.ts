import debug from "debug";
import delay from "delay";
import { format, inspect } from "util";
import { randomInRange, deepEquals } from "./utils";
import { OVar } from "./observable";
import {
  Raft,
  Term,
  Logger,
  Transport,
  Receiver,
  NullReceiver,
  Json,
  TransportError,
  Config,
  Address,
} from "./raft";
import { Scheduler } from './scheduler';

class Cluster {
  private stopped = false;
  public servers: Raft[];
  public logger: Logger;
  private net: MemNetwork;

  constructor(addrs: Address[], config: ClusterConfig) {
    const logger = debug("raft");
    logger.log = (fmt: any, ...args: any[]) => {
      if (!this.stopped) {
        process.stdout.write(format(fmt, ...args) + "\n");
      }
    };

    this.logger = logger;
    if (process.env.LOG_OVAR) {
      OVar.logger = logger.extend("ovar");
    }

    this.net = new MemNetwork(addrs, this.logger.extend("net"));
    this.servers = addrs.map((addr) => {
      const s = new Raft({
        transport: this.net.nodes.get(addr)!,
        servers: addrs,
        myAddress: addr,
        heartbeatInterval,
        minElectionTimeout,
        maxElectionTimeout,
        logger: this.logger.extend(addr),
        ...config,
      });
      s.start();
      return s;
    });
  }

  server(addr: Address): Raft {
    return this.servers.find((s) => s.address === addr)!;
  }

  leader() {
    let term = 0;
    let leader: Address | null = null;
    for (const s of this.servers) {
      // NOTE: the order here matters.
      // `s.isLeader` reads an OVar, but `s.term` doesn't.
      // Therefore if we fail the term check, we don't register any OVar dependencies!
      if (s.isLeader && s.term > term) {
        term = s.term;
        leader = s.address;
      }
    }
    return leader;
  }

  get term() {
    let term = 0;
    for (const s of this.servers) {
      term = Math.max(term, s.term);
    }
    return term;
  }

  stop() {
    this.logger("stopping the cluster");
    this.stopped = true;
    for (const s of this.servers) {
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
    if (!leader) {
      return false;
    }
    const leaders = this.servers.map((s) => [s.term, s.leader]);
    this.logger("leaders: %O", leaders);
    if (
      this.servers.some(
        (s) => s.term === term && s.leader && s.leader !== leader
      )
    ) {
      throw new Error(
        "Not all nodes know the same leader: " + inspect(leaders)
      );
    }
    return this.servers.every((s) => s.leader);
  }
}

export class MemTransport {
  private receiver: Receiver = new NullReceiver();

  constructor(
    private net: MemNetwork,
    private myAddress: Address,
    private logger: Logger
  ) {}

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
    const response = await otherNode.receiver.handleMessage(
      this.myAddress,
      request
    );
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
  const addrs = Array.from(Array(config.numServers || 3).keys()).map(
    (index) => "s" + (index + 1)
  );
  return new Cluster(addrs, config);
}

async function waitFor<T>(s: Scheduler, fn: () => T): Promise<T> {
  let result;
  let steps = 0;
  while(!(result = fn())) {
    if(steps++ >= 5000) {
      throw new Error("waitForS: timeout waiting for " + fn);
    }
    await s.step();
    if(s.finished()) {
      throw new Error("waitForS(" + fn + "): process finished");
    }
    s.resumeRandom();
  }
  return result;
}

const deferred: (() => Promise<void> | void)[] = [];

function defer(callback: () => Promise<void> | void) {
  deferred.push(callback);
}

afterEach(flushDeferred);

async function flushDeferred() {
  let cb;
  while(cb = deferred.pop()) {
    await Promise.resolve(cb());
  }
}

describe("Leader election", () => {
  it("works without network disruption", async () => {
    for(let i = 1; i <= 100; i++) {
      try {
        const s = new Scheduler;
        s.install();
        defer(async () => { await s.step(); s.uninstall() });
        const c = setupCluster();
        defer(() => c.stop());
        const debug = c.logger.extend("test");
        debug("========= ITERATION %d ==========", i);
        await waitFor(s, () => c.checkAllLeaders());
      } finally {
        await flushDeferred();
      }
    }
  });
});

describe("Replication", () => {
  it("can replicate log entries", async () => {
    // In this test we only check that the logs are replicated - by directly inspecting logs on all the nodes.
    // Note that this doesn't look at state machine application, or even tracking the commit index.
    for(let i = 1; i <= 100; i++) {
      try {
        const s = new Scheduler;
        s.install();
        defer(async () => { await s.step(); s.uninstall() });
        const c = setupCluster();
        defer(() => c.stop());
        const debug = c.logger.extend("test");
        debug("========= ITERATION %d ==========", i);

        // Debug
        OVar.waitFor(() => {
          for (const s of c.servers) {
            debug("%s: %O", s.address, s.getLog());
          }
        });

        await waitFor(s, () => c.leader());
        const leader = c.server(c.leader()!);
        const entry = await leader.propose("Hello");
        // Leader should have the entry in the log immediately
        const expectedLog = [[entry.term, "Hello"]];
        expect(leader.getLog()).toEqual(expectedLog);

        function waitForLogsToConverge() {
          return waitFor(s, () => {
            for (const s of c.servers) {
              if (!deepEquals(s.getLog(), expectedLog)) {
                return false;
              }
            }
            return true;
          });
        }
        await waitForLogsToConverge();

        // Another entry
        const entry2 = await leader.propose("World");
        expectedLog.push([entry2.term, "World"]);
        await waitForLogsToConverge();
      } finally {
        await flushDeferred();
      }
    }
  });
});
