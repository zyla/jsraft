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
        electionDisabled: config.oneLeader && addr !== "s1",
        ...config,
      });
      s.start();
      return s;
    });
  }

  server(addr: Address): Raft {
    return this.servers.find((s) => s.address === addr)!;
  }

  leader(): Address | null {
    return this.leaderWithTerm()[0];
  }

  leaderWithTerm(): [Address | null, number | null] {
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
    return [leader, term];
  }

  /**
   * Returns the latest term on all nodes.
   * Note: the term may not have a leader, and so is different from the term returned by `leaderWithTerm`.
   */
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

  checkAllLeaders() {
    return this.checkNLeaders(this.servers.length);
  }

  /**
   * Check if at least `n` servers know the leader for the latest term.
   * Returns true if so.
   * Throws an error if any node knows a different leader.
   */
  checkNLeaders(n: number) {
    const [leader, term] = this.leaderWithTerm();
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
    return this.servers.filter((s) => s.leader === leader && s.term === term).length >= n;
  }

  connect(addr: Address) {
    this.logger('connecting ' + addr);
    this.net.connect(addr);
  }

  disconnect(addr: Address) {
    this.logger('disconnecting ' + addr);
    this.net.disconnect(addr);
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
    if(!this.net.connections.has(toConnectionKey(this.myAddress, to))) {
      if(this.logger.enabled) {
        this.logger("DROPPED %s->%s %s", this.myAddress, to, JSON.stringify(request));
      }
      return await new Promise(() => {});
    }
    await delay(randomInRange(0, 10));
    if(this.logger.enabled) {
      this.logger("%s->%s %s", this.myAddress, to, JSON.stringify(request));
    }
    const response = await otherNode.receiver.handleMessage(
      this.myAddress,
      request
    );
    if(!this.net.connections.has(toConnectionKey(to, this.myAddress))) {
      if(this.logger.enabled) {
        this.logger("DROPPED %s->%s %s", to, this.myAddress, JSON.stringify(response));
      }
      return await new Promise(() => {});
    }
    await delay(randomInRange(0, 10));
    if(this.logger.enabled) {
      this.logger("%s->%s %s", to, this.myAddress, JSON.stringify(response));
    }
    return response;
  }
}

export class MemNetwork {
  public nodes: Map<Address, MemTransport> = new Map();
  public connections: Set<string> = new Set();

  constructor(public addrs: Address[], logger: Logger) {
    for (const addr of this.addrs) {
      this.nodes.set(addr, new MemTransport(this, addr, logger));
      this.connect(addr);
    }
  }

  connect(addr: Address) {
    for (const addr2 of this.addrs) {
      this.connections.add(toConnectionKey(addr, addr2));
      this.connections.add(toConnectionKey(addr2, addr));
    }
  }

  disconnect(addr: Address) {
    for (const addr2 of this.addrs) {
      this.connections.delete(toConnectionKey(addr, addr2));
      this.connections.delete(toConnectionKey(addr2, addr));
    }
  }
}

function toConnectionKey(addr: string, addr2: string) {
  return addr + ' ' + addr2;
}

const SLOW = false;

const heartbeatInterval = SLOW ? 500 : 50;
const minElectionTimeout = SLOW ? 1000 : 100;
const maxElectionTimeout = SLOW ? 2000 : 200;

const ITERATIONS = 1000;

type ClusterConfig = {
  numServers?: number;
  heartbeatInterval?: number;
  minElectionTimeout?: number;
  maxElectionTimeout?: number;

  /** Allow only the first node to become leader by disabling election timers for all other nodes. */
  oneLeader?: boolean;
};

function setupCluster(config: ClusterConfig = {}) {
  const addrs = Array.from(Array(config.numServers || 3).keys()).map(
    (index) => "s" + (index + 1)
  );
  return new Cluster(addrs, config);
}

async function waitFor<T>(s: Scheduler, fn: () => T, logger?: Logger): Promise<T> {
  let result;
  let steps = 0;
  while(!(result = fn())) {
    if(steps++ >= 5000) {
      if(logger) {
        logger("waitFor: timeout waiting for " + fn);
      }
      throw new Error("waitFor: timeout waiting for " + fn);
    }
    await s.step();
    if(s.finished()) {
      throw new Error("waitFor(" + fn + "): process finished");
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
  test("with all nodes", async () => {
    for(let i = 1; i <= ITERATIONS; i++) {
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

  test("with one faulty node", async () => {
    for(let i = 1; i <= ITERATIONS; i++) {
      try {
        const s = new Scheduler;
        s.install();
        defer(async () => { await s.step(); s.uninstall() });
        const c = setupCluster();
        defer(() => c.stop());
        const debug = c.logger.extend("test");
        debug("========= ITERATION %d ==========", i);
        c.disconnect("s3");
        await waitFor(s, () => c.checkNLeaders(2), c.logger);
      } finally {
        await flushDeferred();
      }
    }
  });

  test("can reelect a leader", async () => {
    for(let i = 1; i <= ITERATIONS; i++) {
      try {
        const s = new Scheduler;
        s.install();
        defer(async () => { await s.step(); s.uninstall() });
        const c = setupCluster();
        defer(() => c.stop());
        const debug = c.logger.extend("test");
        debug("========= ITERATION %d ==========", i);
        const firstLeader = await waitFor(s, () => c.leader(), c.logger);
        c.disconnect(firstLeader!);
        await waitFor(s, () => c.leader() !== firstLeader, c.logger);
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
    for(let i = 1; i <= ITERATIONS; i++) {
      try {
        const s = new Scheduler;
        s.install();
        defer(async () => { await s.step(); s.uninstall() });
        const c = setupCluster({ oneLeader: true });
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

  it("can replicate when one node is disconnected", async () => {
    for(let i = 1; i <= ITERATIONS; i++) {
      try {
        const s = new Scheduler;
        s.install();
        defer(async () => { await s.step(); s.uninstall() });
        const c = setupCluster({ oneLeader: true });
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

        function waitForLogsToConverge(servers: Raft[] = c.servers) {
          return waitFor(s, () => {
            for (const s of servers) {
              if (!deepEquals(s.getLog(), expectedLog)) {
                return false;
              }
            }
            return true;
          });
        }
        await waitForLogsToConverge();
        c.disconnect("s3");

        debug('proposing 2');

        // Another entry, but without one node
        const entry2 = await leader.propose("World");
        expectedLog.push([entry2.term, "World"]);
        await waitForLogsToConverge([c.server("s2")]);
      } finally {
        await flushDeferred();
      }
    }
  });

  it("won't elect leader with incomplete log", async () => {
    for(let i = 1; i <= ITERATIONS; i++) {
      try {
        const s = new Scheduler;
        s.install();
        defer(async () => { await s.step(); s.uninstall() });
        const c = setupCluster({ oneLeader: true });
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

        function waitForLogsToConverge(servers: Raft[] = c.servers) {
          return waitFor(s, () => {
            for (const s of servers) {
              if (!deepEquals(s.getLog(), expectedLog)) {
                return false;
              }
            }
            return true;
          });
        }
        await waitForLogsToConverge();
        c.disconnect("s3");

        debug('proposing 2');

        // Another entry, but without one node
        const entry2 = await leader.propose("World");
        expectedLog.push([entry2.term, "World"]);
        await waitForLogsToConverge([c.server("s2")]);

        debug('forcing reelection');

        c.disconnect('s1');
        c.connect('s3');
        c.server('s2')!.electionDisabled.set(false);
        c.server('s3')!.electionDisabled.set(false);
        await waitFor(s, () => c.leader() !== 's1', c.logger);
        expect(c.leader()).toEqual('s2');
      } finally {
        await flushDeferred();
      }
    }
  });
});
