"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const setTimeout = this.setTimeout;
class NullReceiver {
    handleMessage() { }
}
class TransportError extends Error {
}
class MemTransport {
    constructor(net, myAddress) {
        this.net = net;
        this.myAddress = myAddress;
        this.receiver = new NullReceiver();
    }
    setReceiver(receiver) {
        this.receiver = receiver;
    }
    rpc(to, request) {
        const otherNode = this.net.nodes.get(to);
        if (!otherNode) {
            throw new TransportError('unknown_address');
        }
        return otherNode.receiver.handleMessage(this.myAddress, request);
    }
}
class MemNetwork {
    constructor(addrs) {
        this.nodes = new Map();
        for (const addr of addrs) {
            this.nodes.set(addr, new MemTransport(this, addr));
        }
    }
}
/// OVar - "observable variable"
class OVar {
    constructor(value) {
        this.value = value;
        this.listeners = [];
    }
    addListener(l) {
        this.listeners.push(l);
    }
    removeListener(l) {
        const i = this.listeners.indexOf(l);
        if (i !== -1) {
            this.listeners.splice(i, 1);
        }
    }
    get() {
        return this.value;
    }
    set(v) {
        this.value = v;
        setTimeout(() => {
            for (const l of this.listeners) {
                l(v);
            }
        }, 0);
    }
    waitFor(predicate) {
        return new Promise(resolve => {
            if (predicate(this.get())) {
                resolve();
                return;
            }
            const l = (v) => {
                if (predicate(v)) {
                    this.removeListener(l);
                    resolve();
                }
            };
            this.addListener(l);
        });
    }
}
/// delay
function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
// A raft instance.
class Raft {
    constructor(config) {
        this.config = config;
        this.isLeader = new OVar(false);
        this.logSize = new OVar(0);
        this.matchIndex = new Map();
        this.currentTerm = 0;
        this.log = [];
        this.commitIndex = 0;
        this.transport.setReceiver(this);
    }
    get debug() {
        return this.config.logger;
    }
    get transport() {
        return this.config.transport;
    }
    get me() {
        return this.config.myAddress;
    }
    // Returns current matchIndex for the given peer, or -1 if there's none.
    getMatchIndex(peer) {
        const index = this.matchIndex.get(peer);
        return index !== undefined ? index : -1;
    }
    // Returns log term for the given log index, or -1 if there's none.
    getLogTerm(index) {
        return index >= 0 && index < this.log.length ? this.log[index][0] : -1;
    }
    rpc(to, request) {
        return this.transport.rpc(to, request);
    }
    // Receiver implementation.
    handleMessage(from, message) {
        return this.handleRequest(from, message);
    }
    /// Logic
    start() {
        for (const server of this.config.servers) {
            if (server === this.me) {
                continue;
            }
            this.replicationTask(server);
        }
        this.electionTask();
    }
    replicationTask(peer) {
        return __awaiter(this, void 0, void 0, function* () {
            while (true) {
                yield this.isLeader.waitFor(isLeader => isLeader);
                while (this.isLeader) {
                    const steppedDown = this.isLeader.waitFor(isLeader => !isLeader);
                    yield this.sendEntries(peer);
                    yield Promise.race([
                        delay(this.config.heartbeatInterval),
                        steppedDown,
                        // FIXME: this leaks subscribers
                        this.logSize.waitFor(logSize => logSize > this.getMatchIndex(peer)),
                    ]);
                }
            }
        });
    }
    sendEntries(peer) {
        return __awaiter(this, void 0, void 0, function* () {
            const debug = this.debug.extend('replication:' + peer);
            let nextIndex = this.matchIndex.has(peer) ? this.matchIndex.get(peer) + 1 : this.log.length - 1;
            while (true) {
                const targetIndex = this.log.length - 1;
                const numEntries = targetIndex - nextIndex;
                debug('attempting to replicate %d entries from index %d', numEntries, nextIndex);
                const reply = yield this.rpc(peer, {
                    type: "AppendEntries",
                    term: this.currentTerm,
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
                    debug('replicated up to %d', targetIndex);
                    this.matchIndex.set(peer, targetIndex);
                    return;
                }
                else {
                    // TODO: implement actual binary search
                    debug('peer log does not match, backtracking by 1');
                    nextIndex--;
                }
            }
        });
    }
    updateTerm(newTerm) {
        if (newTerm > this.currentTerm) {
            this.debug('our term is stale (%d > %d)', newTerm, this.currentTerm);
            // Warning: updating currentTerm and isLeader has to be atomic!
            this.currentTerm = newTerm;
            if (this.isLeader.get()) {
                this.debug('stepping down');
                this.isLeader.set(false);
            }
        }
    }
    electionTask() {
        return __awaiter(this, void 0, void 0, function* () {
            while (true) {
                yield this.isLeader.waitFor(isLeader => !isLeader);
            }
        });
    }
    // The real RPC handler, appropriately typed.
    handleRequest(from, request) {
        throw new Error('nope');
    }
}
