import delay from 'delay';

export class Scheduler {
  private nextTimerId = 1;
  private numInteractions = 0;
  public timers = new Map<number, Timer>();

  private oldGlobalSetTimeout = global.setTimeout;
  private oldGlobalClearTimeout = global.clearTimeout;
  private oldGlobalNextTick = process.nextTick;
  private oldPromiseThen = Promise.prototype.then;

  setTimeout(callback: (...args: any[]) => void, delay: number, ...args: any[]) {
    this.numInteractions++;
    const id = this.nextTimerId++;
//    console.log('setTimeout', callback.toString(), id, delay);
    this.timers.set(id, new Timer(callback.bind(null, ...args)));
    return id;
  }

  clearTimeout(id: number) {
    this.timers.delete(id);
  }

  nextTick(callback: () => void, ...args: any[]) {
    this.numInteractions++;
//    process.stdout.write("nextTick " + callback.toString() + "\n");
    this.oldGlobalNextTick(callback, ...args);
  }

  install() {
    const s = this;
    global.setTimeout = this.setTimeout.bind(this) as unknown as typeof global.setTimeout;
    global.clearTimeout = this.clearTimeout.bind(this) as unknown as typeof global.clearTimeout;
    process.nextTick = this.nextTick.bind(this);
    (Promise.prototype as any).then = function(onSuccess: (value: any) => any, onError: (value: any) => any) {
      return s.oldPromiseThen.call(this,
        value => s.nextTick(() => onSuccess(value)),
        value => s.nextTick(() => onError(value))
      );
    };
  }

  uninstall() {
    global.setTimeout = this.oldGlobalSetTimeout;
    global.clearTimeout = this.oldGlobalClearTimeout;
    process.nextTick = this.oldGlobalNextTick;
    Promise.prototype.then = this.oldPromiseThen;

  }

  /*
   * Run the event queue until there's no more timers added.
   */
  async step() {
    let lastNumInteractions;
    do {
      lastNumInteractions = this.numInteractions;
      await new Promise(resolve => this.oldGlobalNextTick(resolve));
    } while(this.numInteractions > lastNumInteractions);
    return !this.finished();
  }

  finished() {
    return this.timers.size === 0;
  }

  resumeRandom() {
    if(this.finished()) {
      throw new Error('Process finished, nothing to resume');
    }
    const id = Array.from(this.timers.keys())[Math.floor(Math.random() * this.timers.size)];
    const timer = this.timers.get(id)!;
    this.clearTimeout(id);
    timer.callback();
  }
}

class Timer {
  constructor(public callback: () => void) {}
}

/*
(async () => {
  const s = new Scheduler;
  s.install();
  try {
    (async () => {
//      await new Promise(resolve => resolve(null));
      await delay(1000);
    })();
//    delay(100);
    while(await s.step()) {
      console.log(s.timers);
      s.resumeFirst();
    }
  } catch(e) {
    console.error(e);
    process.exit(1);
  } finally {
    s.uninstall();
  }
})();
*/
