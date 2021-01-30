import { debug as makeDebug } from "debug";

/**
 * OVar - "observable variable".
 * A variable whose changes you can listen to.
 */
export class OVar<T> {
  private listeners: ((v: T) => void)[] = [];
  private static nextId = 1;
  private id = ++OVar.nextId;

  public static logger = makeDebug("__disabled__");

  constructor(private value: T, public name: string | null = null) {}

  describe() {
    return this.name || "var" + this.id;
  }

  addListener(l: (v: T) => void) {
    this.listeners.push(l);
  }

  removeListener(l: (v: T) => void) {
    const i = this.listeners.indexOf(l);
    if (i !== -1) {
      this.listeners.splice(i, 1);
    }
  }

  get __numListeners() {
    return this.listeners.length;
  }

  private static collectingVarsToWatch: boolean = false;
  private static varsToWatch: Map<number, OVar<any>> = new Map;

  get() {
    if (OVar.collectingVarsToWatch) {
      OVar.logger("adding %s to deps", this.describe());
      OVar.varsToWatch.set(this.id, this);
    }
    return this.value;
  }

  set(v: T) {
    if (OVar.collectingVarsToWatch) {
      throw new Error("Can't modify OVars while reading");
    }
    this.value = v;
    process.nextTick(() => {
      for (const l of this.listeners) {
        l(v);
      }
    });
  }

  /* private API */
  static __evalWithDependencies<T>(fn: () => T): [T, OVar<any>[]] {
    if (OVar.collectingVarsToWatch) {
      throw new Error("Already collecting dependencies");
    }
    OVar.collectingVarsToWatch = true;
    try {
      if(OVar.logger.enabled) {
        OVar.logger("evaluating %s", fn);
      }
      const value = fn();
      const deps = Array.from(OVar.varsToWatch.values());
      if(OVar.logger.enabled) {
        OVar.logger(
          "evaluated %s deps: %O",
          fn,
          deps.map((v) => v.describe())
        );
      }
      return [value, deps];
    } finally {
      OVar.collectingVarsToWatch = false;
      OVar.varsToWatch.clear();
    }
  }

  /**
   * Wait until `fn` returns a truthy value, or, if timeout is specified, the timeout passes.
   *
   * `fn` may read OVars. If one of them changes while we're waiting, `fn` will be reevaluated.
   *
   * If timeout happens, this function will return the special value `'timeout'`.
   */
  static async waitFor<T>(
    fn: () => T,
    timeout?: number
  ): Promise<T | "timeout"> {
    let listener: () => void = () => {};
    let timedOut = false;
    const timeoutId = timeout
      ? setTimeout(() => {
          timedOut = true;
          listener();
        }, timeout)
      : null;
    while (true) {
      if (timedOut) {
        OVar.logger("waitFor %s timed out", fn);
        return "timeout";
      }
      const [value, deps] = OVar.__evalWithDependencies(fn);
      try {
        if (value) {
          OVar.logger("waitFor %s finished with %O", fn, value);
          return value;
        }
        let scheduled = false;
        await new Promise((resolve) => {
          listener = () => {
            if (timeoutId && !timedOut) {
              clearTimeout(timeoutId);
            }
            if (!scheduled) {
              scheduled = true;
              resolve(void 0);
            }
          };
          for (const dep of deps) {
            dep.addListener(listener);
          }
        });
      } finally {
        if (listener) {
          for (const dep of deps) {
            dep.removeListener(listener);
          }
        }
      }
    }
  }
}

export default OVar;

export const waitFor = OVar.waitFor.bind(OVar);
