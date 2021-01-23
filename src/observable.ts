/**
 * OVar - "observable variable".
 * A variable whose changes you can listen to.
 */
export default class OVar<T> {
  private listeners: ((v: T) => void)[] = [];
  private static nextId = 1;
  private id = ++OVar.nextId;

  constructor(private value: T) {}

  addListener(l: (v: T) => void) {
    this.listeners.push(l);
  }

  removeListener(l: (v: T) => void) {
    const i = this.listeners.indexOf(l);
    if (i !== -1) {
      this.listeners.splice(i, 1);
    }
  }

  private static collectingVarsToWatch: Map<number, OVar<any>> | null = null;

  get() {
    if (OVar.collectingVarsToWatch) {
      OVar.collectingVarsToWatch.set(this.id, this);
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

  private static evalWithDependencies<T>(fn: () => T): [T, OVar<any>[]] {
    if (OVar.collectingVarsToWatch) {
      throw new Error("Already collecting dependencies");
    }
    OVar.collectingVarsToWatch = new Map();
    try {
      const value = fn();
      const deps = Array.from(OVar.collectingVarsToWatch.values());
      return [value, deps];
    } finally {
      OVar.collectingVarsToWatch = null;
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
        return "timeout";
      }
      const [value, deps] = this.evalWithDependencies(fn);
      try {
        if (value) {
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
