/// OVar - "observable variable"

export default class OVar<T> {
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
    process.nextTick(() => {
      for(const l of this.listeners) {
        l(v);
      }
    });
  }

  // Wait for the next change
  wait(): Promise<void> {
    return new Promise(resolve => {
      const l = () => {
        this.removeListener(l);
        resolve();
      };
      this.addListener(l);
    });
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
