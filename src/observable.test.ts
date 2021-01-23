import { OVar, waitFor } from './observable';
import delay from 'delay';

describe("waitFor", () => {
  it("returns immediately when no vars", async () => {
    expect(await waitFor(() => true)).toEqual(true);
  });
  it("returns immediately when condition is instantly true", async () => {
    const v = new OVar(true);
    expect(await waitFor(() => v.get())).toEqual(true);
  });
  it("waits for the condition to change", async () => {
    const v = new OVar(false);
    const p = waitFor(() => v.get());
    await assertNotResolved(p);
    v.set(true);
    expect(await p).toEqual(true);
  });
  it("doesn't leak listeners", async () => {
    const v1 = new OVar(false);
    const v2 = new OVar(false);
    const p = waitFor(() => v1.get() && v2.get());
    v1.set(true);
    v2.set(true);
    expect(await p).toEqual(true);
    expect(v1.__numListeners).toEqual(0);
    expect(v2.__numListeners).toEqual(0);
  });
  it("can time out", async () => {
    const v = new OVar(false);
    expect(await waitFor(() => v.get(), 100)).toEqual('timeout');
  });
  it("doesn't leak listeners after timeout", async () => {
    const v = new OVar(false);
    await waitFor(() => v.get(), 100);
    expect(v.__numListeners).toEqual(0);
  });
  it("can resolve even when there's timeout", async () => {
    const v = new OVar(false);
    const p = waitFor(() => v.get(), 100);
    v.set(true);
    expect(await p).toEqual(true);
  });
  it("notifies multiple listeners", async () => {
    const v = new OVar('initial');
    let x1: string = '';
    let x2: string = '';
    waitFor(() => { x1 = v.get(); });
    waitFor(() => { x2 = v.get(); });
    v.set('foo');
    await delay(10);
    expect([x1, x2]).toEqual(['foo', 'foo']);
    v.set('bar');
    await delay(10);
    expect([x1, x2]).toEqual(['bar', 'bar']);
  });
});

async function assertNotResolved<T>(p: Promise<T>): Promise<void> {
  let resolved = false;
  p.then(() => resolved = true);
  await delay(100);
  if(resolved) {
    throw new Error('Expected promise not to be resolved');
  }
}
