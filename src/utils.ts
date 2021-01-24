export function randomInRange(lo: number, hi: number): number {
  return lo + Math.random() * (hi - lo);
}

export function deepEquals<T>(x: T, y: T) {
  return JSON.stringify(x) === JSON.stringify(y);
}
