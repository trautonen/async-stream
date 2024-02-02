type Deferred<T> = {
  promise: Promise<T>
  pending: boolean
  resolve: (value: T) => void
  reject: (reason: unknown) => void
}

const createDeferred = <T>(): Deferred<T> => {
  let pending: boolean = true
  let resolve: (value: T) => void = () => {}
  let reject: (reason: unknown) => void = () => {}
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  }).finally(() => {
    pending = false
  })
  return {
    promise,
    pending,
    resolve,
    reject,
  }
}

export class AsyncQueue<T> {
  private items: Deferred<T>[]
  private waiting: Deferred<void>[]
  private maxWorkers: number
  private workers: number

  constructor(maxWorkers: number) {
    this.items = []
    this.waiting = []
    this.maxWorkers = maxWorkers
    this.workers = 0
  }

  offer(operation: () => T | Promise<T>): Promise<void> {
    let promise = Promise.resolve()
    if (this.workers >= this.maxWorkers) {
      const waiter = createDeferred<void>()
      this.waiting.push(waiter)
      promise = waiter.promise
    }
    promise
      .then(() => {
        this.workers++
        return operation()
      })
      .then(value => this.getNextItem(true).resolve(value))
      .catch(error => this.getNextItem(true).reject(error))
    return promise
  }

  poll(): Promise<T> {
    const item = this.getNextItem(false)
    item.promise.finally(() => {
      this.items.shift()
      this.workers--
      if (this.workers < this.maxWorkers) {
        const waiter = this.waiting.shift()
        waiter?.resolve()
      }
    })
    return item.promise
  }

  private getNextItem(mustBePending: boolean): Deferred<T> {
    const existing = mustBePending ? this.items.find(item => item.pending) : this.items[0]
    if (existing) {
      return existing
    }
    const item = createDeferred<T>()
    this.items.push(item)
    return item
  }
}
