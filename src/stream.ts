import { AsyncQueue } from './queue'

async function* generator<T, N>(values: T[] | AsyncIterator<T>): AsyncGenerator<T> {
  const iterator = Array.isArray(values) ? values.values() : values
  let result = await iterator.next()
  while (!result.done) {
    yield result.value
    result = await iterator.next()
  }
}

export class AsyncStream<T> implements AsyncIterator<T> {
  private iterator: AsyncGenerator<T>

  constructor(values: T[] | AsyncIterator<T>) {
    this.iterator = generator(values)
  }

  [Symbol.asyncIterator](): AsyncStream<T> {
    return new AsyncStream(this.iterator)
  }

  next(): Promise<IteratorResult<T>> {
    return this.iterator.next()
  }

  map<O>(fn: (value: T, index: number) => O | Promise<O>): AsyncStream<O> {
    const iter = this.iterator

    const gen = async function* () {
      let index = 0
      for await (const element of iter) {
        yield await fn(element, index++)
      }
    }
    return new AsyncStream(gen())
  }

  parallelMap<O>(concurrency: number, fn: (value: T) => O | Promise<O>): AsyncStream<O> {
    const iter = this.iterator
    const queue = new AsyncQueue<IteratorResult<O>>(concurrency)

    const offer = async function () {
      for await (const element of iter) {
        await queue.offer(async () => ({
          done: false,
          value: await fn(element),
        }))
      }
      await queue.offer(() => ({
        done: true,
        value: undefined,
      }))
    }

    const gen = async function* () {
      offer()
      let result = await queue.poll()
      while (!result.done) {
        yield result.value
        result = await queue.poll()
      }
    }
    return new AsyncStream(gen())
  }

  flatMap<O>(fn: (value: T, index: number) => O[] | AsyncIterator<O>): AsyncStream<O> {
    const iter = this.iterator

    const gen = async function* () {
      let index = 0
      for await (const element of iter) {
        yield* generator(fn(element, index++))
      }
    }
    return new AsyncStream(gen())
  }

  sliding(size: number): AsyncStream<T[]> {
    const iter = this.iterator

    const gen = async function* () {
      let chunk: T[] = []
      for await (const element of iter) {
        chunk.push(element)
        if (chunk.length === size) {
          yield chunk
          chunk = []
        }
      }
      if (chunk.length > 0) {
        yield chunk
      }
    }
    return new AsyncStream(gen())
  }

  async toArray(): Promise<T[]> {
    const values: T[] = []
    for await (const value of this.iterator) {
      values.push(value)
    }
    return values
  }
}
