import { z } from 'koishi'
import { Writable } from 'stream'

// Schema

declare global {
  interface Schemastery<S = any, T = S> {
    default(value: S): z<S, T>
  }
}

export const zValidator = <T>(schema: z<any, T>): z<any, T> => schema
export const zPosint = () => z.number().step(1).min(1)
export const zNumberString = (zNumber: z<number>) => z.transform(z.string(), (str) => {
  if (str === undefined) return zNumber()
  return zNumber(Number(str))
})


export const zJSON = <T>(zInner: z<any, T>): z<string, T> => z.transform(z.string(), (str, options) => {
  try {
    const value = JSON.parse(str)
    return zInner(value)
  }
  catch (err) {
    if (err instanceof z.ValidationError) throw err
    if (err instanceof SyntaxError) throw new z.ValidationError(`invalid json: ${err.message}`, options)
    throw err
  }
})

// Basic

export const unreachable = (_: never): never => {
  throw new Error('unreachable')
}

// Result

export const Ok = (data: object = {}) => ({ ok: 1, ...data })
export type Ok<T> = { ok: 1 } & T

export const Err = (reason: string, data: object = {}) => ({ ok: 0, reason, ...data })
export type Err<E> = { ok: 0, reason: string } & E

export type Result<T, E> = Ok<T> | Err<E>

// Stream

export class NullWritable extends Writable {
  _write(_chunk: any, _encoding: BufferEncoding, callback: (error?: Error | null) => void): void {
    callback()
  }
  _writev?(_chunks: Array<{chunk: any; encoding: BufferEncoding}>, callback: (error?: Error | null) => void): void {
    callback()
  }
}
