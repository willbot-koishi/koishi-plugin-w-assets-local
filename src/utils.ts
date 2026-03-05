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
export const zNumberString = (zNumber: z<number>) => z.transform(z.string(), (str, options) => {
  if (str === undefined) return zNumber()
  return zNumber(Number(str))
})
