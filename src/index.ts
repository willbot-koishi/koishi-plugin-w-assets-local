import path from 'node:path'
import { createHash, createHmac } from 'node:crypto'
import { Readable, Transform } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { Blob, Buffer } from 'node:buffer'

import type {} from '@koishijs/plugin-server'
import { Context, sanitize, Schema as z, $ } from 'koishi'
import AssetsPro, { AssetCreateInfo, AssetInfo, AssetLife, AssetUsageInfo, GcOptions, GcResult } from 'koishi-plugin-w-assets-core'

import type Koa from 'koa'
import type {} from 'koa-body'
import FileType from 'file-type'
import mime from 'mime-types'
import fs from 'fs-extra'
import { newQueue } from '@henrygd/queue'

import { unreachable, zJSON, zNumberString, zPosint, zValidator, Ok, Err, NullWritable } from './utils'

declare module 'koishi' {
  interface Tables {
    'w-assets-stats': AssetsPro.Stats
    'w-assets-local-stats': AssetsProLocal.Stats
    'w-assets-info': AssetInfo
  }
}

class AssetsProLocal extends AssetsPro<AssetsProLocal.Config> {
  static inject = ['server', 'database']

  protected rootPath: string
  protected selfUrl: string | undefined
  protected path: string | undefined
  protected baseUrl: string
  protected noServer = false
  protected usedNonces = new Map<string, number>()

  constructor(ctx: Context, config: AssetsProLocal.Config) {
    super(ctx, config)

    ctx.model.extend('w-assets-stats', {
      id: 'unsigned',
      assetCount: 'unsigned',
      assetSize: 'unsigned',
    })

    ctx.model.extend('w-assets-local-stats', {
      id: 'unsigned',
      prevDefaultLife: 'unsigned',
    })

    ctx.model.extend('w-assets-info', {
      id: 'string',
      categoryId: 'string',
      name: 'string',
      size: 'unsigned',
      type: 'string',
      checksum: 'string',
      sourceUrl: 'string',
      createdAt: 'unsigned',
      life: 'integer',
      expiresAt: 'unsigned',
    }, {
      primary: 'id',
    })

    this.rootPath = path.resolve(ctx.baseDir, config.baseDir)
    this.selfUrl = config.selfUrl || ctx.server.config.selfUrl

    if (this.selfUrl && config.path) {
      this.path = sanitize(config.path)
      this.baseUrl = new URL(this.path, this.selfUrl).href
    }
    else {
      this.ctx.logger.info('missing config "selfUrl", fallback to "file:" scheme')
      this.baseUrl = 'file://'
      this.noServer = true
    }
  }

  protected async start() {
    await Promise.all([
      this.initFilesystem(),
      this.initDatabase(),
      this.noServer || this.initServer(),
    ])
  }

  protected resolve(id: string) {
    return path.resolve(this.rootPath, id)
  }

  url(id: string) {
    return `${this.baseUrl}/${id}`
  }

  async uploadFromUrl(url: string, assetCreate: AssetCreateInfo): Promise<AssetUsageInfo> {
    const file = await this.ctx.http.file(url)
    const buffer = Buffer.from(file.data)

    return this.uploadFromFile(buffer, {
      type: file.type,
      ...assetCreate,
      sourceUrl: url
    })
  }

  protected calcExpiresAt(createdAt: number, life: number): number {
    if (life === AssetLife.Auto) life = this.config.gc.defaultLife
    else if (life === AssetLife.Permanent) return 0
    return createdAt + life
  }

  protected async updateExpiresAtForAutoLifeAssets() {
    const life = this.config.gc.defaultLife
    await Promise.all([
      this.ctx.database.set('w-assets-info', { life: AssetLife.Auto }, row => ({
        expiresAt: $.add(row.createdAt, life),
      })),
      this.ctx.database.set('w-assets-local-stats', 1, {
        prevDefaultLife: this.config.gc.defaultLife,
      }),
    ])
  }

  async uploadFromFile(file: Blob | Buffer | Readable | string, { ...assetCreate }: AssetCreateInfo): Promise<AssetUsageInfo> {
    const id = crypto.randomUUID()
    const assetPath = this.resolve(id)

    // Convert file input to a readable stream
    const fileStream: Readable = (
      file instanceof Readable ? file :
      Buffer.isBuffer(file) ? Readable.from(file) :
      typeof file === 'string' ? fs.createReadStream(file) :
      file instanceof Blob ? Readable.fromWeb(file.stream()) :
      unreachable(file)
    )

    // Create a transform stream to calculate hash and size
    const hash = createHash('sha256')
    let size = 0
    const analyze = new Transform({
      transform(chunk: Buffer, _, callback) {
        hash.update(chunk)
        size += chunk.byteLength
        callback(null, chunk)
      },
    })

    // Create a writable stream to save the file if it's not already on disk
    const sinkStream = typeof file === 'string'
      ? new NullWritable()
      : fs.createWriteStream(assetPath)

    // Run the pipeline
    await pipeline(
      fileStream,
      analyze,
      sinkStream,
    )

    // Get checksum and check for duplicates
    const checksum = hash.digest('hex')

    const [duplicate] = await this.ctx.database.get('w-assets-info', { checksum })
    if (duplicate) {
      if (typeof file !== 'string') await fs.rm(assetPath, { force: true })
      return { ...duplicate, url: this.url(duplicate.id) }
    }

    // Move the file if it was uploaded from existing path
    if (typeof file === 'string') await fs.rename(file, assetPath)

    // Detect the file type if not provided
    if (! assetCreate.type) {
      const fileType = await FileType.fromFile(assetPath)
      assetCreate.type = fileType?.ext
    }

    // Ensure the asset has a name and an extension if possible
    let name = assetCreate.name ??= id
    if (! name.includes('.') && assetCreate.type) {
      const extension = mime.extension(assetCreate.type)
      name += `.${extension}`
    }

    const life = assetCreate.life ?? AssetLife.Auto
    const createdAt = Date.now()
    const expiresAt = this.calcExpiresAt(createdAt, life)
    const asset: AssetInfo = {
      ...assetCreate,
      name,
      checksum,
      id,
      size,
      life,
      createdAt,
      expiresAt,
    }
    await Promise.all([
      this.ctx.database.create('w-assets-info', asset),
      this.ctx.database.set('w-assets-stats', 1, row => ({
        assetCount: $.add(row.assetCount, 1),
        assetSize: $.add(row.assetSize, asset.size),
      })),
    ])

    return { ...asset, url: this.url(asset.id) }
  }

  async delete(id: string): Promise<boolean> {
    const [asset] = await this.ctx.database.get('w-assets-info', { id })
    if (! asset) return false

    await fs.rm(this.resolve(id), { force: true })
    await Promise.all([
      this.ctx.database.remove('w-assets-info', { id }),
      this.ctx.database.set('w-assets-stats', 1, row => ({
        assetCount: $.sub(row.assetCount, 1),
        assetSize: $.sub(row.assetSize, asset.size),
      }))
    ])

    return true
  }

  async gc({}: GcOptions): Promise<GcResult> {
    const expiredAssets = await this.ctx.database.get('w-assets-info', {
      expiresAt: { $gt: 0, $lte: Date.now() },
    })
    if (! expiredAssets.length) return { count: 0, size: 0 }

    // Remove all the files
    const rmQueue = newQueue(this.config.gc.concurrency)
    await rmQueue.all(expiredAssets.map((asset =>
      fs.rm(this.resolve(asset.id), { force: true })
    )))

    // Remove the metadata records and update stats
    const ids = expiredAssets.map(asset => asset.id)
    const size = expiredAssets.reduce((total, asset) => total + asset.size, 0)
    await Promise.all([
      this.ctx.database.remove('w-assets-info', { id: { $in: ids } }),
      this.ctx.database.set('w-assets-stats', 1, row => ({
        assetCount: $.sub(row.assetCount, expiredAssets.length),
        assetSize: $.sub(row.assetSize, size),
      })),
    ])

    return { count: expiredAssets.length, size }
  }

  async stats() {
    const [stats] = await this.ctx.database.get('w-assets-stats', 1)
    return stats
  }

  protected async initFilesystem() {
    await fs.ensureDir(this.rootPath)
  }

  protected async initDatabase() {
    const [stats] = await this.ctx.database.get('w-assets-stats', 1)
    if (! stats) await this.ctx.database.create('w-assets-stats', {
      id: 1,
      assetCount: 0,
      assetSize: 0,
    })

    const [localStats] = await this.ctx.database.get('w-assets-local-stats', 1)
    if (! localStats) await this.ctx.database.create('w-assets-local-stats', {
      id: 1,
      prevDefaultLife: this.config.gc.defaultLife,
    })
    else if (localStats.prevDefaultLife !== this.config.gc.defaultLife) {
      await this.updateExpiresAtForAutoLifeAssets()
    }
  }

  protected async streamAsset(ktx: Koa.Context, asset: AssetInfo | undefined) {
    if (! asset) {
      ktx.status = 404
      return ktx.body = Err('Asset not found')
    }

    const assetPath = this.resolve(asset.id)
    const stream = fs.createReadStream(assetPath)
    if (asset.type) ktx.type = asset.type
    ktx.attachment(asset.name, { type: ktx.query.inline === '1' ? 'inline' : 'attachment' })
    return ktx.body = stream
  }

  protected auth(ktx: Koa.Context): boolean {
    const signature = ktx.headers['x-signature']
    const nonce = ktx.headers['x-nonce']
    const timestamp = ktx.headers['x-timestamp']

    if (typeof signature !== 'string' || typeof nonce !== 'string' || typeof timestamp !== 'string') {
      ktx.status = 400
      ktx.body = Err('Missing authentication headers')
      return false
    }

    const time = Number(timestamp)
    if (isNaN(time)) {
      ktx.status = 400
      ktx.body = Err('Invalid timestamp')
      return false
    }

    const timeDiff = Math.abs(Date.now() - time)
    if (timeDiff > this.config.nonceExpire) {
      ktx.status = 401
      ktx.body = Err('Request expired')
      return false
    }

    if (this.usedNonces.has(nonce)) {
      ktx.status = 401
      ktx.body = Err('Nonce already used')
      return false
    }

    const expectedSignature = createHmac('sha256', this.config.secret)
      .update(nonce + timestamp)
      .digest('hex')

    if (signature !== expectedSignature) {
      ktx.status = 401
      ktx.body = Err('Invalid signature')
      return false
    }

    this.usedNonces.set(nonce, Date.now())
    return true
  }

  protected cleanupUsedNonces() {
    const now = Date.now()
    for (const [nonce, timestamp] of this.usedNonces) {
      if (now - timestamp > this.config.nonceExpire) {
        this.usedNonces.delete(nonce)
      }
    }
  }

  protected useAuth(middleware: Koa.Middleware): Koa.Middleware {
    return (ktx, next) => {
      if (! this.auth(ktx)) return
      return middleware(ktx, next)
    }
  }

  protected useCatch(middleware: Koa.Middleware): Koa.Middleware {
    return async (ktx, next) => {
      try {
        return await middleware(ktx, next)
      }
      catch (err) {
        if (err instanceof z.ValidationError) {
          ktx.status = 400
          return ktx.body = Err('Invalid request data', {
            details: err.message,
          })
        }
        throw err
      }
    }
  }

  protected route(method: 'get' | 'post' | 'put' | 'delete', routePath: string, middleware: Koa.Middleware) {
    this.ctx.server[method](`${this.path}${routePath}`, this.useCatch(middleware))
  }

  protected initServer() {
    this.ctx.setInterval(() => {
      this.cleanupUsedNonces()
    }, this.config.nonceExpire)

    this.route('get', '/', this.useAuth(async (ktx) => {
      const { page, limit } = Protocol.zListQuery(ktx.query)

      const assets = await this.ctx.database.get('w-assets-info', {}, {
        limit,
        offset: (page - 1) * limit,
      })
      return ktx.body = Ok({ assets })
    }))

    // GET /stats - get asset stats
    this.route('get', '/stats', this.useAuth(async (ktx) => {
      return ktx.body = Ok(await this.stats())
    }))

    // POST /gc - garbage collect expired assets
    this.route('post', '/gc', this.useAuth(async (ktx) => {
      return ktx.body = Ok(await this.gc({}))
    }))

    // GET /:id - download asset
    this.route('get', '/:id', async (ktx) => {
      const { id } = Protocol.zAssetIdParams(ktx.params)
      const [asset] = await this.ctx.database.get('w-assets-info', { id })
      return this.streamAsset(ktx, asset)
    })

    // POST / - upload asset
    this.route('post', '/', this.useAuth(async (ktx: Koa.Context) => {
      // upload by file if multipart

      if (ktx.is('multipart')) {
        const { info = {} } = Protocol.zUploadFromFileBody(ktx.request.body)

        const file = ktx.request.files?.['file']

        if (! file) {
          ktx.status = 400
          return ktx.body = Err('Missing file')
        }

        if (Array.isArray(file)) {
          ktx.status = 400
          return ktx.body = Err('Multiple file upload is not supported')
        }

        info.name ??= file.originalFilename ? path.basename(file.originalFilename) : undefined
        const asset = await this.uploadFromFile(file.filepath, info)
        return ktx.body = Ok(asset)
      }

      // upload by url otherwise
      const { info = {}, url } = Protocol.zUploadFromUrlBody(ktx.request.body)
      const asset = await this.uploadFromUrl(url, info)
      return ktx.body = Ok(asset)
    }))

    // DELETE /:id - delete asset
    this.route('delete', '/:id', this.useAuth(async (ktx) => {
      const { id } = Protocol.zAssetIdParams(ktx.params)
      const success = await this.delete(id)
      if (! success) {
        ktx.status = 404
        return ktx.body = Err('Asset not found')
      }
      return ktx.body = Ok()
    }))
  }
}

namespace AssetsProLocal {
  export interface GcConfig {
    concurrency: number
    defaultLife: number
  }

  export const GcConfig: z<GcConfig> = z.object({
    concurrency: z.number().default(10),
    defaultLife: z.number().default(7 * 24 * 60 * 60 * 1000),
  })

  export interface Config extends AssetsPro.Config {
    baseDir: string
    selfUrl: string
    path: string
    secret: string
    nonceExpire: number

    gc: GcConfig
  }

  export const Config: z<Config> = z.object({
    whitelist: z.array(z.string()).default([]),
    baseDir: z.string().default('data/assets'),
    selfUrl: z.string().default(''),
    path: z.string().default('/assets'),
    secret: z.string().required(),
    nonceExpire: z.number().default(5 * 60 * 1000),

    gc: GcConfig,
  })

  export interface Stats {
    id: 1
    prevDefaultLife: number
  }
}

export namespace Protocol {
  export interface ListQuery {
    page: number
    limit: number
  }

  export const DEFAULT_LIST_LIMIT = 20

  export const zListQuery = zValidator<ListQuery>(z.object({
    page: zNumberString(zPosint()).default('1'),
    limit: zNumberString(zPosint()).default(`${DEFAULT_LIST_LIMIT}`),
  }))

  export const zAssetCreateInfo: z<AssetCreateInfo> = z.object({
    name: z.string(),
    type: z.string(),
    categoryId: z.string(),
    life: z.number(),
  })

  export interface UploadFromUrlBody {
    url: string
    info?: AssetCreateInfo,
  }

  export const zUploadFromUrlBody = zValidator<UploadFromUrlBody>(z.object({
    url: z.string().required(),
    info: zAssetCreateInfo,
  }))

  export const zUploadFromFileBody = zValidator(z.object({
    info: zJSON(zAssetCreateInfo),
  }))

  export interface AssetIdParams {
    id: string
  }

  export const zAssetIdParams = zValidator<AssetIdParams>(z.object({
    id: z.string().required(),
  }))
}

export default AssetsProLocal
