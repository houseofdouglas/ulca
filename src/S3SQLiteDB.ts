import Database from 'better-sqlite3';
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  PutObjectCommandInput,
  HeadObjectCommand,
} from '@aws-sdk/client-s3';
import { createHmac } from 'node:crypto';
import { writeFileSync, readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import type { OpenOptions, ExecuteResult, SqlParams } from './types.js';

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/** Thrown when an S3 conditional write is rejected (ETag changed under us). */
export class ETagMismatchError extends Error {
  constructor(
    public readonly key: string,
    public readonly expected: string | null,
  ) {
    super(
      `S3 conditional write failed for key "${key}". ` +
        `Expected ETag ${expected ?? '(none — new object)'}. ` +
        `The object was modified by another writer, or a previous write crashed mid-flight. ` +
        `The in-memory state has been refreshed from S3.`,
    );
    this.name = 'ETagMismatchError';
  }
}

/** Thrown if `hmacSecret` is too short. */
export class InsecureSecretError extends Error {
  constructor() {
    super('hmacSecret must be at least 32 characters long.');
    this.name = 'InsecureSecretError';
  }
}

// ---------------------------------------------------------------------------
// Key derivation
// ---------------------------------------------------------------------------

/**
 * Derives an opaque, unguessable S3 object key from (namespace, id, secret).
 *
 * The caller only needs to know `id` to access their own database; they
 * cannot enumerate or predict keys for other `id` values without knowing
 * `hmacSecret`.
 */
function deriveKey(namespace: string, id: string, secret: string): string {
  const hmac = createHmac('sha256', secret);
  hmac.update(`${namespace}:${id}`);
  return `dbs/${hmac.digest('hex')}.db`;
}

// ---------------------------------------------------------------------------
// S3SQLiteDB
// ---------------------------------------------------------------------------

export class S3SQLiteDB {
  private db: Database.Database | null = null;
  private etag: string | null = null; // ETag of the copy currently in /tmp
  private isNew = false; // true if no S3 object existed at open time
  private _dirty = false; // true if local writes have not yet been flushed to S3

  private constructor(
    private readonly s3: S3Client,
    private readonly bucket: string,
    private readonly s3Key: string,
    private readonly localPath: string,
    private readonly onCreateFn: ((db: Database.Database) => void) | undefined,
  ) {}

  // -------------------------------------------------------------------------
  // Factory
  // -------------------------------------------------------------------------

  /**
   * Opens (or creates) a SQLite database backed by S3.
   *
   * On cold start / first call the object is downloaded from S3 into `tmpDir`.
   * Subsequent calls on the same warm Lambda instance re-validate the cached
   * copy via HeadObject and only re-download if the ETag has changed.
   */
  static async open(opts: OpenOptions): Promise<S3SQLiteDB> {
    if (opts.hmacSecret.length < 32) throw new InsecureSecretError();

    const ns = opts.namespace ?? 'default';
    const key = deriveKey(ns, opts.id, opts.hmacSecret);
    const dir = opts.tmpDir ?? process.env['TMPDIR'] ?? tmpdir();
    // Use the hex digest directly as the filename — no slashes.
    const localPath = join(dir, key.replace(/\//g, '_'));

    const instance = new S3SQLiteDB(
      opts.s3,
      opts.bucket,
      key,
      localPath,
      opts.onCreate,
    );

    await instance._sync(/* forceDownload */ true);
    return instance;
  }

  // -------------------------------------------------------------------------
  // Public query API (synchronous — no S3 I/O)
  // -------------------------------------------------------------------------

  /**
   * Executes a SELECT and returns all matching rows.
   * Does NOT flush to S3 — read-only, always operates on the local copy.
   */
  query<T = Record<string, unknown>>(sql: string, params: SqlParams = []): T[] {
    return this._stmt(sql).all(this._bind(params)) as T[];
  }

  /**
   * Executes an INSERT / UPDATE / DELETE statement against the local SQLite
   * copy.  Does NOT flush to S3 automatically — call `flush()` when you want
   * to durably persist, or use `transaction()` which flushes on commit.
   */
  execute(sql: string, params: SqlParams = []): ExecuteResult {
    const result = this._stmt(sql).run(this._bind(params));
    this._dirty = true;
    return {
      changes: result.changes,
      lastInsertId: Number(result.lastInsertRowid),
    };
  }

  /**
   * Wraps `fn` in a SQLite transaction and, on successful commit, atomically
   * re-uploads the database to S3 using a conditional `If-Match` / `If-None-Match`
   * PUT.  If the S3 ETag has changed since we downloaded (crash recovery or
   * unexpected concurrent write) an `ETagMismatchError` is thrown after
   * re-syncing from S3 so the caller can retry with fresh state.
   */
  async transaction<T>(fn: () => T): Promise<T> {
    const db = this._requireOpen();
    const result: T = db.transaction(fn)();
    this._dirty = true;
    await this._uploadToS3();
    this._dirty = false;
    return result;
  }

  /**
   * Uploads the current local SQLite state to S3.
   * Use this after one or more bare `execute()` calls to make them durable.
   * Throws `ETagMismatchError` on conflict (crash recovery), refreshing local
   * state from S3 so you can retry.
   */
  async flush(): Promise<void> {
    this._requireOpen();
    await this._uploadToS3();
    this._dirty = false;
  }

  /**
   * Flushes pending writes to S3, then closes the local SQLite connection.
   * Call this at the end of a Lambda handler (or whenever you're done) if you
   * have outstanding `execute()` calls that have not yet been flushed.
   *
   * If there are no pending writes this is a no-op beyond closing SQLite.
   */
  async close(): Promise<void> {
    if (this.db) {
      // Only flush if there are unflushed writes.
      if (this._dirty && existsSync(this.localPath)) {
        await this._uploadToS3();
      }
      this.db.close();
      this.db = null;
      this._dirty = false;
    }
  }

  /** Returns the derived S3 object key (useful for debugging / logging). */
  get objectKey(): string {
    return this.s3Key;
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  /**
   * Downloads from S3 if needed, or creates a fresh database.
   * When `forceDownload` is true we always fetch (used at open time).
   * When false we HEAD first and skip the GET if the ETag matches our cache.
   */
  private async _sync(forceDownload: boolean): Promise<void> {
    let downloadNeeded = forceDownload;

    if (!forceDownload && this.etag && existsSync(this.localPath)) {
      // Warm-start optimisation: only re-download if the S3 object changed.
      try {
        const head = await this.s3.send(
          new HeadObjectCommand({ Bucket: this.bucket, Key: this.s3Key }),
        );
        if (head.ETag === this.etag) {
          downloadNeeded = false;
        }
      } catch {
        downloadNeeded = true; // Treat any HEAD error as "re-download"
      }
    }

    if (downloadNeeded) {
      await this._downloadFromS3();
    }

    // (Re-)open SQLite on whatever is now in localPath.
    if (this.db) {
      this.db.close();
      this.db = null;
    }
    this.db = new Database(this.localPath);
    this.db.pragma('journal_mode = DELETE'); // no WAL — single file only
    this.db.pragma('foreign_keys = ON');

    if (this.isNew && this.onCreateFn) {
      this.onCreateFn(this.db);
      this._dirty = true; // onCreate modified the DB; must be flushed
    }
  }

  private async _downloadFromS3(): Promise<void> {
    try {
      const res = await this.s3.send(
        new GetObjectCommand({ Bucket: this.bucket, Key: this.s3Key }),
      );
      const bytes = await res.Body!.transformToByteArray();
      writeFileSync(this.localPath, bytes);
      this.etag = res.ETag ?? null;
      this.isNew = false;
    } catch (err: unknown) {
      if (isNoSuchKey(err)) {
        // Brand-new database — SQLite will create the file on open.
        this.etag = null;
        this.isNew = true;
      } else {
        throw err;
      }
    }
  }

  private async _uploadToS3(): Promise<void> {
    const db = this._requireOpen();
    // Checkpoint: ensure all changes are written to the db file.
    // With DELETE journal mode there is no WAL to checkpoint, but a
    // synchronous PRAGMA ensures the file is flushed.
    db.pragma('wal_checkpoint(FULL)');

    const body = readFileSync(this.localPath);

    const putInput: PutObjectCommandInput = {
      Bucket: this.bucket,
      Key: this.s3Key,
      Body: body,
      ContentType: 'application/octet-stream',
    };

    if (this.etag) {
      // Existing object: require ETag to still match.
      putInput.IfMatch = this.etag;
    } else {
      // New object: fail if it was somehow already created.
      putInput.IfNoneMatch = '*';
    }

    try {
      const res = await this.s3.send(new PutObjectCommand(putInput));
      this.etag = res.ETag ?? null;
      this.isNew = false;
    } catch (err: unknown) {
      if (isConditionalFail(err)) {
        // Crash recovery: S3 has a version we don't know about.
        // Best-effort re-sync so the caller has fresh state after catching.
        const expectedEtag = this.etag;
        try {
          await this._sync(/* forceDownload */ true);
          this._dirty = false;
        } catch {
          // Re-sync failed (e.g. corrupt remote data). Still throw the right
          // error so the caller knows the upload was rejected, not that SQLite
          // broke. The DB remains in its pre-flush local state.
        }
        throw new ETagMismatchError(this.s3Key, expectedEtag);
      }
      throw err;
    }
  }

  private _requireOpen(): Database.Database {
    if (!this.db) throw new Error('Database is closed. Call S3SQLiteDB.open() first.');
    return this.db;
  }

  private _stmt(sql: string): Database.Statement {
    return this._requireOpen().prepare(sql);
  }

  /** Normalises SqlParams into the form better-sqlite3 accepts. */
  private _bind(params: SqlParams): unknown[] | Record<string, unknown> {
    return params;
  }
}

// ---------------------------------------------------------------------------
// Error classification helpers
// ---------------------------------------------------------------------------

function isNoSuchKey(err: unknown): boolean {
  if (!err || typeof err !== 'object') return false;
  const name = (err as { name?: string }).name ?? '';
  const code = (err as { Code?: string }).Code ?? '';
  return name === 'NoSuchKey' || code === 'NoSuchKey';
}

function isConditionalFail(err: unknown): boolean {
  if (!err || typeof err !== 'object') return false;
  const name = (err as { name?: string }).name ?? '';
  // AWS SDK v3 uses these two error names for 412 Precondition Failed.
  return name === 'PreconditionFailed' || name === 'ConditionalRequestConflict';
}
