/**
 * In-memory S3 mock that supports the subset of the S3 API used by S3SQLiteDB:
 *   - GetObjectCommand
 *   - PutObjectCommand  (with IfMatch / IfNoneMatch conditional writes)
 *   - HeadObjectCommand
 *
 * ETags are computed as a simple hex counter so they are stable and
 * deterministic across a single test run.
 */
import {
  GetObjectCommand,
  PutObjectCommand,
  HeadObjectCommand,
} from '@aws-sdk/client-s3';
import { createHash } from 'node:crypto';
import { Readable } from 'node:stream';
import { sdkStreamMixin } from '@smithy/util-stream';

interface StoredObject {
  body: Buffer;
  etag: string;
  contentType: string;
}

// Minimal shape of the commands we care about.
type AnyCommand = GetObjectCommand | PutObjectCommand | HeadObjectCommand;

export class MockS3Client {
  private store = new Map<string, StoredObject>();

  /** Expose the underlying store for test assertions. */
  get objects(): ReadonlyMap<string, StoredObject> {
    return this.store;
  }

  // S3Client.send() duck-type
  async send(command: AnyCommand): Promise<unknown> {
    if (command instanceof GetObjectCommand) return this._get(command);
    if (command instanceof PutObjectCommand) return this._put(command);
    if (command instanceof HeadObjectCommand) return this._head(command);
    throw new Error(`MockS3Client: unsupported command ${command.constructor.name}`);
  }

  /** Pre-populate the store with an existing object (useful in tests). */
  seed(key: string, body: Buffer): string {
    const etag = this._etag(body);
    this.store.set(key, { body, etag, contentType: 'application/octet-stream' });
    return etag;
  }

  /** Delete a stored object (simulate external deletion). */
  delete(key: string): void {
    this.store.delete(key);
  }

  /** Replace content without going through the conditional-write path. */
  overwrite(key: string, body: Buffer): string {
    const etag = this._etag(body);
    this.store.set(key, { body, etag, contentType: 'application/octet-stream' });
    return etag;
  }

  // -------------------------------------------------------------------------

  private _get(cmd: GetObjectCommand): unknown {
    const { Key } = cmd.input;
    const obj = this.store.get(Key!);
    if (!obj) {
      const err = Object.assign(new Error(`No such key: ${Key}`), {
        name: 'NoSuchKey',
        $metadata: { httpStatusCode: 404 },
      });
      throw err;
    }

    // Wrap the Buffer in a Readable that also has the smithy stream mixin so
    // that `transformToByteArray()` works (just like the real SDK does).
    const readable = Readable.from(obj.body);
    const body = sdkStreamMixin(readable);

    return { Body: body, ETag: obj.etag, ContentLength: obj.body.length };
  }

  private _put(cmd: PutObjectCommand): unknown {
    const { Bucket: _bucket, Key, Body, ContentType, IfMatch, IfNoneMatch } =
      cmd.input;

    const existing = this.store.get(Key!);

    // Conditional write: If-None-Match: * → fail if object already exists
    if (IfNoneMatch === '*' && existing) {
      throw Object.assign(
        new Error(`ConditionalRequestConflict: object already exists at ${Key}`),
        { name: 'ConditionalRequestConflict', $metadata: { httpStatusCode: 412 } },
      );
    }

    // Conditional write: If-Match: <etag> → fail if ETag doesn't match
    if (IfMatch !== undefined) {
      if (!existing || existing.etag !== IfMatch) {
        throw Object.assign(
          new Error(
            `PreconditionFailed: ETag mismatch for ${Key}. ` +
              `Expected ${IfMatch}, got ${existing?.etag ?? '(none)'}`,
          ),
          { name: 'PreconditionFailed', $metadata: { httpStatusCode: 412 } },
        );
      }
    }

    const bodyBuf =
      Body instanceof Buffer
        ? Body
        : Buffer.isBuffer(Body)
          ? Body
          : Buffer.from(Body as Uint8Array);

    const etag = this._etag(bodyBuf);
    this.store.set(Key!, {
      body: bodyBuf,
      etag,
      contentType: ContentType ?? 'application/octet-stream',
    });

    return { ETag: etag, $metadata: { httpStatusCode: 200 } };
  }

  private _head(cmd: HeadObjectCommand): unknown {
    const { Key } = cmd.input;
    const obj = this.store.get(Key!);
    if (!obj) {
      throw Object.assign(new Error(`No such key: ${Key}`), {
        name: 'NoSuchKey',
        $metadata: { httpStatusCode: 404 },
      });
    }
    return { ETag: obj.etag, ContentLength: obj.body.length };
  }

  private _etag(body: Buffer): string {
    // Use MD5 in quotes — identical to what S3 returns for un-chunked objects.
    return `"${createHash('md5').update(body).digest('hex')}"`;
  }
}
