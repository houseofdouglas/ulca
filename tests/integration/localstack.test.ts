/**
 * Integration tests against a real S3-compatible store.
 *
 * Requires either:
 *   a) LocalStack:  docker run --rm -p 4566:4566 localstack/localstack
 *   b) MinIO:       docker run --rm -p 9000:9000 -e MINIO_ROOT_USER=minioadmin \
 *                     -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
 *
 * Set env vars before running:
 *   S3_ENDPOINT   default: http://localhost:4566  (LocalStack)
 *   S3_REGION     default: us-east-1
 *   S3_BUCKET     default: s3sqlite-integration
 *   AWS_ACCESS_KEY_ID      default: test
 *   AWS_SECRET_ACCESS_KEY  default: test
 *
 * Run with:  npm run test:integration
 * (INTEGRATION=true vitest run tests/integration)
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { S3Client, CreateBucketCommand, DeleteObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { S3SQLiteDB } from '../../src/S3SQLiteDB.js';

if (!process.env['INTEGRATION']) {
  // Skip entire file when not running integration tests.
  describe.skip('integration (set INTEGRATION=true to enable)', () => {
    it('skipped', () => {});
  });
} else {
  const ENDPOINT = process.env['S3_ENDPOINT'] ?? 'http://localhost:4566';
  const REGION = process.env['S3_REGION'] ?? 'us-east-1';
  const BUCKET = process.env['S3_BUCKET'] ?? 's3sqlite-integration';
  const SECRET = 'integration-test-secret-key-32chars!';

  const s3 = new S3Client({
    endpoint: ENDPOINT,
    region: REGION,
    forcePathStyle: true, // Required for MinIO / LocalStack
    credentials: {
      accessKeyId: process.env['AWS_ACCESS_KEY_ID'] ?? 'test',
      secretAccessKey: process.env['AWS_SECRET_ACCESS_KEY'] ?? 'test',
    },
  });

  let tmpDir: string;

  beforeAll(async () => {
    tmpDir = mkdtempSync(join(tmpdir(), 's3sqlite-integration-'));
    // Create the test bucket (ignore error if it already exists).
    try {
      await s3.send(new CreateBucketCommand({ Bucket: BUCKET }));
    } catch {
      // Already exists — that's fine.
    }
  });

  afterAll(async () => {
    // Clean up all objects created by the tests.
    const list = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET }));
    for (const obj of list.Contents ?? []) {
      await s3.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: obj.Key! }));
    }
    rmSync(tmpDir, { recursive: true, force: true });
  });

  describe('S3SQLiteDB — integration', () => {
    it('round-trips data through a real S3-compatible store', async () => {
      const db = await S3SQLiteDB.open({
        bucket: BUCKET,
        id: 'integration-user-1',
        hmacSecret: SECRET,
        s3,
        tmpDir,
        onCreate: (raw) =>
          raw.exec('CREATE TABLE notes (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT)'),
      });

      await db.transaction(() => {
        db.execute("INSERT INTO notes (text) VALUES ('hello')");
        db.execute("INSERT INTO notes (text) VALUES ('world')");
      });
      await db.close();

      // Re-open from S3 in a fresh tmp directory.
      const tmpDir2 = mkdtempSync(join(tmpdir(), 's3sqlite-int2-'));
      try {
        const db2 = await S3SQLiteDB.open({
          bucket: BUCKET,
          id: 'integration-user-1',
          hmacSecret: SECRET,
          s3,
          tmpDir: tmpDir2,
        });
        const rows = db2.query<{ text: string }>('SELECT text FROM notes ORDER BY id');
        expect(rows).toEqual([{ text: 'hello' }, { text: 'world' }]);
        await db2.close();
      } finally {
        rmSync(tmpDir2, { recursive: true, force: true });
      }
    });

    it('two databases for different users are fully isolated', async () => {
      const openUser = (id: string, dir: string) =>
        S3SQLiteDB.open({
          bucket: BUCKET,
          id,
          hmacSecret: SECRET,
          s3,
          tmpDir: dir,
          onCreate: (raw) => raw.exec('CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)'),
        });

      const db1 = await openUser('alice', tmpDir);
      await db1.transaction(() => {
        db1.execute("INSERT INTO kv VALUES ('secret', 'alice-data')");
      });
      await db1.close();

      const db2 = await openUser('bob', tmpDir);
      // Bob's database is brand new — no rows.
      const rows = db2.query('SELECT * FROM kv');
      expect(rows).toEqual([]);
      await db2.close();
    });
  });
}
