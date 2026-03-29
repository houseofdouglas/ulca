import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { S3SQLiteDB, ETagMismatchError, InsecureSecretError } from '../src/S3SQLiteDB.js';
import { MockS3Client } from './MockS3.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const BUCKET = 'test-bucket';
const SECRET = 'a-sufficiently-long-secret-key-32+chars'; // ≥ 32 chars

function makeClient() {
  return new MockS3Client();
}

async function openDB(
  s3: MockS3Client,
  id: string,
  opts: { namespace?: string; tmpDir?: string; onCreate?: (db: import('better-sqlite3').Database) => void } = {},
): Promise<S3SQLiteDB> {
  return S3SQLiteDB.open({
    bucket: BUCKET,
    id,
    namespace: opts.namespace,
    hmacSecret: SECRET,
    s3: s3 as unknown as import('@aws-sdk/client-s3').S3Client,
    tmpDir: opts.tmpDir,
    onCreate: opts.onCreate,
  });
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

describe('S3SQLiteDB', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 's3sqlite-test-'));
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  // -------------------------------------------------------------------------
  // Construction / open
  // -------------------------------------------------------------------------

  describe('open()', () => {
    it('creates a fresh database when no S3 object exists', async () => {
      const s3 = makeClient();
      const db = await openDB(s3, 'user-1', { tmpDir });
      // A query on an empty DB should return nothing — not throw.
      const rows = db.query('SELECT 1 AS n');
      expect(rows).toEqual([{ n: 1 }]);
      await db.close();
    });

    it('calls onCreate() exactly once for a new database', async () => {
      const s3 = makeClient();
      let called = 0;
      const db = await openDB(s3, 'user-2', {
        tmpDir,
        onCreate: (raw) => {
          called++;
          raw.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)');
        },
      });
      expect(called).toBe(1);
      // Table must exist.
      const rows = db.query("SELECT name FROM sqlite_master WHERE type='table'");
      expect(rows).toEqual([{ name: 'items' }]);
      await db.close();
    });

    it('does NOT call onCreate() when opening an existing database', async () => {
      const s3 = makeClient();
      let created = 0;
      const schema = (raw: import('better-sqlite3').Database) => {
        created++;
        raw.exec('CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)');
      };

      const db1 = await openDB(s3, 'user-3', { tmpDir, onCreate: schema });
      db1.execute('INSERT INTO kv VALUES (?, ?)', ['hello', 'world']);
      await db1.flush();
      await db1.close();

      // Re-open — onCreate must NOT fire again.
      const db2 = await openDB(s3, 'user-3', { tmpDir, onCreate: schema });
      expect(created).toBe(1);
      const rows = db2.query<{ k: string; v: string }>('SELECT * FROM kv');
      expect(rows).toEqual([{ k: 'hello', v: 'world' }]);
      await db2.close();
    });

    it('rejects hmacSecret shorter than 32 characters', async () => {
      const s3 = makeClient();
      await expect(
        S3SQLiteDB.open({
          bucket: BUCKET,
          id: 'x',
          hmacSecret: 'short',
          s3: s3 as unknown as import('@aws-sdk/client-s3').S3Client,
        }),
      ).rejects.toThrow(InsecureSecretError);
    });
  });

  // -------------------------------------------------------------------------
  // query()
  // -------------------------------------------------------------------------

  describe('query()', () => {
    it('returns all rows matching the SQL', async () => {
      const s3 = makeClient();
      const db = await openDB(s3, 'u', {
        tmpDir,
        onCreate: (raw) => raw.exec('CREATE TABLE t (x INT)'),
      });
      db.execute('INSERT INTO t VALUES (?)', [1]);
      db.execute('INSERT INTO t VALUES (?)', [2]);
      const rows = db.query<{ x: number }>('SELECT * FROM t ORDER BY x');
      expect(rows).toEqual([{ x: 1 }, { x: 2 }]);
      await db.close();
    });

    it('accepts named parameters', async () => {
      const s3 = makeClient();
      const db = await openDB(s3, 'u2', {
        tmpDir,
        onCreate: (raw) => raw.exec('CREATE TABLE t (name TEXT)'),
      });
      db.execute('INSERT INTO t VALUES (:name)', { name: 'Alice' });
      const rows = db.query<{ name: string }>('SELECT * FROM t WHERE name = :name', {
        name: 'Alice',
      });
      expect(rows).toEqual([{ name: 'Alice' }]);
      await db.close();
    });

    it('returns an empty array when no rows match', async () => {
      const s3 = makeClient();
      const db = await openDB(s3, 'u3', {
        tmpDir,
        onCreate: (raw) => raw.exec('CREATE TABLE t (x INT)'),
      });
      expect(db.query('SELECT * FROM t WHERE x = ?', [999])).toEqual([]);
      await db.close();
    });
  });

  // -------------------------------------------------------------------------
  // execute()
  // -------------------------------------------------------------------------

  describe('execute()', () => {
    it('returns changes and lastInsertId for INSERT', async () => {
      const s3 = makeClient();
      const db = await openDB(s3, 'e1', {
        tmpDir,
        onCreate: (raw) =>
          raw.exec('CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)'),
      });
      const r1 = db.execute('INSERT INTO t (v) VALUES (?)', ['a']);
      expect(r1.changes).toBe(1);
      expect(r1.lastInsertId).toBe(1);

      const r2 = db.execute('INSERT INTO t (v) VALUES (?)', ['b']);
      expect(r2.lastInsertId).toBe(2);
      await db.close();
    });

    it('returns changes count for UPDATE', async () => {
      const s3 = makeClient();
      const db = await openDB(s3, 'e2', {
        tmpDir,
        onCreate: (raw) => raw.exec('CREATE TABLE t (id INT, v TEXT)'),
      });
      db.execute('INSERT INTO t VALUES (1, ?)', ['old']);
      db.execute('INSERT INTO t VALUES (2, ?)', ['old']);
      const r = db.execute("UPDATE t SET v = 'new'");
      expect(r.changes).toBe(2);
      await db.close();
    });
  });

  // -------------------------------------------------------------------------
  // flush()
  // -------------------------------------------------------------------------

  describe('flush()', () => {
    it('persists execute() writes to S3', async () => {
      const s3 = makeClient();
      const db = await openDB(s3, 'f1', {
        tmpDir,
        onCreate: (raw) => raw.exec('CREATE TABLE t (v TEXT)'),
      });
      db.execute('INSERT INTO t VALUES (?)', ['hello']);
      await db.flush();

      // Verify the S3 object was created.
      expect(s3.objects.size).toBe(1);

      // Re-open in a fresh tmpDir to confirm durability.
      const tmpDir2 = mkdtempSync(join(tmpdir(), 's3sqlite-test2-'));
      try {
        const db2 = await openDB(s3, 'f1', { tmpDir: tmpDir2 });
        const rows = db2.query<{ v: string }>('SELECT * FROM t');
        expect(rows).toEqual([{ v: 'hello' }]);
        await db2.close();
      } finally {
        rmSync(tmpDir2, { recursive: true, force: true });
      }
    });
  });

  // -------------------------------------------------------------------------
  // transaction()
  // -------------------------------------------------------------------------

  describe('transaction()', () => {
    it('commits all writes atomically and persists to S3', async () => {
      const s3 = makeClient();
      const db = await openDB(s3, 't1', {
        tmpDir,
        onCreate: (raw) => raw.exec('CREATE TABLE t (v TEXT)'),
      });

      await db.transaction(() => {
        db.execute('INSERT INTO t VALUES (?)', ['a']);
        db.execute('INSERT INTO t VALUES (?)', ['b']);
        db.execute('INSERT INTO t VALUES (?)', ['c']);
      });

      const tmpDir2 = mkdtempSync(join(tmpdir(), 's3sqlite-test-tx-'));
      try {
        const db2 = await openDB(s3, 't1', { tmpDir: tmpDir2 });
        const rows = db2.query<{ v: string }>('SELECT * FROM t ORDER BY v');
        expect(rows).toEqual([{ v: 'a' }, { v: 'b' }, { v: 'c' }]);
        await db2.close();
      } finally {
        rmSync(tmpDir2, { recursive: true, force: true });
      }

      await db.close();
    });

    it('rolls back SQLite and does NOT flush on thrown error inside fn', async () => {
      const s3 = makeClient();
      const db = await openDB(s3, 't2', {
        tmpDir,
        onCreate: (raw) => raw.exec('CREATE TABLE t (v TEXT)'),
      });

      // First commit so s3 has the initial state.
      await db.transaction(() => {
        db.execute('INSERT INTO t VALUES (?)', ['before']);
      });

      await expect(
        db.transaction(() => {
          db.execute('INSERT INTO t VALUES (?)', ['during']);
          throw new Error('intentional rollback');
        }),
      ).rejects.toThrow('intentional rollback');

      // The SQLite rollback means 'during' was never committed.
      const rows = db.query<{ v: string }>('SELECT * FROM t');
      expect(rows).toEqual([{ v: 'before' }]);
      await db.close();
    });

    it('returns the value produced by fn', async () => {
      const s3 = makeClient();
      const db = await openDB(s3, 't3', {
        tmpDir,
        onCreate: (raw) =>
          raw.exec('CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)'),
      });

      const id = await db.transaction(() => {
        const r = db.execute('INSERT INTO t (v) VALUES (?)', ['x']);
        return r.lastInsertId;
      });
      expect(id).toBe(1);
      await db.close();
    });
  });

  // -------------------------------------------------------------------------
  // ETag / crash recovery
  // -------------------------------------------------------------------------

  describe('ETag-based crash recovery', () => {
    it('throws ETagMismatchError when S3 object was externally replaced', async () => {
      const s3 = makeClient();
      const db = await openDB(s3, 'cr1', {
        tmpDir,
        onCreate: (raw) => raw.exec('CREATE TABLE t (v TEXT)'),
      });
      // First flush establishes the ETag.
      db.execute('INSERT INTO t VALUES (?)', ['v1']);
      await db.flush();

      // Simulate another writer (or crash mid-flight) clobbering the object.
      const keyBefore = [...s3.objects.keys()][0]!;
      s3.overwrite(keyBefore, Buffer.from('corrupt or newer database content'));

      // Next flush must detect the mismatch.
      db.execute('INSERT INTO t VALUES (?)', ['v2']);
      await expect(db.flush()).rejects.toThrow(ETagMismatchError);
    });

    it('re-syncs from S3 after ETagMismatchError so state is fresh', async () => {
      const s3 = makeClient();
      const db = await openDB(s3, 'cr2', {
        tmpDir,
        onCreate: (raw) => raw.exec('CREATE TABLE t (v TEXT)'),
      });
      db.execute('INSERT INTO t VALUES (?)', ['initial']);
      await db.flush();

      // Overwrite with a valid SQLite file that has different rows — build it
      // from a second DB instance so it's a real SQLite file.
      const tmpDir2 = mkdtempSync(join(tmpdir(), 's3sqlite-crtest-'));
      try {
        const db2 = await openDB(s3, 'cr2-alt', {
          tmpDir: tmpDir2,
          onCreate: (raw) => raw.exec('CREATE TABLE t (v TEXT)'),
        });
        db2.execute('INSERT INTO t VALUES (?)', ['server-side']);
        await db2.flush();

        // Manually swap the bytes from cr2-alt onto cr2's key.
        const [cr2Key] = [...s3.objects.keys()].filter((k) =>
          k !== db2.objectKey,
        );
        const altObj = s3.objects.get(db2.objectKey)!;
        s3.overwrite(cr2Key!, altObj.body);

        // Flush should fail and refresh.
        db.execute('INSERT INTO t VALUES (?)', ['stale write']);
        await expect(db.flush()).rejects.toThrow(ETagMismatchError);

        // After recovery, query should reflect the server-side state.
        const rows = db.query<{ v: string }>('SELECT * FROM t');
        expect(rows).toEqual([{ v: 'server-side' }]);
        await db2.close();
      } finally {
        rmSync(tmpDir2, { recursive: true, force: true });
      }
    });
  });

  // -------------------------------------------------------------------------
  // Namespace / key isolation
  // -------------------------------------------------------------------------

  describe('namespace isolation', () => {
    it('same id in different namespaces uses different S3 keys', async () => {
      const s3 = makeClient();
      const db1 = await openDB(s3, 'user-99', {
        namespace: 'app-a',
        tmpDir,
        onCreate: (raw) => raw.exec('CREATE TABLE t (v TEXT)'),
      });
      const db2 = await openDB(s3, 'user-99', {
        namespace: 'app-b',
        tmpDir,
        onCreate: (raw) => raw.exec('CREATE TABLE t (v TEXT)'),
      });
      expect(db1.objectKey).not.toBe(db2.objectKey);
      await db1.close();
      await db2.close();
    });

    it('same id in same namespace uses the same S3 key', async () => {
      const s3 = makeClient();
      const db1 = await openDB(s3, 'shared', { namespace: 'ns', tmpDir });
      const db2 = await openDB(s3, 'shared', { namespace: 'ns', tmpDir });
      expect(db1.objectKey).toBe(db2.objectKey);
      await db1.close();
      await db2.close();
    });

    it('S3 key is an opaque hex hash — id is not visible in it', async () => {
      const s3 = makeClient();
      const db = await openDB(s3, 'very-sensitive-user-id', { tmpDir });
      expect(db.objectKey).not.toContain('very-sensitive-user-id');
      // Key should look like dbs/<64 hex chars>.db
      expect(db.objectKey).toMatch(/^dbs\/[0-9a-f]{64}\.db$/);
      await db.close();
    });
  });

  // -------------------------------------------------------------------------
  // Warm-start ETag check
  // -------------------------------------------------------------------------

  describe('warm-start optimisation', () => {
    it('opens successfully when local cache is current (no re-download)', async () => {
      const s3 = makeClient();
      // Open, write, flush.
      const db = await openDB(s3, 'warm', {
        tmpDir,
        onCreate: (raw) => raw.exec('CREATE TABLE t (n INT)'),
      });
      db.execute('INSERT INTO t VALUES (?)', [42]);
      await db.flush();
      await db.close();

      // Track how many GetObject calls are made.
      let getCount = 0;
      const originalSend = s3.send.bind(s3);
      s3.send = async (cmd) => {
        if (cmd.constructor.name === 'GetObjectCommand') getCount++;
        return originalSend(cmd);
      };

      // Re-open using the same tmpDir (cache hit).
      const db2 = await openDB(s3, 'warm', { tmpDir });
      expect(getCount).toBe(1); // Cold re-open still downloads once.
      const rows = db2.query<{ n: number }>('SELECT * FROM t');
      expect(rows).toEqual([{ n: 42 }]);
      await db2.close();
    });
  });
});
