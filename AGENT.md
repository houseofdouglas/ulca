# s3-sqlite — Agent Reference

## What this module is

A TypeScript class (`S3SQLiteDB`) that stores a SQLite database as a single S3 object.
Each database maps 1-to-1 with a unique `(namespace, id)` pair.
Intended runtime: AWS Lambda with **reserved concurrency = 1** per `(namespace, id)`.
That concurrency constraint is what makes serialisation safe — this module does not implement distributed locking.

## What this module is NOT

- Not a general-purpose distributed database.
- Not safe to use with Lambda concurrency > 1 for the same `(namespace, id)`.
- Not a replacement for RDS or DynamoDB in write-heavy, multi-writer scenarios.

---

## Installation

```bash
npm install better-sqlite3 @aws-sdk/client-s3
# compile
npx tsc
```

`better-sqlite3` is a native module. For Lambda (ARM64) it must be compiled for
`linux/arm64`. Use a matching build environment or a Lambda layer.

---

## Imports

```typescript
import { S3SQLiteDB, ETagMismatchError, InsecureSecretError } from './dist/index.js';
import type { OpenOptions, ExecuteResult, SqlParams } from './dist/index.js';
import { S3Client } from '@aws-sdk/client-s3';
```

---

## OpenOptions

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `bucket` | `string` | yes | — | S3 bucket that holds all database objects. |
| `id` | `string` | yes | — | Unique identifier for this database (user ID, tenant ID, etc.). Never stored in S3 directly. |
| `namespace` | `string` | no | `"default"` | Logical scope for `id`. Same `id` in two namespaces produces different S3 keys. Use to separate environments (prod/staging) or applications. |
| `hmacSecret` | `string` | yes | — | Secret used to derive the S3 object key via HMAC-SHA256. **Must be ≥ 32 characters.** Store in AWS Secrets Manager or a Lambda environment variable. Never hardcode. |
| `s3` | `S3Client` | yes | — | Pre-configured `S3Client` instance. Caller controls region, endpoint, credentials. |
| `tmpDir` | `string` | no | `process.env.TMPDIR ?? "/tmp"` | Local directory for the SQLite scratch file. On Lambda, `/tmp` has 512 MB–10 GB depending on configuration. |
| `onCreate` | `(db: Database) => void` | no | — | Callback invoked exactly once when no existing S3 object is found. Use for `CREATE TABLE` and seed inserts. The `db` argument is a raw `better-sqlite3` `Database` instance. |

---

## S3 object key format

```
dbs/<hex64>.db
```

Where `hex64 = HMAC-SHA256(hmacSecret, "<namespace>:<id>")`.

The key is opaque. Listing the bucket reveals no information about user IDs.
Two callers using different `hmacSecret` values will never collide for the same `id`.

---

## API

### `S3SQLiteDB.open(opts: OpenOptions): Promise<S3SQLiteDB>`

Factory. Creates or opens the database.

Behaviour:
1. Derives the S3 key from `(namespace, id, hmacSecret)`.
2. Attempts `GetObject`. If the object does not exist, marks the database as new.
3. Writes the downloaded (or empty) file to `tmpDir`.
4. Opens SQLite with `journal_mode = DELETE` and `foreign_keys = ON`.
5. If new and `onCreate` is provided, calls `onCreate(db)` and marks the instance dirty.

Throws:
- `InsecureSecretError` — `hmacSecret.length < 32`.
- Any S3 error other than `NoSuchKey`.

```typescript
const s3 = new S3Client({ region: 'us-east-1' });

const db = await S3SQLiteDB.open({
  bucket: 'my-app-databases',
  id: userId,
  namespace: 'prod',
  hmacSecret: process.env.DB_HMAC_SECRET!,
  s3,
  onCreate: raw => {
    raw.exec(`
      CREATE TABLE notes (
        id    INTEGER PRIMARY KEY AUTOINCREMENT,
        text  TEXT    NOT NULL,
        ts    INTEGER NOT NULL DEFAULT (unixepoch())
      )
    `);
  },
});
```

---

### `db.query<T>(sql, params?): T[]`

Synchronous read. Executes `sql` against the local SQLite file and returns all matching rows.

- `params` may be a positional array (`[value, value]`) or a named object (`{ $name: value }`).
  Named parameters in SQL use `:name`, `@name`, or `$name` syntax per SQLite conventions.
- Does **not** flush to S3.
- Does **not** mark the instance dirty.
- Return type defaults to `Record<string, unknown>[]`; pass a generic to get a typed result.

```typescript
const rows = db.query<{ id: number; text: string }>(
  'SELECT id, text FROM notes WHERE ts > ? ORDER BY ts DESC',
  [cutoff]
);

// Named params
const row = db.query<{ count: number }>(
  'SELECT COUNT(*) AS count FROM notes WHERE text LIKE :pattern',
  { pattern: '%hello%' }
);
```

---

### `db.execute(sql, params?): ExecuteResult`

Synchronous write. Runs an `INSERT`, `UPDATE`, or `DELETE`.

Returns `{ changes: number, lastInsertId: number }`.

- Marks the instance dirty. Writes are **not** durable until `flush()` or `close()` is called.
- For atomic, immediately durable writes, use `transaction()` instead.

```typescript
const { lastInsertId } = db.execute(
  'INSERT INTO notes (text) VALUES (?)',
  ['hello world']
);
```

---

### `db.transaction<T>(fn: () => T): Promise<T>`

Wraps `fn` in a SQLite transaction. On successful SQLite commit, atomically re-uploads the
database to S3 using a conditional PUT (`If-Match` / `If-None-Match`).

- If `fn` throws, SQLite rolls back and **no S3 upload occurs**.
- If the S3 conditional PUT fails (ETag changed), throws `ETagMismatchError` after
  best-effort re-sync from S3. The caller can re-open or retry.
- Returns the value returned by `fn`.
- Clears the dirty flag on success.

```typescript
const newId = await db.transaction(() => {
  const { lastInsertId } = db.execute(
    'INSERT INTO notes (text) VALUES (?)', ['note 1']
  );
  db.execute('INSERT INTO notes (text) VALUES (?)', ['note 2']);
  return lastInsertId;
});
```

---

### `db.flush(): Promise<void>`

Uploads the current local SQLite state to S3 unconditionally (subject to ETag check).

Use this after one or more bare `execute()` calls when you want to batch writes before
persisting — more efficient than wrapping every `execute()` in its own `transaction()`.

Throws `ETagMismatchError` on conditional write failure (see crash recovery below).
Clears the dirty flag on success.

```typescript
for (const item of batch) {
  db.execute('INSERT INTO items (v) VALUES (?)', [item]);
}
await db.flush();
```

---

### `db.close(): Promise<void>`

Flushes to S3 **only if the instance is dirty**, then closes the SQLite connection.

If there are no unflushed writes (e.g. a read-only session, or all writes were already
flushed via `transaction()` or `flush()`), the S3 upload is skipped.

Call this at the end of every Lambda handler.

```typescript
export const handler = async (event) => {
  const db = await S3SQLiteDB.open({ ... });
  try {
    // ... use db ...
  } finally {
    await db.close();
  }
};
```

---

### `db.objectKey: string` (read-only)

The derived S3 object key for this database instance. Useful for logging and debugging.

```typescript
console.log('db key:', db.objectKey);
// → "dbs/3f4a...c7d2.db"
```

---

## Types

```typescript
type SqlParams = unknown[] | Record<string, unknown>;

interface ExecuteResult {
  changes:      number;  // rows affected
  lastInsertId: number;  // ROWID of last INSERT (0 if not an INSERT)
}
```

---

## Errors

### `InsecureSecretError`

Thrown by `open()` when `hmacSecret.length < 32`.

```typescript
import { InsecureSecretError } from './dist/index.js';
// name: 'InsecureSecretError'
```

### `ETagMismatchError`

Thrown by `transaction()`, `flush()`, or `close()` when the S3 conditional PUT is rejected
(HTTP 412). This means either:

- A crash left an uncommitted object in S3 from a previous invocation, **or**
- (should not happen with concurrency=1) another writer modified the object.

```typescript
import { ETagMismatchError } from './dist/index.js';

err.key      // string — S3 object key
err.expected // string | null — the ETag we held before the conflict
```

After `ETagMismatchError` is thrown, the instance performs a best-effort re-sync from S3.
If re-sync succeeds, `db.query()` reflects the server-side state and the caller can retry.
If re-sync fails (e.g. corrupt remote data), the instance retains its pre-flush local state.

**Recovery pattern:**

```typescript
try {
  await db.transaction(() => { /* writes */ });
} catch (err) {
  if (err instanceof ETagMismatchError) {
    // Instance has been re-synced. Inspect state and retry if appropriate.
    const currentRows = db.query('SELECT * FROM notes');
    // ... decide whether to retry
  } else {
    throw err;
  }
}
```

---

## Concurrency model and durability guarantees

| Scenario | Result |
|---|---|
| Lambda concurrency = 1, normal path | Fully atomic. Each `transaction()` / `flush()` writes a new S3 object only if the ETag matches. |
| Lambda cold start after crash mid-upload | The S3 object has the last successfully uploaded version. `open()` downloads it. Any writes from the crashed invocation are lost. |
| Lambda concurrency > 1 for same `(namespace, id)` | The second writer's upload will throw `ETagMismatchError`. **Do not do this.** Use Lambda reserved concurrency = 1. |
| S3 object deleted externally | Next `open()` treats the database as new and calls `onCreate` if provided. |
| `transaction()` fn throws | SQLite rolls back. S3 is not touched. No data loss. |

---

## SQLite pragmas set at open

| Pragma | Value | Reason |
|---|---|---|
| `journal_mode` | `DELETE` | No WAL / SHM sidecar files. Single-file database compatible with S3. |
| `foreign_keys` | `ON` | Enforce referential integrity by default. |

To set additional pragmas, use `onCreate` for one-time settings or add them to your
handler before calling any query:

```typescript
// Access the underlying better-sqlite3 handle via a raw query
// (better-sqlite3 pragmas return rows; ignore the result)
db.query("PRAGMA cache_size = -8000");  // 8 MB page cache
```

---

## Lambda deployment checklist

1. **Reserved concurrency = 1** per logical database (per `(namespace, id)` if databases
   are per-user, or globally if you use one Lambda for all users and serialize via SQS/queue).
2. `better-sqlite3` must be compiled for `linux/arm64` (or `linux/x86_64` to match your
   Lambda architecture). Use a Docker build or Lambda layer.
3. Store `hmacSecret` in AWS Secrets Manager or as a Lambda environment variable.
   Rotate it only during a maintenance window — changing it makes all existing keys
   unreachable under the new secret.
4. `/tmp` ephemeral storage: default 512 MB, configurable up to 10 GB.
   Set `tmpDir` to `/tmp` (the default) unless you have a reason to change it.
5. S3 bucket: enable versioning if you want point-in-time recovery.
   Block all public access. Use bucket policy to restrict to the Lambda execution role.
6. IAM permissions required: `s3:GetObject`, `s3:PutObject`, `s3:HeadObject` on
   `arn:aws:s3:::your-bucket/dbs/*`.

---

## Minimal Lambda handler pattern

```typescript
import { S3Client } from '@aws-sdk/client-s3';
import { S3SQLiteDB } from './dist/index.js';

const s3 = new S3Client({ region: process.env.AWS_REGION });

export const handler = async (event: { userId: string; sql: string; params: unknown[] }) => {
  const db = await S3SQLiteDB.open({
    bucket: process.env.DB_BUCKET!,
    id: event.userId,
    namespace: 'v1',
    hmacSecret: process.env.DB_HMAC_SECRET!,
    s3,
    onCreate: raw => raw.exec('CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)'),
  });

  try {
    const rows = db.query(event.sql, event.params);
    return { rows };
  } finally {
    await db.close();
  }
};
```

---

## Test harness

Unit tests use `MockS3Client` (in `tests/MockS3.ts`) — an in-memory S3 substitute
that supports `GetObject`, `PutObject` (with `IfMatch` / `IfNoneMatch`), and `HeadObject`.
No Docker or network required.

```typescript
import { MockS3Client } from './tests/MockS3.js';

const s3 = new MockS3Client();
const db = await S3SQLiteDB.open({
  bucket: 'test',
  id: 'user-1',
  hmacSecret: 'thirty-two-character-secret-here!!',
  s3: s3 as unknown as S3Client,
});
```

Integration tests against LocalStack or MinIO:

```bash
# LocalStack
docker run --rm -p 4566:4566 localstack/localstack

INTEGRATION=true npm run test:integration
```

Environment variables for integration tests:

| Variable | Default |
|---|---|
| `S3_ENDPOINT` | `http://localhost:4566` |
| `S3_REGION` | `us-east-1` |
| `S3_BUCKET` | `s3sqlite-integration` |
| `AWS_ACCESS_KEY_ID` | `test` |
| `AWS_SECRET_ACCESS_KEY` | `test` |
