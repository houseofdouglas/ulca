import type { S3Client } from '@aws-sdk/client-s3';
import type Database from 'better-sqlite3';

export type SqlParams = unknown[] | Record<string, unknown>;

export interface ExecuteResult {
  changes: number;
  lastInsertId: number;
}

/**
 * Options for opening (or creating) a database.
 *
 * The S3 object key is derived as:
 *   HMAC-SHA256(hmacSecret, "<namespace>:<id>")
 *
 * This makes the key opaque — knowing a user's `id` without the secret
 * reveals nothing about which object to look for, preventing enumeration
 * and cross-account collisions.
 */
export interface OpenOptions {
  /** S3 bucket that holds all databases. */
  bucket: string;

  /**
   * Unique identifier for this database — typically a user ID or tenant ID.
   * Combined with `namespace` before hashing, so the same `id` in different
   * namespaces produces a different S3 key.
   */
  id: string;

  /**
   * Logical namespace that scopes the `id`.
   * Useful for multi-tenant deployments where the same `id` value could
   * appear in multiple product contexts (e.g. "prod", "staging", or an
   * application name).
   * Defaults to `"default"`.
   */
  namespace?: string;

  /**
   * Secret used to HMAC the object key.  Keep this in AWS Secrets Manager
   * or as a Lambda environment variable — never in source code.
   * Must be at least 32 characters.
   */
  hmacSecret: string;

  /** Pre-configured S3Client.  Injected so callers control region / endpoint. */
  s3: S3Client;

  /**
   * Directory for the local SQLite scratch file.
   * Defaults to `process.env.TMPDIR ?? "/tmp"`.
   */
  tmpDir?: string;

  /**
   * Called exactly once when a brand-new database is created (no existing
   * S3 object found).  Use this to run CREATE TABLE statements and seed data.
   */
  onCreate?: (db: Database.Database) => void;
}
