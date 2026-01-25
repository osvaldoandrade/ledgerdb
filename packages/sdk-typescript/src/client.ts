import { execFile, spawn, type ChildProcess } from "node:child_process";
import path from "node:path";

export type IndexMode = "history" | "state";

export interface IndexConfig {
  dbPath?: string;
  mode?: IndexMode;
  intervalMs?: number;
  jitterMs?: number;
  batchCommits?: number;
  fast?: boolean;
  fetch?: boolean;
  onlyChanges?: boolean;
}

export interface ClientConfig {
  repoPath: string;
  binaryPath?: string;
  env?: Record<string, string>;
  autoSync?: boolean;
  index?: IndexConfig;
}

export interface PutResult {
  commit: string;
  tx_hash: string;
  tx_id: string;
}

export interface GetResult {
  doc: unknown;
  tx_hash?: string;
  tx_id?: string;
  op?: string;
}

export interface LogEntry {
  tx_hash: string;
  tx_id: string;
  parent_hash?: string;
  timestamp: number;
  op: string;
}

export interface IndexSyncResult {
  reset: boolean;
  fetched: boolean;
  commits: number;
  txs_applied: number;
  docs_upserted: number;
  docs_deleted: number;
  collections: number;
  last_commit?: string;
}

export interface IndexWatchOptions extends IndexConfig {
  json?: boolean;
  stdio?: "inherit" | "pipe";
}

export function resolveBinaryPath(): string {
  if (process.env.LEDGERDB_BIN && process.env.LEDGERDB_BIN.trim() !== "") {
    return process.env.LEDGERDB_BIN;
  }
  const binName = process.platform === "win32" ? "ledgerdb.exe" : "ledgerdb";
  return path.resolve(__dirname, "..", "bin", binName);
}

export class LedgerDBClient {
  private readonly repoPath: string;
  private readonly binaryPath: string;
  private readonly env: NodeJS.ProcessEnv;
  private readonly autoSync: boolean;
  private readonly index: Required<IndexConfig>;

  constructor(cfg: ClientConfig) {
    if (!cfg.repoPath || cfg.repoPath.trim() === "") {
      throw new Error("repoPath is required");
    }
    this.repoPath = cfg.repoPath;
    this.binaryPath = cfg.binaryPath || resolveBinaryPath();
    this.env = { ...process.env, ...(cfg.env || {}) };
    this.autoSync = cfg.autoSync !== false;
    this.index = {
      dbPath: cfg.index?.dbPath || path.join(cfg.repoPath, "index.db"),
      mode: cfg.index?.mode || "state",
      intervalMs: cfg.index?.intervalMs ?? 1000,
      jitterMs: cfg.index?.jitterMs ?? 0,
      batchCommits: cfg.index?.batchCommits ?? 200,
      fast: cfg.index?.fast ?? true,
      fetch: cfg.index?.fetch ?? true,
      onlyChanges: cfg.index?.onlyChanges ?? true,
    };
  }

  get indexDbPath(): string {
    return this.index.dbPath;
  }

  async get(collection: string, docId: string): Promise<GetResult> {
    return this.execJson<GetResult>(["doc", "get", collection, docId], false);
  }

  async put(collection: string, docId: string, payload: unknown): Promise<PutResult> {
    const data = normalizePayload(payload);
    return this.execJson<PutResult>(["doc", "put", collection, docId, "--payload", data], true);
  }

  async patch(collection: string, docId: string, ops: unknown): Promise<PutResult> {
    const data = normalizePayload(ops);
    return this.execJson<PutResult>(["doc", "patch", collection, docId, "--ops", data], true);
  }

  async delete(collection: string, docId: string): Promise<PutResult> {
    return this.execJson<PutResult>(["doc", "delete", collection, docId], true);
  }

  async log(collection: string, docId: string): Promise<LogEntry[]> {
    const result = await this.execJson<{ entries: LogEntry[] }>(["doc", "log", collection, docId], false);
    return result.entries || [];
  }

  async revert(collection: string, docId: string, opts: { txId?: string; txHash?: string }): Promise<PutResult> {
    const args = ["doc", "revert", collection, docId];
    if (opts.txId) {
      args.push("--tx-id", opts.txId);
    }
    if (opts.txHash) {
      args.push("--tx-hash", opts.txHash);
    }
    return this.execJson<PutResult>(args, true);
  }

  async indexSync(overrides?: IndexConfig): Promise<IndexSyncResult> {
    return this.execJson<IndexSyncResult>(this.buildIndexArgs("sync", overrides), false);
  }

  startIndexWatch(options?: IndexWatchOptions): ChildProcess {
    const args = this.buildIndexArgs("watch", options);
    const fullArgs = [...this.baseArgs(false, options?.json ?? false), ...args];
    return spawn(this.binaryPath, fullArgs, {
      cwd: this.repoPath,
      env: this.env,
      stdio: options?.stdio || "inherit",
    });
  }

  async push(): Promise<void> {
    await this.execPlain(["push"], false);
  }

  private async execJson<T>(args: string[], writeOperation: boolean): Promise<T> {
    const fullArgs = [...this.baseArgs(writeOperation, true), ...args];
    const result = await execFileAsync(this.binaryPath, fullArgs, {
      cwd: this.repoPath,
      env: this.env,
      maxBuffer: 10 * 1024 * 1024,
    });
    try {
      return JSON.parse(result.stdout) as T;
    } catch (err) {
      const details = result.stderr.trim();
      throw new Error(`ledgerdb json parse failed: ${String(err)}${details ? `: ${details}` : ""}`);
    }
  }

  private async execPlain(args: string[], writeOperation: boolean): Promise<string> {
    const fullArgs = [...this.baseArgs(writeOperation, false), ...args];
    const result = await execFileAsync(this.binaryPath, fullArgs, {
      cwd: this.repoPath,
      env: this.env,
      maxBuffer: 10 * 1024 * 1024,
    });
    return result.stdout;
  }

  private baseArgs(writeOperation: boolean, json: boolean): string[] {
    const args = ["--repo", this.repoPath];
    if (json) {
      args.push("--json");
    }
    if (writeOperation && !this.autoSync) {
      args.push("--sync=false");
    }
    return args;
  }

  private buildIndexArgs(subcommand: "sync" | "watch", overrides?: IndexConfig): string[] {
    const cfg = { ...this.index, ...(overrides || {}) };
    const args = ["index", subcommand, "--db", cfg.dbPath, "--mode", cfg.mode];

    if (subcommand === "watch") {
      if (cfg.intervalMs <= 0) {
        throw new Error("index watch interval must be > 0");
      }
      args.push("--interval", `${cfg.intervalMs}ms`);
      if (cfg.jitterMs && cfg.jitterMs > 0) {
        args.push("--jitter", `${cfg.jitterMs}ms`);
      }
      if (cfg.onlyChanges) {
        args.push("--only-changes");
      }
    }

    if (cfg.batchCommits && cfg.batchCommits > 0) {
      args.push("--batch-commits", String(cfg.batchCommits));
    }
    if (cfg.fast) {
      args.push("--fast");
    }
    if (cfg.fetch === false) {
      args.push("--fetch=false");
    }
    return args;
  }
}

function normalizePayload(payload: unknown): string {
  if (typeof payload === "string") {
    return payload;
  }
  return JSON.stringify(payload ?? null);
}

function execFileAsync(
  file: string,
  args: string[],
  options: { cwd?: string; env?: NodeJS.ProcessEnv; maxBuffer?: number }
): Promise<{ stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    execFile(file, args, options, (error, stdout, stderr) => {
      if (error) {
        const details = stderr ? stderr.toString().trim() : "";
        const message = details ? `${error.message}: ${details}` : error.message;
        reject(new Error(message));
        return;
      }
      resolve({ stdout: stdout.toString(), stderr: stderr.toString() });
    });
  });
}
