#!/usr/bin/env node
const path = require("node:path");
const { spawn } = require("node:child_process");

const bin = process.env.LEDGERDB_BIN && process.env.LEDGERDB_BIN.trim() !== ""
  ? process.env.LEDGERDB_BIN
  : path.resolve(__dirname, process.platform === "win32" ? "ledgerdb.exe" : "ledgerdb");

const child = spawn(bin, process.argv.slice(2), { stdio: "inherit" });
child.on("error", (err) => {
  console.error("ledgerdb: failed to start", err.message || err);
  process.exit(1);
});
child.on("exit", (code) => {
  process.exit(code === null ? 1 : code);
});
