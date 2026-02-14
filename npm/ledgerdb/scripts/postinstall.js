/* eslint-disable no-console */
'use strict';

const crypto = require('crypto');
const fs = require('fs');
const https = require('https');
const path = require('path');

function log(msg) {
  console.log(`[ledgerdb] ${msg}`);
}

function fail(msg) {
  console.error(`[ledgerdb] error: ${msg}`);
  process.exit(1);
}

function getGoOS() {
  switch (process.platform) {
    case 'darwin':
      return 'darwin';
    case 'linux':
      return 'linux';
    case 'win32':
      return 'windows';
    default:
      return null;
  }
}

function getGoArch() {
  switch (process.arch) {
    case 'x64':
      return 'amd64';
    case 'arm64':
      return 'arm64';
    default:
      return null;
  }
}

function authHeaders() {
  const token = process.env.GITHUB_TOKEN || process.env.GH_TOKEN || '';
  const headers = {
    'User-Agent': '@osvaldoandrade/ledgerdb postinstall',
    Accept: '*/*',
  };
  if (token) {
    headers.Authorization = `token ${token}`;
  }
  return headers;
}

function request(url, headers, redirectsLeft) {
  return new Promise((resolve, reject) => {
    const req = https.request(url, { headers }, (res) => {
      const code = res.statusCode || 0;
      const location = res.headers.location;

      if (code >= 300 && code < 400 && location && redirectsLeft > 0) {
        // GitHub release downloads redirect to a signed URL.
        res.resume();
        resolve(request(location, headers, redirectsLeft - 1));
        return;
      }

      if (code < 200 || code >= 300) {
        let body = '';
        res.setEncoding('utf8');
        res.on('data', (d) => {
          body += d;
        });
        res.on('end', () => {
          reject(new Error(`HTTP ${code} for ${url}${body ? `: ${body.slice(0, 200)}` : ''}`));
        });
        return;
      }

      resolve(res);
    });

    req.on('error', reject);
    req.end();
  });
}

async function downloadText(url) {
  const res = await request(url, authHeaders(), 5);
  return await new Promise((resolve, reject) => {
    let out = '';
    res.setEncoding('utf8');
    res.on('data', (d) => {
      out += d;
    });
    res.on('end', () => resolve(out));
    res.on('error', reject);
  });
}

function parseExpectedSha256(sumText, assetName) {
  const lines = sumText.split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const parts = trimmed.split(/\s+/);
    if (parts.length < 2) continue;
    const sha = parts[0];
    const file = parts[1].replace(/^dist\//, '');
    if (file === assetName) {
      return sha.toLowerCase();
    }
  }
  return null;
}

async function downloadFileWithSha256(url, outPath, expectedSha256) {
  if (!expectedSha256) throw new Error('missing expected sha256');

  const res = await request(url, authHeaders(), 5);

  await fs.promises.mkdir(path.dirname(outPath), { recursive: true });
  const tmpPath = `${outPath}.tmp`;

  const hash = crypto.createHash('sha256');
  const ws = fs.createWriteStream(tmpPath, { mode: 0o755 });

  await new Promise((resolve, reject) => {
    res.on('data', (chunk) => hash.update(chunk));
    res.pipe(ws);
    res.on('error', reject);
    ws.on('error', reject);
    ws.on('finish', resolve);
  });

  const got = hash.digest('hex').toLowerCase();
  if (got !== expectedSha256) {
    await fs.promises.rm(tmpPath, { force: true });
    throw new Error(`sha256 mismatch for ${path.basename(outPath)}: expected ${expectedSha256}, got ${got}`);
  }

  await fs.promises.rename(tmpPath, outPath);
}

async function main() {
  const goos = getGoOS();
  const goarch = getGoArch();
  if (!goos) fail(`unsupported platform: ${process.platform}`);
  if (!goarch) fail(`unsupported arch: ${process.arch}`);

  const pkgPath = path.join(__dirname, '..', 'package.json');
  // eslint-disable-next-line global-require, import/no-dynamic-require
  const pkg = require(pkgPath);
  const version = pkg.version;

  if (!version || typeof version !== 'string') fail('missing package version');

  const owner = process.env.LEDGERDB_GH_OWNER || 'osvaldoandrade';
  const repo = process.env.LEDGERDB_GH_REPO || 'ledgerdb';
  const tag = `v${version}`;

  const ext = goos === 'windows' ? '.exe' : '';
  const assetName = `ledgerdb-${goos}-${goarch}${ext}`;
  const base = `https://github.com/${owner}/${repo}/releases/download/${tag}`;
  const checksumUrl = `${base}/SHA256SUMS.txt`;
  const binUrl = `${base}/${assetName}`;

  const outPath = path.join(__dirname, '..', 'bin', `ledgerdb-bin${ext}`);
  log(`downloading ${assetName} (${tag})`);

  const txt = await downloadText(checksumUrl);
  const expected = parseExpectedSha256(txt, assetName);
  if (!expected) fail(`checksum entry not found for ${assetName} in SHA256SUMS.txt`);

  await downloadFileWithSha256(binUrl, outPath, expected);

  if (goos !== 'windows') {
    await fs.promises.chmod(outPath, 0o755);
  }

  log(`installed ${path.relative(process.cwd(), outPath)}`);
}

main().catch((e) => fail(e.message));

