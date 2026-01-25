const fs = require("node:fs");
const path = require("node:path");
const https = require("node:https");

const SKIP = (process.env.LEDGERDB_SKIP_DOWNLOAD || "").toLowerCase();
if (SKIP === "1" || SKIP === "true") {
  console.log("ledgerdb: skipping download (LEDGERDB_SKIP_DOWNLOAD)");
  process.exit(0);
}
if (process.env.LEDGERDB_BIN && process.env.LEDGERDB_BIN.trim() !== "") {
  console.log("ledgerdb: using LEDGERDB_BIN, skipping download");
  process.exit(0);
}

const pkg = require("../package.json");
const tag = process.env.LEDGERDB_RELEASE_TAG || `v${pkg.version}`;
const base = process.env.LEDGERDB_DOWNLOAD_BASE || `https://github.com/osvaldoandrade/ledgerdb/releases/download/${tag}`;

const platformMap = {
  darwin: "darwin",
  linux: "linux",
  win32: "windows",
};
const archMap = {
  x64: "amd64",
  arm64: "arm64",
};

const platform = platformMap[process.platform];
const arch = archMap[process.arch];
if (!platform || !arch) {
  console.warn(`ledgerdb: unsupported platform/arch ${process.platform}/${process.arch}`);
  process.exit(0);
}

const ext = platform === "windows" ? ".exe" : "";
const assetName = `ledgerdb-${platform}-${arch}${ext}`;
const url = `${base}/${assetName}`;

const binDir = path.resolve(__dirname, "..", "bin");
const binPath = path.join(binDir, platform === "windows" ? "ledgerdb.exe" : "ledgerdb");

fs.mkdirSync(binDir, { recursive: true });

console.log(`ledgerdb: downloading ${url}`);

function download(currentUrl, destination) {
  return new Promise((resolve, reject) => {
    https
      .get(currentUrl, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          return resolve(download(res.headers.location, destination));
        }
        if (res.statusCode !== 200) {
          res.resume();
          return reject(new Error(`HTTP ${res.statusCode}`));
        }
        const file = fs.createWriteStream(destination);
        res.pipe(file);
        file.on("finish", () => file.close(resolve));
        file.on("error", reject);
      })
      .on("error", reject);
  });
}

(async () => {
  try {
    await download(url, binPath);
    if (platform !== "windows") {
      fs.chmodSync(binPath, 0o755);
    }
    console.log(`ledgerdb: installed ${binPath}`);
  } catch (err) {
    try {
      if (fs.existsSync(binPath)) {
        fs.unlinkSync(binPath);
      }
    } catch (_) {
      // ignore cleanup errors
    }
    console.error("ledgerdb: download failed", err.message || err);
    console.error("ledgerdb: set LEDGERDB_BIN to an existing binary or set LEDGERDB_SKIP_DOWNLOAD=1");
    process.exit(1);
  }
})();
