const NAV = [
  {
    section: "Start Here",
    pages: [
      ["Home", "Home"],
      ["Get Started", "Get-Started"],
      ["Overview", "Overview"],
      ["Architecture", "Architecture"],
    ],
  },
  {
    section: "Core Specs",
    pages: [
      ["Storage Engine and Interface", "Storage-Engine-and-Interface"],
      ["Partitioning and Distribution Strategy", "Partitioning-and-Distribution-Strategy"],
      ["Versioning and Conflict Resolution", "Versioning-and-Conflict-Resolution"],
      ["Execution Model and Consistency", "Execution-Model-and-Consistency"],
      ["Querying and Indexing Strategy", "Querying-and-Indexing-Strategy"],
      ["Integrity and Security Strategy", "Integrity-and-Security-Strategy"],
      ["Replication and Synchronization Strategy", "Replication-and-Synchronization-Strategy"],
      ["Operations and CLI Strategy", "Operations-and-CLI-Strategy"],
      ["Client SDK Specifications", "Client-SDK-Specifications"],
    ],
  },
  {
    section: "Use Cases",
    pages: [
      ["Use Cases", "Use-Cases"],
      ["Use Case: Local Write and Commit", "Use-Cases-Local-Write-and-Commit"],
      ["Use Case: Offline First Sync", "Use-Cases-Offline-First-Sync"],
      ["Use Case: Indexed Query Read", "Use-Cases-Indexed-Query-Read"],
      ["Use Case: Integrity Verification", "Use-Cases-Integrity-Verification"],
      ["Use Case: Conflict Detection and Resolution", "Use-Cases-Conflict-Detection-and-Resolution"],
    ],
  },
  {
    section: "Operations",
    pages: [
      ["CLI Reference", "CLI-Reference"],
      ["Troubleshooting", "Troubleshooting"],
    ],
  },
];

const DEFAULT_PAGE = "Home";
const navEl = document.getElementById("nav");
const contentEl = document.getElementById("content");
const searchEl = document.getElementById("search");
const currentPageEl = document.getElementById("current-page");
const ALL_PAGE_SLUGS = NAV.flatMap((group) => group.pages.map((p) => p[1]));

let mermaidConfigured = false;

marked.setOptions({
  gfm: true,
  breaks: false,
  mangle: false,
  headerIds: true,
});

function getPageFromUrl() {
  const page = new URLSearchParams(window.location.search).get("page");
  if (page && ALL_PAGE_SLUGS.includes(page)) {
    return page;
  }
  return DEFAULT_PAGE;
}

function pageLabel(slug) {
  for (const group of NAV) {
    const found = group.pages.find((entry) => entry[1] === slug);
    if (found) {
      return found[0];
    }
  }
  return slug.replaceAll("-", " ");
}

function escapeHtml(value) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function rewriteInternalLinks() {
  const anchors = contentEl.querySelectorAll("a[href]");
  for (const anchor of anchors) {
    const href = anchor.getAttribute("href") || "";
    if (
      !href ||
      href.startsWith("http://") ||
      href.startsWith("https://") ||
      href.startsWith("mailto:") ||
      href.startsWith("#") ||
      href.startsWith("/")
    ) {
      continue;
    }

    const clean = href.replace(/^\.\//, "").replace(/\.md$/, "").replace(/\/$/, "");
    if (!ALL_PAGE_SLUGS.includes(clean)) {
      continue;
    }

    anchor.setAttribute("href", `?page=${encodeURIComponent(clean)}`);
  }
}

function renderNavigation(activePage, filterText = "") {
  const normalizedFilter = filterText.trim().toLowerCase();
  navEl.innerHTML = "";

  for (const group of NAV) {
    const filteredPages = group.pages.filter(([label]) => {
      if (!normalizedFilter) {
        return true;
      }
      return label.toLowerCase().includes(normalizedFilter);
    });

    if (!filteredPages.length) {
      continue;
    }

    const groupWrap = document.createElement("section");
    groupWrap.className = "nav-group";

    const title = document.createElement("h3");
    title.className = "nav-title";
    title.textContent = group.section;
    groupWrap.appendChild(title);

    const list = document.createElement("ul");
    list.className = "nav-list";

    for (const [label, slug] of filteredPages) {
      const li = document.createElement("li");
      const link = document.createElement("a");
      link.href = `?page=${encodeURIComponent(slug)}`;
      link.textContent = label;
      link.className = `nav-link${slug === activePage ? " active" : ""}`;
      li.appendChild(link);
      list.appendChild(li);
    }

    groupWrap.appendChild(list);
    navEl.appendChild(groupWrap);
  }
}

function upgradeMermaidBlocks() {
  const mermaidCodeBlocks = contentEl.querySelectorAll("pre code.language-mermaid, code.language-mermaid");

  for (const codeBlock of mermaidCodeBlocks) {
    const diagramText = (codeBlock.textContent || "").trim();
    if (!diagramText) {
      continue;
    }

    const wrapper = document.createElement("div");
    wrapper.className = "mermaid";
    wrapper.textContent = diagramText;

    const pre = codeBlock.closest("pre");
    if (pre) {
      pre.replaceWith(wrapper);
    } else {
      codeBlock.replaceWith(wrapper);
    }
  }
}

function ensureMermaidConfigured() {
  if (mermaidConfigured || typeof mermaid === "undefined") {
    return;
  }
  mermaid.initialize({
    startOnLoad: false,
    securityLevel: "loose",
    theme: "dark",
    fontFamily: "Inter, sans-serif",
    themeVariables: {
      primaryColor: "#111113",
      primaryTextColor: "#e5e5e5",
      primaryBorderColor: "#8b5cf6",
      lineColor: "#8b5cf6",
      secondaryColor: "#0a0a0a",
      tertiaryColor: "#050505",
      actorBorder: "#8b5cf6",
      actorBkg: "#111113",
      actorTextColor: "#e5e5e5",
      signalColor: "#f59e0b",
      labelBoxBkgColor: "#111113",
      labelBoxBorderColor: "#8b5cf6",
      labelTextColor: "#e5e5e5",
      noteBkgColor: "#1a1325",
      noteBorderColor: "#8b5cf6",
      noteTextColor: "#d8d8dc",
      background: "#050505",
    },
  });
  mermaidConfigured = true;
}

async function renderMermaid() {
  const blocks = contentEl.querySelectorAll(".mermaid");
  if (!blocks.length) {
    return;
  }

  ensureMermaidConfigured();

  if (typeof mermaid === "undefined") {
    return;
  }

  try {
    await mermaid.run({ nodes: blocks });
  } catch (error) {
    console.error("Mermaid render error:", error);
  }
}

async function loadPage(pageSlug) {
  currentPageEl.textContent = pageLabel(pageSlug);
  document.title = `${pageLabel(pageSlug)} | LedgerDB Docs`;
  renderNavigation(pageSlug, searchEl.value);

  try {
    const response = await fetch(`${pageSlug}.md`, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`Unable to load page ${pageSlug} (status ${response.status})`);
    }

    const markdown = await response.text();
    contentEl.innerHTML = marked.parse(markdown);
    upgradeMermaidBlocks();
    rewriteInternalLinks();
    await renderMermaid();
  } catch (error) {
    contentEl.innerHTML = `
      <div class="error-card">
        <strong>Could not load this page.</strong><br>
        ${escapeHtml(error.message)}
      </div>
    `;
  }
}

window.addEventListener("popstate", () => {
  loadPage(getPageFromUrl());
});

document.addEventListener("click", (event) => {
  const target = event.target.closest("a[href]");
  if (!target) {
    return;
  }

  const href = target.getAttribute("href") || "";
  if (!href.startsWith("?page=")) {
    return;
  }

  event.preventDefault();
  const url = new URL(href, window.location.href);
  const page = url.searchParams.get("page");
  if (!page || !ALL_PAGE_SLUGS.includes(page)) {
    return;
  }

  history.pushState({}, "", `?page=${encodeURIComponent(page)}`);
  loadPage(page);
});

searchEl.addEventListener("input", () => {
  renderNavigation(getPageFromUrl(), searchEl.value);
});

loadPage(getPageFromUrl());
