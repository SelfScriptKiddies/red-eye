// Build-time regression check: the production dist tree must be fully
// self-hosted. Run as `node scripts/check-offline-assets.mjs [distDir]`
// after `vite build`; every exported helper is unit-testable (see
// scripts/check-offline-assets.test.mjs).
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { join, relative, resolve, dirname, sep } from 'node:path';
import { pathToFileURL } from 'node:url';

export const CDN_HOSTS = [
    'kit.fontawesome.com',
    'cdnjs.cloudflare.com',
    'cdn.jsdelivr.net',
    'unpkg.com',
    'fonts.googleapis.com',
    'fonts.gstatic.com',
];

export const REQUIRED_FONTS = [
    { family: 'Inter', subsets: ['latin', 'cyrillic'] },
    { family: 'JetBrains Mono', subsets: ['latin', 'cyrillic'] },
    { family: 'Fira Code', subsets: ['latin', 'cyrillic'] },
];

export const REQUIRED_LICENSES = [
    'fonts/licenses/INTER-LICENSE.txt',
    'fonts/licenses/FIRA-CODE-LICENSE.txt',
    'fonts/licenses/JETBRAINS-MONO-LICENSE.txt',
];

const VITE_HASH = /-[A-Za-z0-9_-]{8}$/;
const ASSET_EXT = /\.(js|css|mjs|woff2?|ttf|eot|png|jpe?g|gif|webp|avif|svg|ico|json|txt|map)$/i;
const SKIP_SCHEME = /^(data|blob|mailto|tel|about|javascript):/i;

const kebab = (s) => s.toLowerCase().replace(/\s+/g, '-');

export function stripCssComments(css) {
    return css.replace(/\/\*[\s\S]*?\*\//g, ' ');
}

export function isExternalRef(url) {
    if (!url) return false;
    const u = url.trim();
    if (u === '' || u.startsWith('#')) return false;
    if (u.startsWith('//')) return true;
    if (SKIP_SCHEME.test(u)) return false;
    // Any other absolute scheme (http, https, ws, app-specific) is external.
    return /^[a-z][a-z0-9+.-]*:/i.test(u);
}

// Structural HTML scan: only asset-bearing attributes of asset-bearing tags
// are collected, so URLs inside text content stay data and never fail.
export function findHtmlRefs(html) {
    const refs = [];
    const tagRe = /<(script|link|img|source|iframe|video|audio|track|embed|object|use)\b([^>]*)>/gi;
    const attrRe = /\b(src|href|data|srcset)\s*=\s*("([^"]*)"|'([^']*)'|([^\s>]+))/gi;
    for (const tag of html.matchAll(tagRe)) {
        const name = tag[1].toLowerCase();
        for (const attr of tag[2].matchAll(attrRe)) {
            const attrName = attr[1].toLowerCase();
            const value = attr[3] ?? attr[4] ?? attr[5] ?? '';
            if (name === 'link' && attrName !== 'href') continue;
            if (name === 'object' && attrName !== 'data') continue;
            if (name === 'use' && attrName !== 'href') continue;
            if (attrName === 'srcset') {
                for (const part of value.split(',')) {
                    const candidate = part.trim().split(/\s+/)[0];
                    if (candidate) refs.push({ tag: name, attr: 'srcset', url: candidate });
                }
                continue;
            }
            refs.push({ tag: name, attr: attrName, url: value });
        }
    }
    return refs;
}

export function findCssRefs(css) {
    const refs = [];
    const clean = stripCssComments(css);
    const importRe = /@import\s+(?:url\s*\(\s*)?["']?\s*([^"'()\s;]+)/gi;
    for (const m of clean.matchAll(importRe)) {
        refs.push({ kind: 'import', url: m[1] });
    }
    // url() only, not url( in a selector or string content: the token must
    // terminate, and quotes/backslashes cannot appear in a real CSS url token.
    const urlRe = /url\s*\(\s*(?:"([^"]*)"|'([^']*)'|([^)'"][^);]*))\s*\)/gi;
    for (const m of clean.matchAll(urlRe)) {
        refs.push({ kind: 'url', url: (m[1] ?? m[2] ?? m[3] ?? '').trim() });
    }
    return refs;
}

// JS scan: a CDN host only counts when it sits in a loader position (assigned
// to src/href/url, or passed to import/fetch/Worker). URLs inside example
// code strings (curl, pwntools, farm configs) are data and are ignored.
const LOADER_TAILS = [
    /(?:^|[^\w.])(?:src|href|action|data|url)\s*[:=]\s*["'`][^"'`]*$/i,
    /\bimport\s*\(\s*["'`][^"'`]*$/i,
    /\bfrom\s+["'`][^"'`]*$/i,
    /\bfetch\s*\(\s*["'`][^"'`]*$/i,
    /\bnew\s+Worker\s*\(\s*["'`][^"'`]*$/i,
    /\burl\s*\(\s*["']?[^)"']*$/i,
    /@import\s+["']?[^;"']*$/i,
];

export function findJsCdnLoaders(js) {
    const hits = [];
    for (const host of CDN_HOSTS) {
        let idx = js.indexOf(host);
        while (idx !== -1) {
            const before = js.slice(Math.max(0, idx - 120), idx);
            if (LOADER_TAILS.some((re) => re.test(before))) {
                hits.push({ host, context: before.slice(-60) });
            }
            idx = js.indexOf(host, idx + host.length);
        }
    }
    return hits;
}

function containsCdnHost(text) {
    return CDN_HOSTS.some((host) => text.includes(host));
}

function stripQueryHash(url) {
    return url.replace(/[?#].*$/, '');
}

// A missing root-relative reference without a file extension is treated as a
// same-origin SPA route (e.g. href="/agents"), not a broken asset.
function isLikelyRoute(url) {
    return url.startsWith('/') && !ASSET_EXT.test(stripQueryHash(url));
}

export function checkDist(distDir, exists = existsSync) {
    const violations = [];
    const fail = (msg) => violations.push(msg);

    const indexHtmlPath = join(distDir, 'index.html');
    if (!exists(indexHtmlPath)) {
        fail(`missing ${indexHtmlPath}`);
        return violations;
    }
    const indexHtml = readFileSync(indexHtmlPath, 'utf8');

    for (const { tag, attr, url } of findHtmlRefs(indexHtml)) {
        if (isExternalRef(url)) {
            fail(`index.html external ${tag}[${attr}]: ${url}`);
            continue;
        }
        if (url === '' || url.startsWith('#') || SKIP_SCHEME.test(url)) continue;
        if (!resolvesInDist(stripQueryHash(url), distDir, distDir, exists) && !isLikelyRoute(url)) {
            fail(`index.html unresolved ${tag}[${attr}]: ${url}`);
        }
    }

    const assetsDir = join(distDir, 'assets');
    const cssFiles = exists(assetsDir)
        ? readdirSync(assetsDir).filter((f) => f.endsWith('.css')).map((f) => join(assetsDir, f))
        : [];
    if (cssFiles.length === 0) fail('no built CSS found under dist/assets/');

    const allCss = cssFiles.map((p) => readFileSync(p, 'utf8')).join('\n');
    for (const cssFile of cssFiles) {
        const css = readFileSync(cssFile, 'utf8');
        for (const { kind, url } of findCssRefs(css)) {
            if (isExternalRef(url)) {
                fail(`built CSS remote ${kind}: ${url} (${basename(cssFile)})`);
                continue;
            }
            if (url === '' || SKIP_SCHEME.test(url)) continue;
            if (!resolvesInDist(stripQueryHash(url), dirname(cssFile), distDir, exists)) {
                fail(`built CSS unresolved ${kind}: ${url} (${basename(cssFile)})`);
            }
        }
    }

    if (containsCdnHost(indexHtml)) {
        for (const host of CDN_HOSTS) {
            if (indexHtml.includes(host)) fail(`index.html references CDN host ${host}`);
        }
    }
    for (const styleBlock of indexHtml.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/gi)) {
        for (const { kind, url } of findCssRefs(styleBlock[1])) {
            if (isExternalRef(url)) fail(`index.html inline style remote ${kind}: ${url}`);
        }
    }

    const jsFiles = [];
    collectFiles(distDir, jsFiles, exists);
    for (const file of jsFiles) {
        if (!/\.(js|mjs)$/.test(file)) continue;
        const js = readFileSync(file, 'utf8');
        for (const { host } of findJsCdnLoaders(js)) {
            fail(`bundled JS CDN loader for ${host} (${relative(distDir, file)})`);
        }
    }

    for (const { family, subsets } of REQUIRED_FONTS) {
        const faceRe = new RegExp(`font-face[^}]*font-family:\\s*['"]?${family}['"]?\\s*;`);
        if (!faceRe.test(allCss)) fail(`built CSS has no @font-face for ${family}`);
        for (const subset of subsets) {
            const name = `${kebab(family)}-${subset}-wght-normal`;
            const hashed = exists(assetsDir) &&
                readdirSync(assetsDir).some((f) => f.startsWith(`${name}-`) && f.endsWith('.woff2'));
            if (!hashed) fail(`dist is missing hashed WOFF2 for ${family} (${subset})`);
        }
    }
    if (!/font-display:\s*swap/.test(allCss.replace(/\s+/g, ''))) {
        fail('built CSS has no font-display: swap on @font-face rules');
    }

    for (const lic of REQUIRED_LICENSES) {
        const p = join(distDir, lic);
        if (!exists(p)) {
            fail(`dist is missing font license ${lic}`);
        } else if (!/Open Font License/i.test(readFileSync(p, 'utf8'))) {
            fail(`font license ${lic} does not look like an OFL text`);
        }
    }

    checkHashing(distDir, fail, exists);

    return violations;
}

function basename(p) {
    return p.split(sep).pop();
}

function resolvesInDist(url, baseDir, distDir, exists) {
    const target = url.startsWith('/')
        ? join(distDir, url.slice(1))
        : resolve(baseDir, url);
    const rel = relative(distDir, target);
    if (rel.startsWith('..') || resolve(target) === resolve(distDir)) return false;
    return exists(target);
}

function collectFiles(dir, out, exists) {
    if (!exists(dir)) return;
    for (const entry of readdirSync(dir)) {
        const p = join(dir, entry);
        if (statSync(p).isDirectory()) collectFiles(p, out, exists);
        else out.push(p);
    }
}

function checkHashing(distDir, fail, exists) {
    const assetsDir = join(distDir, 'assets');
    if (!exists(assetsDir)) {
        fail('dist/assets/ is missing: hashed and unhashed assets cannot be told apart');
        return;
    }
    const hashed = readdirSync(assetsDir).filter((f) => {
        const dot = f.lastIndexOf('.');
        return dot > 0 && VITE_HASH.test(f.slice(0, dot));
    });
    const all = readdirSync(assetsDir);
    if (hashed.length === 0) fail('no content-hashed files under dist/assets/');
    const unhashed = all.filter((f) => !hashed.includes(f));
    if (unhashed.length > 0) {
        fail(`unhashed files inside dist/assets/ (must be hashed or moved to dist root): ${unhashed.join(', ')}`);
    }
    for (const entry of readdirSync(distDir, { withFileTypes: true })) {
        if (!entry.isFile()) continue;
        const dot = entry.name.lastIndexOf('.');
        if (dot > 0 && VITE_HASH.test(entry.name.slice(0, dot))) {
            fail(`hashed file at dist root must live under assets/: ${entry.name}`);
        }
    }
}

function main() {
    const distDir = process.argv[2] ?? 'dist';
    if (!existsSync(distDir)) {
        console.error(`offline-assets check: dist directory not found: ${distDir}`);
        process.exit(1);
    }
    const violations = checkDist(distDir);
    if (violations.length > 0) {
        console.error(`offline-assets check FAILED (${violations.length}):`);
        for (const v of violations) console.error(`  - ${v}`);
        process.exit(1);
    }
    console.log('offline-assets check: dist is fully self-hosted');
}

const isMain = process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href;
if (isMain) main();
