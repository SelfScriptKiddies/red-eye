import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtempSync, readFileSync, rmSync, writeFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
    CDN_HOSTS,
    isExternalRef,
    findHtmlRefs,
    findCssRefs,
    findJsCdnLoaders,
    checkDist,
} from './check-offline-assets.mjs';

describe('isExternalRef', () => {
    it('flags absolute and protocol-relative URLs', () => {
        expect(isExternalRef('https://kit.fontawesome.com/x.js')).toBe(true);
        expect(isExternalRef('http://10.60.0.1:1337')).toBe(true);
        expect(isExternalRef('//cdn.example.com/x.js')).toBe(true);
        expect(isExternalRef('ws://example.com')).toBe(true);
    });

    it('allows self-contained and same-origin references', () => {
        expect(isExternalRef('data:image/svg+xml;base64,AAA')).toBe(false);
        expect(isExternalRef('blob:https://app.local/x')).toBe(false);
        expect(isExternalRef('#anchor')).toBe(false);
        expect(isExternalRef('/assets/index-AbCd1234.js')).toBe(false);
        expect(isExternalRef('./fonts.css')).toBe(false);
        expect(isExternalRef('')).toBe(false);
    });
});

describe('findHtmlRefs', () => {
    it('collects asset-bearing attributes structurally', () => {
        const html = [
            '<html><head>',
            '<link rel="stylesheet" href="/assets/index-AbCd1234.css">',
            '<link rel="modulepreload" href="/assets/index-AbCd1234.js">',
            '<link rel="icon" type="image/svg+xml" href="/red-eye.svg">',
            '<link rel="preconnect" href="https://fonts.gstatic.com">',
            '</head><body>',
            '<script type="module" src="/assets/index-AbCd1234.js"></script>',
            '<img src="/red-eye.svg" srcset="/a-1x.png 1x, /a-2x.png 2x">',
            '<p>read the docs at https://example.com/guide or run curl https://10.60.0.1</p>',
            '</body></html>',
        ].join('');
        const urls = findHtmlRefs(html).map((r) => r.url);
        expect(urls).toContain('/assets/index-AbCd1234.css');
        expect(urls).toContain('/assets/index-AbCd1234.js');
        expect(urls).toContain('/red-eye.svg');
        expect(urls).toContain('https://fonts.gstatic.com');
        expect(urls).toContain('/a-2x.png');
        expect(urls.every((u) => !u.startsWith('https://example.com/guide'))).toBe(true);
        expect(urls.every((u) => !u.includes('curl'))).toBe(true);
    });

    it('data URLs are collected but never count as external', () => {
        const refs = findHtmlRefs('<img src="data:image/png;base64,AAA">');
        expect(refs).toHaveLength(1);
        expect(isExternalRef(refs[0].url)).toBe(false);
    });
});

describe('findCssRefs', () => {
    it('parses imports and url tokens', () => {
        const css = [
            '@import url("https://fonts.googleapis.com/css2?family=X");',
            "@import 'https://cdn.jsdelivr.net/npm/x/x.css';",
            'src: url(/assets/inter-latin-wght-normal-Dx4kXJAl.woff2);',
            "background-image: url('data:image/svg+xml,%3Csvg/%3E');",
        ].join('\n');
        const refs = findCssRefs(css);
        expect(refs.filter((r) => r.kind === 'import')).toHaveLength(2);
        const remote = refs.filter((r) => isExternalRef(r.url));
        expect(remote).toHaveLength(2);
        const local = refs.find((r) => r.url.startsWith('/assets/'));
        expect(local).toBeDefined();
    });

    it('ignores URLs inside comments', () => {
        const css = '/* @import url(https://fonts.googleapis.com/css?family=X); */\nbody{}';
        expect(findCssRefs(css)).toHaveLength(0);
    });
});

describe('findJsCdnLoaders', () => {
    it('flags script-src assignment, dynamic import and fetch of CDN origins', () => {
        const js = [
            'var s=document.createElement("script");s.src="https://kit.fontawesome.com/abc.js";',
            'import("https://cdn.jsdelivr.net/npm/x@1/x.mjs");',
            'fetch("https://unpkg.com/x/data.json");',
        ].join('\n');
        const hosts = findJsCdnLoaders(js).map((h) => h.host);
        expect(hosts).toContain('kit.fontawesome.com');
        expect(hosts).toContain('cdn.jsdelivr.net');
        expect(hosts).toContain('unpkg.com');
    });

    it('ignores CDN URLs inside example code and config strings', () => {
        const js = [
            'const help = `',
            'Upload: curl -F "file=@flag.txt" https://cdnjs.cloudflare.com/README',
            'from pwn import *; remote("10.60.0.1", 1337)',
            'farm: {"url": "https://cdnjs.cloudflare.com/submit", "token": "x"}',
            '`;',
            'const docs = "see https://cdnjs.cloudflare.com/ajax/libs/jquery for reference";',
            'const proxy_url = "https://kit.fontawesome.com";',
        ].join('\n');
        expect(findJsCdnLoaders(js)).toHaveLength(0);
    });

    it('does not treat farm_url style config keys as loader positions', () => {
        const js = 'const farm_url = "https://fonts.gstatic.com";';
        expect(findJsCdnLoaders(js)).toHaveLength(0);
    });

    it('exposes the canonical CDN deny list', () => {
        expect(CDN_HOSTS).toContain('fonts.googleapis.com');
        expect(CDN_HOSTS).toContain('kit.fontawesome.com');
    });
});

const FONT_FAMILIES = ['Inter', 'JetBrains Mono', 'Fira Code'];
const FONT_SUBSETS = ['latin', 'cyrillic'];

function writeFixtureDist(dir, { breakCssRef = false, unhashedAsset = false } = {}) {
    mkdirSync(join(dir, 'assets'));
    mkdirSync(join(dir, 'fonts', 'licenses'), { recursive: true });
    const cssUrl = breakCssRef ? '/assets/inter-latin-wght-normal-NOPE0000.woff2' : '/assets/fira-code-latin-wght-normal-AbCd1234.woff2';
    const fontFaces = FONT_FAMILIES.map((family) =>
        FONT_SUBSETS.map((subset) => {
            const slug = `${family.toLowerCase().replace(/\s+/g, '-')}-${subset}-wght-normal`;
            writeFileSync(join(dir, 'assets', `${slug}-AbCd1234.woff2`), 'woff2');
            return `@font-face{font-family:'${family}';font-style:normal;font-display:swap;font-weight:100 900;src:url(/assets/${slug}-AbCd1234.woff2) format("woff2-variations");}`;
        }).join('\n'),
    ).join('\n');
    writeFileSync(join(dir, 'assets', 'index-AbCd1234.css'), `${fontFaces}\n.a{background:url(${cssUrl})}`);
    writeFileSync(join(dir, 'assets', 'index-XYab12cd.js'), 'console.log(1)');
    writeFileSync(
        join(dir, 'index.html'),
        [
            '<html><head>',
            '<link rel="icon" type="image/svg+xml" href="/red-eye.svg">',
            '<link rel="stylesheet" href="/assets/index-AbCd1234.css">',
            '<link rel="alternate" href="/dashboard">',
            '</head><body>',
            '<script type="module" src="/assets/index-XYab12cd.js"></script>',
            '</body></html>',
        ].join(''),
    );
    writeFileSync(join(dir, 'version.json'), '{}\n');
    writeFileSync(join(dir, 'red-eye.svg'), '<svg xmlns="http://www.w3.org/2000/svg"/>');
    for (const name of ['INTER', 'FIRA-CODE', 'JETBRAINS-MONO']) {
        writeFileSync(join(dir, 'fonts', 'licenses', `${name}-LICENSE.txt`), 'SIL OPEN FONT LICENSE Version 1.1');
    }
    if (unhashedAsset) writeFileSync(join(dir, 'assets', 'logo.svg'), '<svg/>');
}

describe('checkDist', () => {
    let dist;

    beforeEach(() => {
        dist = mkdtempSync(join(tmpdir(), 'offline-assets-'));
    });

    afterEach(() => {
        rmSync(dist, { recursive: true, force: true });
    });

    it('accepts a fully self-hosted dist tree', () => {
        writeFixtureDist(dist);
        expect(checkDist(dist)).toEqual([]);
    });

    it('fails on an external script, stylesheet, icon or preload', () => {
        writeFixtureDist(dist);
        const html = readFileSync(join(dist, 'index.html'), 'utf8');
        writeFileSync(
            join(dist, 'index.html'),
            html.replace(
                '</head>',
                [
                    '<script src="https://kit.fontawesome.com/x.js"></script>',
                    '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=X">',
                    '<link rel="preload" href="https://cdn.jsdelivr.net/x.woff2" as="font">',
                ].join('') + '</head>',
            ),
        );
        const violations = checkDist(dist).join('\n');
        expect(violations).toContain('kit.fontawesome.com');
        expect(violations).toContain('fonts.googleapis.com');
        expect(violations).toContain('cdn.jsdelivr.net');
    });

    it('fails on remote CSS imports and url tokens', () => {
        writeFixtureDist(dist);
        const css = readFileSync(join(dist, 'assets', 'index-AbCd1234.css'), 'utf8');
        writeFileSync(
            join(dist, 'assets', 'index-AbCd1234.css'),
            css + '\n@import url(https://fonts.googleapis.com/css2?family=X);\n.a{background:url("https://fonts.gstatic.com/s/x.woff2")}',
        );
        const violations = checkDist(dist).join('\n');
        expect(violations).toContain('fonts.googleapis.com');
        expect(violations).toContain('fonts.gstatic.com');
    });

    it('fails when a local asset reference does not resolve in dist', () => {
        writeFixtureDist(dist, { breakCssRef: true });
        const violations = checkDist(dist).join('\n');
        expect(violations).toContain('NOPE0000');
    });

    it('allows root-relative extension-less refs as SPA routes', () => {
        writeFixtureDist(dist);
        expect(checkDist(dist).some((v) => v.includes('/dashboard'))).toBe(false);
    });

    it('fails when a required font subset is missing', () => {
        writeFixtureDist(dist);
        rmSync(join(dist, 'assets', 'fira-code-cyrillic-wght-normal-AbCd1234.woff2'));
        expect(checkDist(dist).join('\n')).toContain('Fira Code (cyrillic)');
    });

    it('fails when font licenses are missing', () => {
        writeFixtureDist(dist);
        rmSync(join(dist, 'fonts', 'licenses', 'INTER-LICENSE.txt'));
        expect(checkDist(dist).join('\n')).toContain('INTER-LICENSE.txt');
    });

    it('fails when hashed and unhashed assets are mixed under assets/', () => {
        writeFixtureDist(dist, { unhashedAsset: true });
        expect(checkDist(dist).join('\n')).toContain('unhashed files inside dist/assets/');
    });
});
