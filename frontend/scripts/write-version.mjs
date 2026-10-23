// Writes dist/version.json — the build artifact the backend's
// /api/version endpoint fetches (backend/src/api/version.rs parses this
// shape; keep the fields in sync).
//
// The git sha comes from RED_EYE_GIT_SHA when set (Docker build arg, see
// frontend/Dockerfile — no .git in the image context), else from the local
// git checkout, else "unknown".
import { execSync } from 'node:child_process';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';

const pkg = JSON.parse(readFileSync('package.json', 'utf8'));

const sha =
    process.env.RED_EYE_GIT_SHA?.trim() ||
    (() => {
        try {
            return execSync('git rev-parse --short HEAD').toString().trim();
        } catch {
            return 'unknown';
        }
    })();

mkdirSync('dist', { recursive: true });
writeFileSync(
    'dist/version.json',
    JSON.stringify(
        {
            service: 'frontend',
            version: pkg.version,
            git_sha: sha,
            built_at: new Date().toISOString(),
        },
        null,
        2,
    ) + '\n',
);
console.log(`version.json: ${pkg.version}+${sha}`);
