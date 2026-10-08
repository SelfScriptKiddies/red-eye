import { describe, it, expect } from 'vitest';
import { execFileSync } from 'node:child_process';
import { shq, pyStr } from './exportEscaping';

// Nasty inputs every test below must survive:
const NASTY = [
    `'; rm -rf /; '`,
    `$(reboot)`,
    `back\\slash'"quote'`,   // backslash + double quote + single quote mix
    `a\nb\tc\rd`,             // control characters
    `\`; cat /etc/passwd\``, // backtick command substitution
];

describe('shq — POSIX shell single-quote escaping', () => {
    it('wraps plain values', () => {
        expect(shq('simple')).toBe(`'simple'`);
        expect(shq('')).toBe(`''`);
    });

    it('escapes embedded single quotes POSIX-style', () => {
        expect(shq("it's")).toBe(`'it'\\''s'`);
        expect(shq(`'; rm -rf /; '`).startsWith(`''\\''`)).toBe(true);
    });

    it('round-trips through a real /bin/sh (no command execution)', () => {
        for (const input of NASTY) {
            // printf %s <quoted word> must echo the input back verbatim; if the
            // payload broke out of the quotes, sh would either fail or, for
            // $(reboot)-style inputs, expand them (changing the output).
            const out = execFileSync(
                '/bin/sh',
                ['-c', `printf %s ${shq(input)}`],
                { encoding: 'utf8' },
            );
            expect(out).toBe(input);
        }
    });

    it('keeps $(...) literal', () => {
        const out = execFileSync(
            '/bin/sh',
            ['-c', `printf %s ${shq('$(echo pwned)')}`],
            { encoding: 'utf8' },
        );
        expect(out).toBe('$(echo pwned)');
    });
});

describe('pyStr — Python string literal escaping', () => {
    it('wraps plain values', () => {
        expect(pyStr('simple')).toBe(`'simple'`);
        expect(pyStr('')).toBe(`''`);
    });

    it('escapes backslash and single quote', () => {
        expect(pyStr('a\\b')).toBe(`'a\\\\b'`);
        expect(pyStr("it's")).toBe(`'it\\'s'`);
        expect(pyStr(`back\\slash'"quote'`)).toBe(`'back\\\\slash\\'"quote\\''`);
    });

    it('escapes control characters', () => {
        expect(pyStr('a\nb\tc\rd')).toBe(`'a\\nb\\tc\\rd'`);
        expect(pyStr('\x00\x1f\x7f')).toBe(`'\\x00\\x1f\\x7f'`);
    });

    it('escapes non-ASCII as \\xNN / \\uXXXX; astral chars stay raw', () => {
        expect(pyStr('é')).toBe(`'\\xe9'`);
        expect(pyStr('ж')).toBe(`'\\u0436'`);
        expect(pyStr('😀')).toBe(`'😀'`);
    });

    it('contains no unescaped quote or backslash for nasty inputs', () => {
        for (const input of NASTY) {
            const literal = pyStr(input);
            // Strip the surrounding quotes, then no backslash may be unescaped
            // and no raw single quote may appear (all are \\ or \' pairs).
            const inner = literal.slice(1, -1);
            expect(inner).not.toMatch(/(?<!\\)'/);
            expect(inner).not.toMatch(/(?<!\\)\\(?!['\\nrtxu])/);
        }
    });

    it('injection payloads stay inside the literal', () => {
        expect(pyStr(`'; rm -rf /; '`)).toBe(`'\\'; rm -rf /; \\''`);
        expect(pyStr('$(reboot)')).toBe(`'$(reboot)'`);
    });
});
