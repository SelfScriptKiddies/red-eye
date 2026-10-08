// Escaping helpers for export generators (curl / python requests snippets).
//
// Exported commands interpolate CAPTURED PACKET CONTENT — attacker-controlled
// data — into shell and Python source that the operator will paste into a
// terminal. Every interpolated value must go through these helpers so a
// malicious payload can never break out of the literal and execute as code
// on the operator's machine.

/**
 * POSIX shell single-quoted word: wraps `value` in single quotes and escapes
 * any embedded single quote as '\'' (close quote, escaped quote, reopen).
 * Safe for bash/dash/zsh: the result is always exactly one shell word.
 */
export function shq(value: string): string {
    return `'${value.replace(/'/g, `'\\''`)}'`;
}

/**
 * Python single-quoted string literal. Backslash and quote are escaped;
 * control characters become \xNN / \n / \r / \t; characters above 0xFF become
 * \uXXXX (surrogate pair above 0xFFFF), which is correct for any JS string.
 */
export function pyStr(value: string): string {
    let out = "'";
    for (const ch of value) {
        const code = ch.codePointAt(0)!;
        if (ch === '\\') {
            out += '\\\\';
        } else if (ch === "'") {
            out += "\\'";
        } else if (ch === '\n') {
            out += '\\n';
        } else if (ch === '\r') {
            out += '\\r';
        } else if (ch === '\t') {
            out += '\\t';
        } else if (code < 0x20) {
            out += `\\x${code.toString(16).padStart(2, '0')}`;
        } else if (code >= 0x7f && code <= 0xff) {
            // 0x7F..0xFF: atob()-decoded payload bytes and DEL — emit \xNN so
            // the generated snippet stays pure ASCII regardless of how the
            // clipboard transports it.
            out += `\\x${code.toString(16).padStart(2, '0')}`;
        } else if (code > 0xff && code <= 0xffff) {
            out += `\\u${code.toString(16).padStart(4, '0')}`;
        } else {
            // Printable ASCII, or astral-plane characters (Python strings are
            // code-point based: surrogate \u pairs would NOT recombine there,
            // and a code point above 0xFFFF can never be a quote/backslash,
            // so emitting it raw is safe).
            out += ch;
        }
    }
    return out + "'";
}
