import { useState } from 'react';
import './DiffView.css';

interface Props {
    leftData: string; // base64
    rightData: string; // base64
    leftLabel: string;
    rightLabel: string;
    onClose: () => void;
}

type DiffMode = 'text' | 'hex';

const b64ToBytes = (b64: string): Uint8Array => {
    const bin = atob(b64);
    const arr = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i);
    return arr;
};

const toText = (bytes: Uint8Array): string => {
    try { return new TextDecoder().decode(bytes); }
    catch { return Array.from(bytes).map(b => b >= 0x20 && b <= 0x7e ? String.fromCharCode(b) : '.').join(''); }
};

interface CharToken {
    char: string;
    changed: boolean;
}

interface DiffLine {
    left: string;
    right: string;
    type: 'same' | 'changed' | 'added' | 'removed';
    // Populated for 'changed' lines only
    leftTokens?: CharToken[];
    rightTokens?: CharToken[];
}

// Character-level LCS diff between two strings
function charDiff(oldStr: string, newStr: string): { left: CharToken[], right: CharToken[] } {
    const a = Array.from(oldStr);
    const b = Array.from(newStr);
    const n = a.length, m = b.length;

    const dp: number[][] = Array.from({ length: n + 1 }, () => new Array(m + 1).fill(0));
    for (let i = 1; i <= n; i++) {
        for (let j = 1; j <= m; j++) {
            dp[i][j] = a[i - 1] === b[j - 1]
                ? dp[i - 1][j - 1] + 1
                : Math.max(dp[i - 1][j], dp[i][j - 1]);
        }
    }

    const leftTokens: CharToken[] = [];
    const rightTokens: CharToken[] = [];
    let i = n, j = m;
    const leftStack: CharToken[] = [];
    const rightStack: CharToken[] = [];

    while (i > 0 || j > 0) {
        if (i > 0 && j > 0 && a[i - 1] === b[j - 1]) {
            leftStack.push({ char: a[i - 1], changed: false });
            rightStack.push({ char: b[j - 1], changed: false });
            i--; j--;
        } else if (j > 0 && (i === 0 || dp[i][j - 1] >= dp[i - 1][j])) {
            rightStack.push({ char: b[j - 1], changed: true });
            j--;
        } else {
            leftStack.push({ char: a[i - 1], changed: true });
            i--;
        }
    }

    for (let k = leftStack.length - 1; k >= 0; k--) leftTokens.push(leftStack[k]);
    for (let k = rightStack.length - 1; k >= 0; k--) rightTokens.push(rightStack[k]);

    return { left: leftTokens, right: rightTokens };
}

// LCS-based line diff (like git)
function computeLineDiff(leftText: string, rightText: string): DiffLine[] {
    const a = leftText.split('\n');
    const b = rightText.split('\n');
    const n = a.length, m = b.length;

    // Build LCS table
    const dp: number[][] = Array.from({ length: n + 1 }, () => new Array(m + 1).fill(0));
    for (let i = 1; i <= n; i++) {
        for (let j = 1; j <= m; j++) {
            dp[i][j] = a[i - 1] === b[j - 1] ? dp[i - 1][j - 1] + 1 : Math.max(dp[i - 1][j], dp[i][j - 1]);
        }
    }

    // Backtrack to produce diff
    const result: DiffLine[] = [];
    let i = n, j = m;
    const stack: DiffLine[] = [];

    while (i > 0 || j > 0) {
        if (i > 0 && j > 0 && a[i - 1] === b[j - 1]) {
            stack.push({ left: a[i - 1], right: b[j - 1], type: 'same' });
            i--; j--;
        } else if (j > 0 && (i === 0 || dp[i][j - 1] >= dp[i - 1][j])) {
            stack.push({ left: '', right: b[j - 1], type: 'added' });
            j--;
        } else {
            stack.push({ left: a[i - 1], right: '', type: 'removed' });
            i--;
        }
    }

    // Reverse (backtrack produces reversed order)
    for (let k = stack.length - 1; k >= 0; k--) {
        result.push(stack[k]);
    }

    // Second pass: pair adjacent removed+added lines as 'changed' with char-level diff
    const paired: DiffLine[] = [];
    let p = 0;
    while (p < result.length) {
        if (
            result[p].type === 'removed' &&
            p + 1 < result.length &&
            result[p + 1].type === 'added'
        ) {
            const { left: leftTokens, right: rightTokens } = charDiff(result[p].left, result[p + 1].right);
            paired.push({
                left: result[p].left,
                right: result[p + 1].right,
                type: 'changed',
                leftTokens,
                rightTokens,
            });
            p += 2;
        } else {
            paired.push(result[p]);
            p++;
        }
    }

    return paired;
}

interface HexDiffByte {
    offset: number;
    left: number | null;
    right: number | null;
    different: boolean;
}

function computeHexDiff(leftBytes: Uint8Array, rightBytes: Uint8Array): HexDiffByte[] {
    const maxLen = Math.max(leftBytes.length, rightBytes.length);
    const result: HexDiffByte[] = [];
    for (let i = 0; i < maxLen; i++) {
        const l = i < leftBytes.length ? leftBytes[i] : null;
        const r = i < rightBytes.length ? rightBytes[i] : null;
        result.push({ offset: i, left: l, right: r, different: l !== r });
    }
    return result;
}

function renderHexSide(diff: HexDiffByte[], side: 'left' | 'right'): JSX.Element[] {
    const lines: JSX.Element[] = [];
    for (let row = 0; row < diff.length; row += 16) {
        const chunk = diff.slice(row, row + 16);
        const offset = row.toString(16).padStart(4, '0');
        const hexParts: JSX.Element[] = [];
        const asciiParts: JSX.Element[] = [];

        for (let i = 0; i < 16; i++) {
            const b = i < chunk.length ? chunk[i] : null;
            const val = b ? (side === 'left' ? b.left : b.right) : null;
            const isDiff = b?.different || false;

            if (val !== null) {
                hexParts.push(
                    <span key={i} className={isDiff ? 'diff-byte' : ''}>
                        {val.toString(16).padStart(2, '0')}
                    </span>
                );
                const ch = val >= 0x20 && val <= 0x7e ? String.fromCharCode(val) : '.';
                asciiParts.push(
                    <span key={i} className={isDiff ? 'diff-byte' : ''}>{ch}</span>
                );
            } else {
                hexParts.push(<span key={i} className="diff-empty">  </span>);
                asciiParts.push(<span key={i}> </span>);
            }
            if (i < 15) hexParts.push(<span key={`s${i}`}> </span>);
        }

        lines.push(
            <div key={row} className="hex-line">
                <span className="hex-offset">{offset}:</span>
                <span className="hex-bytes">{hexParts}</span>
                <span className="hex-ascii">{asciiParts}</span>
            </div>
        );
    }
    return lines;
}

export default function DiffView({ leftData, rightData, leftLabel, rightLabel, onClose }: Props) {
    const [mode, setMode] = useState<DiffMode>('text');

    const leftBytes = b64ToBytes(leftData);
    const rightBytes = b64ToBytes(rightData);
    const leftText = toText(leftBytes);
    const rightText = toText(rightBytes);

    const lineDiff = computeLineDiff(leftText, rightText);
    const hexDiff = computeHexDiff(leftBytes, rightBytes);
    const diffCount = mode === 'text'
        ? lineDiff.filter(d => d.type !== 'same').length
        : hexDiff.filter(d => d.different).length;

    return (
        <div className="diff-overlay" onClick={onClose}>
            <div className="diff-modal" onClick={e => e.stopPropagation()}>
                <div className="diff-header">
                    <div className="diff-header-left">
                        <h3>Сравнение пакетов</h3>
                        <span className="diff-count">{diffCount} различий</span>
                    </div>
                    <div className="diff-header-right">
                        <button className={`view-mode-btn ${mode === 'text' ? 'active' : ''}`}
                            onClick={() => setMode('text')}>TXT</button>
                        <button className={`view-mode-btn ${mode === 'hex' ? 'active' : ''}`}
                            onClick={() => setMode('hex')}>HEX</button>
                        <button className="close-btn" onClick={onClose}>✕</button>
                    </div>
                </div>

                <div className="diff-labels">
                    <span className="diff-label">{leftLabel}</span>
                    <span className="diff-label">{rightLabel}</span>
                </div>

                <div className="diff-body">
                    {mode === 'text' ? (
                        <div className="diff-text-panels">
                            <pre className="diff-text-panel diff-left">
                                {lineDiff.map((d, i) => (
                                    <div key={i} className={`diff-line diff-${d.type}`}>
                                        {d.type === 'added'
                                            ? '\u00A0'
                                            : d.type === 'changed' && d.leftTokens
                                                ? d.leftTokens.map((t, j) => (
                                                    <span key={j} className={t.changed ? 'diff-char-changed diff-char-removed' : ''}>{t.char}</span>
                                                ))
                                                : (d.left || '\u00A0')}
                                    </div>
                                ))}
                            </pre>
                            <pre className="diff-text-panel diff-right">
                                {lineDiff.map((d, i) => (
                                    <div key={i} className={`diff-line diff-${d.type}`}>
                                        {d.type === 'removed'
                                            ? '\u00A0'
                                            : d.type === 'changed' && d.rightTokens
                                                ? d.rightTokens.map((t, j) => (
                                                    <span key={j} className={t.changed ? 'diff-char-changed diff-char-added' : ''}>{t.char}</span>
                                                ))
                                                : (d.right || '\u00A0')}
                                    </div>
                                ))}
                            </pre>
                        </div>
                    ) : (
                        <div className="diff-hex-panels">
                            <pre className="diff-hex-panel">{renderHexSide(hexDiff, 'left')}</pre>
                            <pre className="diff-hex-panel">{renderHexSide(hexDiff, 'right')}</pre>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
