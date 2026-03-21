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

interface DiffLine {
    left: string;
    right: string;
    type: 'same' | 'changed' | 'added' | 'removed';
}

function computeLineDiff(leftText: string, rightText: string): DiffLine[] {
    const leftLines = leftText.split('\n');
    const rightLines = rightText.split('\n');
    const maxLen = Math.max(leftLines.length, rightLines.length);
    const result: DiffLine[] = [];

    for (let i = 0; i < maxLen; i++) {
        const l = i < leftLines.length ? leftLines[i] : '';
        const r = i < rightLines.length ? rightLines[i] : '';

        if (i >= leftLines.length) {
            result.push({ left: '', right: r, type: 'added' });
        } else if (i >= rightLines.length) {
            result.push({ left: l, right: '', type: 'removed' });
        } else if (l === r) {
            result.push({ left: l, right: r, type: 'same' });
        } else {
            result.push({ left: l, right: r, type: 'changed' });
        }
    }
    return result;
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
                                        {d.left || '\u00A0'}
                                    </div>
                                ))}
                            </pre>
                            <pre className="diff-text-panel diff-right">
                                {lineDiff.map((d, i) => (
                                    <div key={i} className={`diff-line diff-${d.type}`}>
                                        {d.right || '\u00A0'}
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
