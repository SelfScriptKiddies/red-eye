import React, { useState } from 'react';
import './PacketCard.css';

type ViewMode = 'text' | 'hex' | 'python';

interface PacketProps {
    direction: string;
    id: number;
    packet_data: string;
    stream_id: number;
    timestamp: number;
}

const base64ToBytes = (b64: string): Uint8Array => {
    const binary = atob(b64);
    const len = binary.length;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
        bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
};

const toUtf8Preview = (bytes: Uint8Array): string => {
    try {
        return new TextDecoder().decode(bytes);
    } catch {
        return Array.from(bytes)
            .map((b) => (b >= 0x20 && b <= 0x7e ? String.fromCharCode(b) : '.'))
            .join('');
    }
};

const toHexDump = (bytes: Uint8Array): string => {
    const lines: string[] = [];
    for (let i = 0; i < bytes.length; i += 16) {
        const chunk = bytes.slice(i, i + 16);
        const hex = Array.from(chunk)
            .map((b) => b.toString(16).padStart(2, '0'))
            .join(' ');
        const ascii = Array.from(chunk)
            .map((b) => (b >= 0x20 && b <= 0x7e ? String.fromCharCode(b) : '.'))
            .join('');
        lines.push(`${i.toString(16).padStart(4, '0')}: ${hex.padEnd(16 * 3 - 1, ' ')}  ${ascii}`);
    }
    return lines.join('\n');
};

const toPythonBytesLiteral = (bytes: Uint8Array): string => {
    const body = Array.from(bytes)
        .map((b) => `\\x${b.toString(16).padStart(2, '0')}`)
        .join('');
    return `b'${body}'`;
};

export function PacketCard({ direction, id, packet_data, stream_id, timestamp }: PacketProps) {
    const [viewMode, setViewMode] = useState<ViewMode>('text');
    
    const bytes = base64ToBytes(packet_data);
    const formattedTime = new Date(timestamp).toLocaleTimeString();

    const getContent = (): string => {
        switch (viewMode) {
            case 'text':
                return toUtf8Preview(bytes);
            case 'hex':
                return toHexDump(bytes);
            case 'python':
                return toPythonBytesLiteral(bytes);
        }
    };

    const copy = async () => {
        try {
            await navigator.clipboard.writeText(getContent());
        } catch (e) {
            console.error('Failed to copy:', e);
        }
    };
    
    return (
        <div className={`packet-card ${direction === 'orig' ? 'packet-orig' : 'packet-dest'}`}>
            <div className="packet-header">
                <div className="view-mode-switcher">
                    <button 
                        className={`view-mode-btn ${viewMode === 'text' ? 'active' : ''}`}
                        onClick={() => setViewMode('text')}
                        title="Text view"
                    >
                        TXT
                    </button>
                    <button 
                        className={`view-mode-btn ${viewMode === 'hex' ? 'active' : ''}`}
                        onClick={() => setViewMode('hex')}
                        title="Hex dump"
                    >
                        HEX
                    </button>
                    <button 
                        className={`view-mode-btn ${viewMode === 'python' ? 'active' : ''}`}
                        onClick={() => setViewMode('python')}
                        title="Python bytes literal"
                    >
                        PY
                    </button>
                </div>
                <span className="packet-time">{formattedTime}</span>
                <span className={`packet-direction ${direction}`}>
                    {direction === 'orig' ? '← Исходящий' : '→ Входящий'}
                </span>
            </div>

            <div className="packet-content">
                <pre className={`packet-body ${viewMode !== 'text' ? 'monospace' : ''}`}>
                    {getContent()}
                </pre>
                <button className="copy-btn" onClick={copy} title="Copy to clipboard">
                    Copy
                </button>
            </div>
        </div>
    );
} 