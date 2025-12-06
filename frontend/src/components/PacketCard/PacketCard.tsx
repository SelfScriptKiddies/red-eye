import React from 'react';
import './PacketCard.css';

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
    const bytes = base64ToBytes(packet_data);
    const formattedTime = new Date(timestamp).toLocaleTimeString();
    const textPreview = toUtf8Preview(bytes);
    const hexDump = toHexDump(bytes);
    const pythonBytes = toPythonBytesLiteral(bytes);

    const copy = async (value: string) => {
        try {
            await navigator.clipboard.writeText(value);
        } catch (e) {
            console.error('Не удалось скопировать:', e);
        }
    };
    
    return (
        <div className={`packet-card ${direction === 'orig' ? 'packet-orig' : 'packet-dest'}`}>
            <div className="packet-header">
                <span className="packet-time">{formattedTime}</span>
                <span className={`packet-direction ${direction}`}>
                    {direction === 'orig' ? '← Исходящий' : '→ Входящий'}
                </span>
            </div>

            <div className="packet-section">
                <div className="section-title">Text</div>
                <pre className="packet-body">{textPreview}</pre>
            </div>

            <div className="packet-section">
                <div className="section-title row-space">
                    <span>Hex dump</span>
                    <button className="copy-btn" onClick={() => copy(hexDump)}>Copy</button>
                </div>
                <pre className="packet-body monospace">{hexDump}</pre>
            </div>

            <div className="packet-section">
                <div className="section-title row-space">
                    <span>Python bytes</span>
                    <button className="copy-btn" onClick={() => copy(pythonBytes)}>Copy</button>
                </div>
                <pre className="packet-body monospace">{pythonBytes}</pre>
            </div>
        </div>
    );
} 