import { useState, useEffect } from 'react';
import './PacketCard.css';

type ViewMode = 'text' | 'hex' | 'python' | 'http';

interface PacketProps {
    direction: string;
    id: number;
    packet_data: string;
    stream_id: number;
    timestamp: number;
    isHttp?: boolean;
    onHeaderSearch?: (name: string, value: string) => void;
    _merged_count?: number;
}

interface HttpHeader {
    name: string;
    value: string;
}

interface HttpMessageData {
    id: number;
    is_request: boolean;
    method: string | null;
    path: string | null;
    status_code: number | null;
    http_version: string | null;
    headers: HttpHeader[];
}

function copyText(text: string) {
    if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(text);
    } else {
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
    }
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

function generateCurl(msg: HttpMessageData): string {
    const parts = [`curl -X ${msg.method} '<HOST>${msg.path}'`];
    for (const h of msg.headers) {
        parts.push(`  -H '${h.name}: ${h.value}'`);
    }
    return parts.join(' \\\n');
}

export function PacketCard({ direction, id, packet_data, stream_id, timestamp, isHttp, onHeaderSearch, _merged_count }: PacketProps) {
    const [viewMode, setViewMode] = useState<ViewMode>(isHttp ? 'http' : 'text');
    const [httpData, setHttpData] = useState<HttpMessageData | null>(null);
    const [httpLoading, setHttpLoading] = useState(false);

    const bytes = base64ToBytes(packet_data);
    const formattedTime = new Date(timestamp).toLocaleTimeString();

    // Load HTTP data when switching to http mode or on mount if isHttp
    useEffect(() => {
        if (isHttp && !httpData && !httpLoading) {
            setHttpLoading(true);
            // Find http_message for this packet
            fetch(`/api/http/stream/${stream_id}`)
                .then(r => r.json())
                .then((messages: any[]) => {
                    const msg = messages.find((m: any) => m.packet_id === id);
                    if (msg) {
                        return fetch(`/api/http/headers/${msg.id}`)
                            .then(r => r.json())
                            .then(headersObj => {
                                // Convert JSONB {"Name": "value"} to array
                                const headersArr: HttpHeader[] = headersObj && typeof headersObj === 'object' && !Array.isArray(headersObj)
                                    ? Object.entries(headersObj).map(([name, value]) => ({ name, value: String(value) }))
                                    : Array.isArray(headersObj)
                                        ? headersObj.map((h: any) => ({ name: h.name, value: h.value }))
                                        : [];
                                setHttpData({
                                    id: msg.id,
                                    is_request: msg.is_request,
                                    method: msg.method,
                                    path: msg.path,
                                    status_code: msg.status_code,
                                    http_version: msg.http_version,
                                    headers: headersArr,
                                });
                            });
                    }
                })
                .catch(() => {})
                .finally(() => setHttpLoading(false));
        }
    }, [isHttp, id, stream_id]);

    const getContent = (): string => {
        switch (viewMode) {
            case 'text':
                return toUtf8Preview(bytes);
            case 'hex':
                return toHexDump(bytes);
            case 'python':
                return toPythonBytesLiteral(bytes);
            case 'http':
                return ''; // Rendered as JSX below
        }
    };

    const copy = () => {
        let text: string;
        if (viewMode === 'http' && httpData) {
            text = generateCurl(httpData);
        } else {
            text = getContent();
        }
        copyText(text);
    };

    const renderHttpView = () => {
        if (httpLoading) return <div className="http-loading">Loading...</div>;
        if (!httpData) return <pre className="packet-body">{toUtf8Preview(bytes)}</pre>;

        return (
            <div className="http-parsed">
                {/* Status line */}
                <div className="http-status-line">
                    {httpData.is_request ? (
                        <><span className="http-method">{httpData.method}</span> <span className="http-path">{httpData.path}</span> <span className="http-ver">HTTP/{httpData.http_version}</span></>
                    ) : (
                        <><span className="http-ver">HTTP/{httpData.http_version}</span> <span className={`http-code http-code-${Math.floor((httpData.status_code || 0) / 100)}`}>{httpData.status_code}</span></>
                    )}
                </div>

                {/* Headers */}
                <div className="http-headers">
                    {httpData.headers.map((h, idx) => (
                        <div key={idx} className="http-header-line">
                            <span className="h-name">{h.name}:</span>
                            <span className="h-value">{h.value}</span>
                            {onHeaderSearch && (
                                <button
                                    className="header-search-btn"
                                    title={`Найти: ${h.name}: ${h.value}`}
                                    onClick={() => onHeaderSearch(h.name, h.value)}
                                >
                                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                        <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
                                    </svg>
                                </button>
                            )}
                        </div>
                    ))}
                </div>

                {/* Body separator + raw body */}
                {bytes.length > 0 && (
                    <pre className="http-body">{toUtf8Preview(bytes).split('\r\n\r\n').slice(1).join('\r\n\r\n') || ''}</pre>
                )}

                {/* CURL button for requests */}
                {httpData.is_request && (
                    <button className="curl-copy-btn" onClick={() => copyText(generateCurl(httpData))}>
                        CURL
                    </button>
                )}
            </div>
        );
    };

    return (
        <div className={`packet-card ${direction === 'orig' ? 'packet-orig' : 'packet-dest'}`}>
            <div className="packet-header">
                <div className="view-mode-switcher">
                    <button
                        className={`view-mode-btn ${viewMode === 'text' ? 'active' : ''}`}
                        onClick={() => setViewMode('text')}
                    >TXT</button>
                    <button
                        className={`view-mode-btn ${viewMode === 'hex' ? 'active' : ''}`}
                        onClick={() => setViewMode('hex')}
                    >HEX</button>
                    <button
                        className={`view-mode-btn ${viewMode === 'python' ? 'active' : ''}`}
                        onClick={() => setViewMode('python')}
                    >PY</button>
                    {isHttp && (
                        <button
                            className={`view-mode-btn ${viewMode === 'http' ? 'active' : ''}`}
                            onClick={() => setViewMode('http')}
                        >HTTP</button>
                    )}
                </div>
                <span className="packet-time">{formattedTime}</span>
                <span className={`packet-direction ${direction}`}>
                    {direction === 'orig' ? '← Исходящий' : '→ Входящий'}
                </span>
                {_merged_count && _merged_count > 1 && (
                    <span className="merged-badge">{_merged_count} пакетов</span>
                )}
            </div>

            <div className="packet-content">
                {viewMode === 'http' ? renderHttpView() : (
                    <pre className={`packet-body ${viewMode !== 'text' ? 'monospace' : ''}`}>
                        {getContent()}
                    </pre>
                )}
                {viewMode !== 'http' && (
                    <button className="copy-btn" onClick={copy} title="Copy to clipboard">Copy</button>
                )}
            </div>
        </div>
    );
}
