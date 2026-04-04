import { useState, useEffect } from 'react';
import './PacketCard.css';

type ViewMode = 'text' | 'hex' | 'python' | 'http' | 'grpc';

interface PacketProps {
    direction: string;
    id: number;
    packet_data: string;
    stream_id: number;
    timestamp: number;
    isHttp?: boolean;
    isGrpc?: boolean;
    onHeaderSearch?: (name: string, value: string) => void;
    onHeaderExclude?: (name: string, value: string) => void;
    onQuickBlock?: (pattern: string) => void;
    onDiffSelect?: (packet: { id: number; packet_data: string; direction: string }) => void;
    diffSelected?: boolean;
    _merged_count?: number;
    _stream_id?: number;
    showStreamId?: boolean;
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

function InteractiveHexDump({ bytes, onQuickBlock }: { bytes: Uint8Array; onQuickBlock?: (pattern: string) => void }) {
    const [hoverIdx, setHoverIdx] = useState<number | null>(null);
    const [selStart, setSelStart] = useState<number | null>(null);
    const [selEnd, setSelEnd] = useState<number | null>(null);
    const [selecting, setSelecting] = useState(false);
    const [ctxMenu, setCtxMenu] = useState<{ x: number; y: number } | null>(null);

    const selMin = selStart !== null && selEnd !== null ? Math.min(selStart, selEnd) : null;
    const selMax = selStart !== null && selEnd !== null ? Math.max(selStart, selEnd) : null;
    const isSelected = (idx: number) => selMin !== null && selMax !== null && idx >= selMin && idx <= selMax;

    const getSelectedBytes = (): Uint8Array => {
        if (selMin === null || selMax === null) return new Uint8Array(0);
        return bytes.slice(selMin, selMax + 1);
    };

    const copyAs = (fmt: 'hex' | 'ascii' | 'python' | 'carray') => {
        const sel = getSelectedBytes();
        let text = '';
        if (fmt === 'hex') text = Array.from(sel).map(b => b.toString(16).padStart(2, '0')).join(' ');
        else if (fmt === 'ascii') text = Array.from(sel).map(b => b >= 0x20 && b <= 0x7e ? String.fromCharCode(b) : '.').join('');
        else if (fmt === 'python') text = `b'${Array.from(sel).map(b => `\\x${b.toString(16).padStart(2, '0')}`).join('')}'`;
        else if (fmt === 'carray') text = `{${Array.from(sel).map(b => `0x${b.toString(16).padStart(2, '0')}`).join(', ')}}`;
        copyText(text);
        setCtxMenu(null);
    };

    const handleMouseDown = (idx: number, e: React.MouseEvent) => {
        // Ignore right-click — don't reset selection
        if (e.button === 2) return;
        setSelStart(idx);
        setSelEnd(idx);
        setSelecting(true);
        setCtxMenu(null);
    };

    const handleMouseEnter = (idx: number) => {
        setHoverIdx(idx);
        if (selecting) setSelEnd(idx);
    };

    const handleContextMenu = (e: React.MouseEvent) => {
        if (selMin !== null && selMax !== null && selMax >= selMin) {
            e.preventDefault();
            e.stopPropagation();
            setCtxMenu({ x: e.clientX, y: e.clientY });
        }
    };

    const rows: JSX.Element[] = [];
    for (let row = 0; row < bytes.length; row += 16) {
        const offset = row.toString(16).padStart(4, '0');
        const hexCells: JSX.Element[] = [];
        const asciiCells: JSX.Element[] = [];

        for (let col = 0; col < 16; col++) {
            const idx = row + col;
            if (idx < bytes.length) {
                const b = bytes[idx];
                const hover = hoverIdx === idx;
                const sel = isSelected(idx);
                const ch = b >= 0x20 && b <= 0x7e ? String.fromCharCode(b) : '.';

                hexCells.push(
                    <span key={idx}
                        className={`hx-byte ${hover ? 'hx-hover' : ''} ${sel ? 'hx-selected' : ''}`}
                        onMouseDown={(e) => { e.preventDefault(); handleMouseDown(idx, e); }}
                        onMouseEnter={() => handleMouseEnter(idx)}
                        onContextMenu={handleContextMenu}
                    >{b.toString(16).padStart(2, '0')}</span>
                );
                asciiCells.push(
                    <span key={idx}
                        className={`hx-ascii-char ${hover ? 'hx-hover' : ''} ${sel ? 'hx-selected' : ''} ${b < 0x20 || b > 0x7e ? 'hx-nonprint' : ''}`}
                        onMouseDown={(e) => { e.preventDefault(); handleMouseDown(idx, e); }}
                        onMouseEnter={() => handleMouseEnter(idx)}
                        onContextMenu={handleContextMenu}
                    >{ch}</span>
                );
            } else {
                hexCells.push(<span key={idx} className="hx-pad">  </span>);
            }
            if (col < 15 && col === 7) {
                hexCells.push(<span key={`s${idx}`} className="hx-gap"> </span>);
            }
        }

        rows.push(
            <div key={row} className="hx-row">
                <span className="hx-offset">{offset}</span>
                <span className="hx-hex">{hexCells}</span>
                <span className="hx-ascii">{asciiCells}</span>
            </div>
        );
    }

    return (
        <div className="interactive-hexdump"
            onMouseUp={() => setSelecting(false)}
            onMouseLeave={() => { setHoverIdx(null); setSelecting(false); }}
            onContextMenu={handleContextMenu}
        >
            {rows}
            {ctxMenu && (
                <>
                    <div className="hx-ctx-overlay" onClick={() => setCtxMenu(null)} />
                    <div className="hx-ctx-menu" style={{ top: ctxMenu.y, left: ctxMenu.x }}>
                        <div className="hx-ctx-header">{(selMax! - selMin! + 1)} bytes selected</div>
                        <button onClick={() => copyAs('hex')}>Copy as Hex</button>
                        <button onClick={() => copyAs('ascii')}>Copy as ASCII</button>
                        <button onClick={() => copyAs('python')}>Copy as Python</button>
                        <button onClick={() => copyAs('carray')}>Copy as C array</button>
                        {onQuickBlock && (
                            <button className="hx-ctx-block" onClick={() => {
                                const sel = getSelectedBytes();
                                const hex = Array.from(sel).map(b => b.toString(16).padStart(2, '0')).join('');
                                onQuickBlock(hex);
                                setCtxMenu(null);
                            }}>Block pattern</button>
                        )}
                    </div>
                </>
            )}
        </div>
    );
}

const toPythonBytesLiteral = (bytes: Uint8Array): string => {
    const body = Array.from(bytes)
        .map((b) => `\\x${b.toString(16).padStart(2, '0')}`)
        .join('');
    return `b'${body}'`;
};

function renderHighlightedText(text: string, triggers: TagTrigger[]): (string | JSX.Element)[] {
    if (triggers.length === 0) return [text];

    // Sort by start_index
    const sorted = [...triggers].sort((a, b) => a.start_index - b.start_index);
    const parts: (string | JSX.Element)[] = [];
    let lastEnd = 0;

    for (const t of sorted) {
        if (t.start_index > lastEnd) {
            parts.push(text.slice(lastEnd, t.start_index));
        }
        if (t.start_index < text.length) {
            const end = Math.min(t.end_index, text.length);
            const hue = t.color || 330;
            parts.push(
                <span
                    key={`${t.start_index}-${t.end_index}`}
                    className="highlight-match"
                    style={{ backgroundColor: `hsla(${hue}, 100%, 50%, 0.25)`, borderBottom: `2px solid hsl(${hue}, 100%, 50%)` }}
                    title={t.name}
                >
                    {text.slice(t.start_index, end)}
                </span>
            );
            lastEnd = end;
        }
    }
    if (lastEnd < text.length) {
        parts.push(text.slice(lastEnd));
    }
    return parts;
}

function generateCurl(msg: HttpMessageData): string {
    const parts = [`curl -X ${msg.method} '<HOST>${msg.path}'`];
    for (const h of msg.headers) {
        parts.push(`  -H '${h.name}: ${h.value}'`);
    }
    return parts.join(' \\\n');
}

interface TagTrigger {
    start_index: number;
    end_index: number;
    name: string;
    color: number;
}

export function PacketCard({ direction, id, packet_data, stream_id, timestamp, isHttp, isGrpc, onHeaderSearch, onHeaderExclude, onQuickBlock, onDiffSelect, diffSelected, _merged_count, _stream_id, showStreamId }: PacketProps) {
    const [viewMode, setViewMode] = useState<ViewMode>(isGrpc ? 'grpc' : isHttp ? 'http' : 'text');
    const [grpcData, setGrpcData] = useState<any[] | null>(null);
    const [grpcLoading, setGrpcLoading] = useState(false);
    const [httpData, setHttpData] = useState<HttpMessageData | null>(null);
    const [httpLoading, setHttpLoading] = useState(false);
    const [triggers, setTriggers] = useState<TagTrigger[]>([]);

    const bytes = base64ToBytes(packet_data);
    const formattedTime = new Date(timestamp).toLocaleTimeString();

    // Load triggers for highlight
    useEffect(() => {
        fetch(`/api/tag/stream_id/${stream_id}`)
            .then(r => r.json())
            .then((tags: any[]) => {
                const mine = tags
                    .filter((t: any) => t.packet_id === id)
                    .map((t: any) => ({
                        start_index: t.start_index,
                        end_index: t.end_index,
                        name: t.name || '',
                        color: t.color || 0,
                    }));
                setTriggers(mine);
            })
            .catch(() => {});
    }, [id, stream_id]);

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

    // Load gRPC data
    useEffect(() => {
        if (isGrpc && !grpcData && !grpcLoading) {
            setGrpcLoading(true);
            fetch(`/api/grpc/stream/${stream_id}`)
                .then(r => r.json())
                .then((msgs: any[]) => setGrpcData(msgs || []))
                .catch(() => setGrpcData([]))
                .finally(() => setGrpcLoading(false));
        }
    }, [isGrpc, stream_id]);

    const getContent = (): string => {
        switch (viewMode) {
            case 'text':
                return toUtf8Preview(bytes);
            case 'hex':
                return toHexDump(bytes);
            case 'python':
                return toPythonBytesLiteral(bytes);
            case 'http':
            case 'grpc':
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

    const renderGrpcView = () => {
        if (grpcLoading) return <div className="http-loading">Loading...</div>;
        if (!grpcData || grpcData.length === 0) return <pre className="packet-body">{toUtf8Preview(bytes)}</pre>;

        // Match gRPC message to this specific packet by packet_id
        const msg = grpcData.find((m: any) => m.packet_id === id)
            // Fallback: match by direction (for data without packet_id)
            || grpcData.find((m: any) => m.is_request === (direction === 'dest'));
        if (!msg) return <pre className="packet-body">{toUtf8Preview(bytes)}</pre>;

        let decoded: Record<string, any> = {};
        try {
            decoded = JSON.parse(msg.decoded_json || '{}');
        } catch {}

        return (
            <div className="http-parsed">
                <div className="http-status-line">
                    <span className="http-method">{msg.is_request ? 'REQUEST' : 'RESPONSE'}</span>
                    {msg.service_name && <span className="http-path"> {msg.service_name}/{msg.method_name}</span>}
                </div>
                <div className="http-headers">
                    {Object.entries(decoded).map(([key, val]) => (
                        <div key={key} className="http-header-line">
                            <span className="h-name">{key}:</span>
                            <span className="h-value">{typeof val === 'string' ? val : JSON.stringify(val)}</span>
                        </div>
                    ))}
                </div>
                {Object.keys(decoded).length === 0 && (
                    <div className="text-muted" style={{fontSize: '12px', padding: '8px 0'}}>
                        No decoded fields (raw protobuf)
                    </div>
                )}
            </div>
        );
    };

    const renderHttpView = () => {
        if (httpLoading) return <div className="http-loading">Loading...</div>;
        if (!httpData) return <pre className="packet-body">{toUtf8Preview(bytes)}</pre>;

        return (
            <div className="http-parsed">
                {/* Status line */}
                <div className="http-status-line">
                    {httpData.is_request ? (
                        <>
                            <span className="http-method">{httpData.method}</span>
                            {' '}
                            <span className="http-path">{httpData.path}</span>
                            {onHeaderSearch && httpData.path && (
                                <button className="header-search-btn path-search" title={`Все запросы на ${httpData.path}`}
                                    onClick={() => onHeaderSearch('_path', httpData.path!)}>
                                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                        <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
                                    </svg>
                                </button>
                            )}
                            {onHeaderExclude && httpData.path && (
                                <button className="header-search-btn header-exclude-btn path-search" title={`Скрыть запросы на ${httpData.path}`}
                                    onClick={() => onHeaderExclude('_path', httpData.path!)}>
                                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                        <path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94"/>
                                        <path d="M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19"/>
                                        <line x1="1" y1="1" x2="23" y2="23"/>
                                    </svg>
                                </button>
                            )}
                            {' '}
                            <span className="http-ver">HTTP/{httpData.http_version}</span>
                        </>
                    ) : (
                        <><span className="http-ver">HTTP/{httpData.http_version}</span> <span className={`http-code http-code-${Math.floor((httpData.status_code || 0) / 100)}`}>{httpData.status_code}</span></>
                    )}
                </div>

                {/* Headers */}
                <div className="http-headers">
                    {httpData.headers.flatMap((h, idx) => {
                        // Parse Cookie header into individual cookies
                        if (h.name.toLowerCase() === 'cookie' && h.value.includes('=')) {
                            const cookies = h.value.split(';').map(c => c.trim()).filter(c => c.includes('='));
                            return [
                                <div key={`cookie-label-${idx}`} className="http-header-line">
                                    <span className="h-name">Cookie:</span>
                                    <span className="h-value" style={{ color: '#666', fontSize: '11px' }}>{cookies.length} cookies</span>
                                </div>,
                                ...cookies.map((cookie, ci) => {
                                    const [cName, ...cRest] = cookie.split('=');
                                    const cValue = cRest.join('=');
                                    return (
                                        <div key={`cookie-${idx}-${ci}`} className="http-header-line cookie-line">
                                            <span className="h-name cookie-name">{cName.trim()}=</span>
                                            <span className="h-value">{cValue}</span>
                                            {onHeaderSearch && (
                                                <button className="header-search-btn" title={`Найти: cookie ${cName.trim()}`}
                                                    onClick={() => onHeaderSearch('Cookie', `${cName.trim()}=${cValue}`)}>
                                                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                                        <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
                                                    </svg>
                                                </button>
                                            )}
                                            {onHeaderExclude && (
                                                <button className="header-search-btn header-exclude-btn" title={`Скрыть: cookie ${cName.trim()}`}
                                                    onClick={() => onHeaderExclude('Cookie', `${cName.trim()}=${cValue}`)}>
                                                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                                        <path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94"/>
                                                        <path d="M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19"/>
                                                        <line x1="1" y1="1" x2="23" y2="23"/>
                                                    </svg>
                                                </button>
                                            )}
                                        </div>
                                    );
                                })
                            ];
                        }
                        return [(
                            <div key={idx} className="http-header-line">
                                <span className="h-name">{h.name}:</span>
                                <span className="h-value">{h.value}</span>
                                {onHeaderSearch && (
                                    <button className="header-search-btn" title={`Найти: ${h.name}: ${h.value}`}
                                        onClick={() => onHeaderSearch(h.name, h.value)}>
                                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                            <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
                                        </svg>
                                    </button>
                                )}
                                {onHeaderExclude && (
                                    <button className="header-search-btn header-exclude-btn" title={`Скрыть: ${h.name}: ${h.value}`}
                                        onClick={() => onHeaderExclude(h.name, h.value)}>
                                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                                            <path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94"/>
                                            <path d="M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19"/>
                                            <line x1="1" y1="1" x2="23" y2="23"/>
                                        </svg>
                                    </button>
                                )}
                            </div>
                        )];
                    })}
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
                    {isGrpc && (
                        <button
                            className={`view-mode-btn ${viewMode === 'grpc' ? 'active' : ''}`}
                            onClick={() => setViewMode('grpc')}
                        >GRPC</button>
                    )}
                </div>
                <span className="packet-time">{formattedTime}</span>
                {showStreamId && _stream_id && (
                    <span style={{ fontSize: '10px', color: 'var(--re-text-dim)', fontFamily: 'var(--re-font-mono, monospace)' }}>#{_stream_id}</span>
                )}
                <span className={`packet-direction ${direction}`}>
                    {direction === 'orig' ? '← Исходящий' : '→ Входящий'}
                </span>
                {_merged_count && _merged_count > 1 && (
                    <span className="merged-badge">{_merged_count} пакетов</span>
                )}
            </div>

            <div className="packet-content">
                {viewMode === 'grpc' ? renderGrpcView() : viewMode === 'http' ? renderHttpView() : viewMode === 'hex' ? (
                    <div className="packet-body monospace">
                        <InteractiveHexDump bytes={bytes} onQuickBlock={onQuickBlock} />
                    </div>
                ) : (
                    <pre className={`packet-body ${viewMode === 'python' ? 'monospace' : ''}`}>
                        {viewMode === 'text' && triggers.length > 0
                            ? renderHighlightedText(toUtf8Preview(bytes), triggers)
                            : getContent()
                        }
                    </pre>
                )}
                {viewMode !== 'http' && (
                    <div className="packet-btn-group">
                        <button className="copy-btn" onClick={copy} title="Copy to clipboard">Copy</button>
                        {onDiffSelect && (
                            <button className={`diff-select-btn ${diffSelected ? 'selected' : ''}`}
                                onClick={() => onDiffSelect({ id, packet_data, direction })}
                                title={diffSelected ? 'Выбран для сравнения' : 'Сравнить'}>
                                Diff
                            </button>
                        )}
                        {onQuickBlock && (
                            <button className="block-btn" onMouseDown={e => e.preventDefault()} onClick={() => {
                                const sel = window.getSelection()?.toString();
                                if (sel && sel.length > 0) {
                                    onQuickBlock(sel);
                                } else {
                                    alert('Выделите текст для блокировки');
                                }
                            }} title="Заблокировать выделенный текст">Block</button>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
}
