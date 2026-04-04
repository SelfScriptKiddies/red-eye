import { ReactElement } from "react";
import './PacketsPage.css'
import { useState, useEffect, useRef } from 'react';
import { StreamCard } from '../../components/StreamCard'
import { ServiceCard } from '../../components/ServiceCard'
import { PacketCard } from '../../components/PacketCard';
import { DiffView } from '../../components/DiffView';
import { TimelineBar } from '../../components/TimelineBar/TimelineBar';
import { fetchApi, copyText } from '../../lib/api';
import { useStreams } from '../../lib/useStreams';
import { usePackets } from '../../lib/usePackets';
import { useRealtimeUpdates } from '../../lib/useRealtimeUpdates';

interface PacketEvent {
    packet_id: number;
    stream_id: number;
    timestamp_seconds: number;
    timestamp_microseconds: number;
    direction: string;
}

export function PacketsPage() {
    const [servicesArray, setServices] = useState([]);
    const [tagsArray, setTags] = useState([]);
    const [tagColors, setTagColors] = useState({});
    const [servicesLoaded, setServicesLoaded] = useState(false);
    const [tagsLoaded, setTagsLoaded] = useState(false);
    const [usedTags, setUsedTags] = useState([]);
    const [usedServices, setUsedServices] = useState([]);
    const [startTime, setStartTime] = useState<number | null>(null);
    const [endTime, setEndTime] = useState<number | null>(null);
    const [headerFilters, setHeaderFilters] = useState<{name: string, value: string}[]>([]);
    const [headerSearchResults, setHeaderSearchResults] = useState<{stream_ids: number[], total: number} | null>(null);
    const [headerSearching, setHeaderSearching] = useState(false);
    const [diffLeft, setDiffLeft] = useState<any | null>(null);
    const [diffRight, setDiffRight] = useState<any | null>(null);
    const [diffOpen, setDiffOpen] = useState(false);
    const [shareCopied, setShareCopied] = useState(false);
    const [exportMenuOpen, setExportMenuOpen] = useState(false);

    // Получаем состояние RedEye из localStorage
    const isRedEyeActive = localStorage.getItem('redEyeActive') === 'true';

    const {
        streams: streamsArray,
        isLoadingMore,
        hasMoreStreams,
        scrollRef: streamsContainerRef,
        refresh,
        updateStream,
        clearStreams,
        loadStreams,
    } = useStreams({ tags: usedTags, services: usedServices, startTime, endTime, enabled: !isRedEyeActive });

    const {
        packetData,
        selectedStreamId,
        selectedStreamL7,
        mergeOverride,
        shouldMerge,
        mergedPackets,
        setMergeOverride,
        handleStreamClick,
    } = usePackets(streamsArray, servicesArray);

    useRealtimeUpdates(refresh, !isRedEyeActive);

    const getServices = async () => {
        if (servicesLoaded) return;
        setServicesLoaded(true);
        const data = await fetchApi("/api/service");
        if (data) setServices(Object.values(data));
    };
    const getTags = async () => {
        if (tagsLoaded) return;
        setTagsLoaded(true);
        const data = await fetchApi("/api/tag/names");
        if (data) {
            const tagsData = Object.values(data) as any[];
            setTags(tagsData as any);
            const colorMap = {};
            tagsData.forEach((tag: any) => {
                colorMap[tag.name] = tag.color;
            });
            setTagColors(colorMap);
        }
    };

    // Initial service/tag load + handle ?stream=ID from URL
    const pageBootRef = useRef(false);
    useEffect(() => {
        if (isRedEyeActive) return;
        if (!servicesLoaded) getServices();
        if (!tagsLoaded) getTags();

        if (!pageBootRef.current) {
            pageBootRef.current = true;
            const params = new URLSearchParams(window.location.search);
            const streamParam = params.get('stream');
            if (streamParam) {
                setTimeout(() => handleStreamClick(parseInt(streamParam)), 500);
            }
        }
    }, [isRedEyeActive, servicesLoaded, tagsLoaded]);

    // When header search is active, load matching streams individually
    const [headerFilteredStreams, setHeaderFilteredStreams] = useState<any[]>([]);

    useEffect(() => {
        if (!headerSearchResults || headerSearchResults.stream_ids.length === 0) {
            setHeaderFilteredStreams([]);
            return;
        }
        const ids = new Set(headerSearchResults.stream_ids);
        const found = streamsArray.filter((s: any) => ids.has(s.id));
        if (found.length > 0) {
            setHeaderFilteredStreams(found);
        } else {
            fetchApi(`/api/stream?limit=200&offset=0`)
                .then(data => {
                    if (data) {
                        const all = Object.values(data) as any[];
                        const matched = all.filter((s: any) => ids.has(s.id));
                        setHeaderFilteredStreams(matched);
                    }
                })
                .catch(() => setHeaderFilteredStreams([]));
        }
    }, [headerSearchResults]);

    const filteredStreams = headerSearchResults
        ? headerFilteredStreams
        : streamsArray;

    const addHeaderFilter = (name: string, value: string) => {
        const exists = headerFilters.some(f => f.name === name && f.value === value);
        if (!exists) {
            const updated = [...headerFilters, { name, value }];
            setHeaderFilters(updated);
            runHeaderSearch(updated);
        }
    };

    const removeHeaderFilter = (index: number) => {
        const updated = headerFilters.filter((_, i) => i !== index);
        setHeaderFilters(updated);
        if (updated.length > 0) {
            runHeaderSearch(updated);
        } else {
            setHeaderSearchResults(null);
        }
    };

    const runHeaderSearch = async (filters: {name: string, value: string}[]) => {
        setHeaderSearching(true);
        try {
            const data = await fetch('/api/http/search/multi', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(filters.map(f => ({ name: f.name, value: f.value }))),
            }).then(r => r.json());
            setHeaderSearchResults(data);
        } catch {
            setHeaderSearchResults(null);
        }
        setHeaderSearching(false);
    };

    const handleStreamNavigate = (streamId: number) => {
        handleStreamClick(streamId);
        const streamEl = document.querySelector(`[data-stream-id="${streamId}"]`);
        if (streamEl) streamEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
    };

    const handleFavoriteToggle = async (streamId, currentFavorite) => {
        try {
            const response = await fetch(`/api/stream/${streamId}/favorite`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ favorite: !currentFavorite })
            });
            if (!response.ok) {
                console.error('Не удалось обновить избранное:', await response.text());
                return;
            }
            updateStream(streamId, (s: any) => ({ ...s, favorite: !currentFavorite }));
        } catch (error) {
            console.error('Ошибка при обновлении избранного:', error);
        }
    };

    // Share: copy link + auto-favorite
    const shareStream = async () => {
        if (!selectedStreamId) return;
        const url = `${window.location.origin}/packets?stream=${selectedStreamId}`;
        copyText(url);
        const numId = typeof selectedStreamId === 'string' ? parseInt(selectedStreamId as string) : selectedStreamId;
        const stream = streamsArray.find((s: any) => s.id === numId);
        if (stream && !stream.favorite) {
            handleFavoriteToggle(numId, false);
        }
        setShareCopied(true);
        setTimeout(() => setShareCopied(false), 2000);
    };

    // Export generators
    const exportPwntools = (): string => {
        if (!packetData || packetData.length === 0) return '';
        const stream = streamsArray.find((s: any) => s.id === (typeof selectedStreamId === 'string' ? parseInt(selectedStreamId as string) : selectedStreamId));
        const host = stream ? `TARGET_IP` : 'TARGET_IP';
        const port = stream?.service_name ? '????' : '????';
        const lines = [`from pwn import *\n`, `r = remote('${host}', ${port})\n`];
        for (const pkt of packetData) {
            const raw = atob(pkt.packet_data);
            const pyBytes = Array.from(raw).map((c: string) => {
                const code = c.charCodeAt(0);
                if (code >= 0x20 && code <= 0x7e && c !== '\\' && c !== "'") return c;
                return `\\x${code.toString(16).padStart(2, '0')}`;
            }).join('');
            if (pkt.direction === 'dest') {
                lines.push(`r.send(b'${pyBytes}')`);
            } else {
                lines.push(`r.recv(${raw.length})  # ${raw.length} bytes`);
            }
        }
        lines.push(`\nr.interactive()`);
        return lines.join('\n');
    };

    const exportRequests = (): string => {
        if (!packetData || packetData.length === 0) return '';
        const lines = [`import requests\n`, `s = requests.Session()`, `BASE = 'http://TARGET_IP:PORT'\n`];
        for (const pkt of mergedPackets) {
            if (pkt.direction !== 'dest') continue;
            const text = atob(pkt.packet_data);
            const methodMatch = text.match(/^(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+(\S+)\s+HTTP/);
            if (!methodMatch) continue;
            const [, method, path] = methodMatch;
            const headerSection = text.split('\r\n\r\n')[0];
            const headerLines = headerSection.split('\r\n').slice(1);
            const headers: Record<string, string> = {};
            for (const line of headerLines) {
                const idx = line.indexOf(':');
                if (idx > 0) headers[line.slice(0, idx).trim()] = line.slice(idx + 1).trim();
            }
            const body = text.split('\r\n\r\n').slice(1).join('\r\n\r\n');
            const headersStr = Object.entries(headers)
                .filter(([k]) => !['Host', 'Content-Length', 'Connection'].includes(k))
                .map(([k, v]) => `        '${k}': '${v}'`).join(',\n');

            let call = `resp = s.${method.toLowerCase()}(f'{BASE}${path}'`;
            if (headersStr) call += `,\n    headers={\n${headersStr}\n    }`;
            if (body && method !== 'GET') call += `,\n    data=${JSON.stringify(body)}`;
            call += ')';
            lines.push(call);
            lines.push(`print(resp.status_code, resp.text[:200])\n`);
        }
        return lines.join('\n');
    };

    const exportCurl = (): string => {
        if (!packetData || packetData.length === 0) return '';
        const lines = ['#!/bin/bash\n'];
        for (const pkt of mergedPackets) {
            if (pkt.direction !== 'dest') continue;
            const text = atob(pkt.packet_data);
            const methodMatch = text.match(/^(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+(\S+)\s+HTTP/);
            if (!methodMatch) continue;
            const [, method, path] = methodMatch;
            const headerSection = text.split('\r\n\r\n')[0];
            const headerLines = headerSection.split('\r\n').slice(1);
            const body = text.split('\r\n\r\n').slice(1).join('\r\n\r\n');

            const parts = [`curl -s -X ${method} 'http://TARGET_IP:PORT${path}'`];
            for (const line of headerLines) {
                const idx = line.indexOf(':');
                if (idx > 0) {
                    const name = line.slice(0, idx).trim();
                    if (!['Host', 'Content-Length', 'Connection'].includes(name)) {
                        parts.push(`  -H '${line.trim()}'`);
                    }
                }
            }
            if (body && method !== 'GET') {
                parts.push(`  -d '${body.replace(/'/g, "'\\''")}'`);
            }
            lines.push(parts.join(' \\\n'));
            lines.push('echo\n');
        }
        return lines.join('\n');
    };

    const handleTagClick = (e) => {
        const tagName = e.target.dataset.service;
        setUsedTags(prev => {
            if (prev.includes(tagName)) return prev.filter(t => t !== tagName);
            return [...prev, tagName];
        });
    };
    const handleServiceClick = (e) => {
        const serviceName = e.target.dataset.service;
        setUsedServices(prev => {
            if (prev.includes(serviceName)) return prev.filter(s => s !== serviceName);
            return [...prev, serviceName];
        });
    };

    return (
        <div className="packetsPage">
            <div className="filterTape">
                <button className="clear-button" onClick={clearStreams}>
                    Очистить очередь
                </button>
                <div className="tag-filter d-inline-block">
                    {tagsArray.map((service, index) => {
                        return < ServiceCard service={service} onTagClick={handleTagClick}/>
                    })}
                    </div>
                <div className="service-filter d-inline-block">
                    {servicesArray.map((service, index) => {
                        return < ServiceCard service={service} onTagClick={handleServiceClick}/>
                    })}
                </div>
            </div>
            <div className="streams-wrapper">
                <TimelineBar
                    onSelect={(start, end) => { setStartTime(Math.floor(start)); setEndTime(Math.ceil(end)); }}
                    onClear={() => { setStartTime(null); setEndTime(null); }}
                />
                {/* Filter indicator — above scroll area */}
                {(usedTags.length > 0 || usedServices.length > 0 || headerFilters.length > 0 || startTime != null) && (
                    <div className="filter-indicator">
                        <span>Фильтр активен</span>
                        {startTime != null && (
                            <div style={{ color: 'var(--re-text-secondary)', fontSize: '11px', marginTop: '4px' }}>
                                {new Date(startTime * 1000).toLocaleTimeString()} — {new Date(endTime! * 1000).toLocaleTimeString()}
                                <button onClick={() => { setStartTime(null); setEndTime(null); }} style={{ background: 'none', border: 'none', color: 'var(--re-text-muted)', cursor: 'pointer', marginLeft: '6px', fontSize: '11px', padding: '0' }}>✕</button>
                            </div>
                        )}
                        {headerFilters.length > 0 && (
                            <div className="filter-indicator-chips">
                                {headerFilters.map((f, i) => (
                                    <span key={i} className="filter-indicator-chip">
                                        {f.name}: {f.value.length > 25 ? f.value.slice(0, 25) + '...' : f.value}
                                        <button onClick={() => removeHeaderFilter(i)}>✕</button>
                                    </span>
                                ))}
                            </div>
                        )}
                        {headerFilters.length > 0 && (
                            <button className="filter-clear-btn" onClick={() => { setHeaderFilters([]); setHeaderSearchResults(null); }}>
                                Сбросить HTTP фильтры
                            </button>
                        )}
                    </div>
                )}
                <div id="streams" className="streams" ref={streamsContainerRef}>
                {filteredStreams.map((stream, index) => {
                    return < StreamCard
                        key={stream.id}
                        streamObj={stream}
                        onStreamClick={handleStreamClick}
                        onFavoriteToggle={handleFavoriteToggle}
                        onIpFilter={(ip: string) => addHeaderFilter('_ip', ip)}
                        onIpBan={(ip: string) => {
                            if (!confirm(`Забанить IP ${ip} на всех агентах?`)) return;
                            fetch('/api/protect/ip-ban', {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({ agent_id: '_all', ip, ban: true })
                            });
                        }}
                        tagColors={tagColors}
                    />
                })}
                {isLoadingMore && (
                    <div style={{ padding: '20px', textAlign: 'center', color: '#ccc' }}>
                        Загрузка...
                    </div>
                )}
                {!hasMoreStreams && filteredStreams.length > 0 && !headerSearchResults && (
                    <div style={{ padding: '20px', textAlign: 'center', color: '#888' }}>
                        Все стримы загружены
                    </div>
                )}
                {headerSearchResults && filteredStreams.length === 0 && (
                    <div style={{ padding: '20px', textAlign: 'center', color: '#888' }}>
                        Нет стримов по заданным фильтрам
                    </div>
                )}
                </div>{/* streams */}
            </div>{/* streams-wrapper */}
            <div className="packetData">
                {packetData.length > 0 && (
                    <div className="packet-toolbar">
                        <div className="toolbar-left">
                            <button
                                className={`merge-toggle-btn ${shouldMerge ? 'active' : ''}`}
                                onClick={() => setMergeOverride(shouldMerge ? false : true)}
                                title={shouldMerge ? 'Показать отдельные TCP-сегменты' : 'Склеить соседние пакеты'}
                            >
                                {shouldMerge ? 'Склейка вкл' : 'Склейка выкл'}
                            </button>
                            {mergeOverride !== null && (
                                <button className="merge-reset-btn" onClick={() => setMergeOverride(null)}>авто</button>
                            )}
                        </div>
                        <div className="toolbar-right">
                            <button className="toolbar-btn" onClick={shareStream} title="Скопировать ссылку на стрим">
                                {shareCopied ? '✓ Скопировано' : 'Share'}
                            </button>
                            <div className="export-dropdown">
                                <button className="toolbar-btn" onClick={() => setExportMenuOpen(!exportMenuOpen)}>
                                    Export ▾
                                </button>
                                {exportMenuOpen && (
                                    <div className="export-menu">
                                        <button onClick={() => { copyText(exportPwntools()); setExportMenuOpen(false); }}>
                                            pwntools
                                        </button>
                                        {selectedStreamL7 === 'http1' && (
                                            <>
                                                <button onClick={() => { copyText(exportRequests()); setExportMenuOpen(false); }}>
                                                    requests
                                                </button>
                                                <button onClick={() => { copyText(exportCurl()); setExportMenuOpen(false); }}>
                                                    curl
                                                </button>
                                            </>
                                        )}
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>
                )}
                {mergedPackets.map(packet => (
                    <PacketCard
                        key={packet.id}
                        {...packet}
                        isHttp={selectedStreamL7 === 'http1'}
                        isGrpc={selectedStreamL7 === 'grpc'}
                        onHeaderSearch={addHeaderFilter}
                        diffSelected={diffLeft?.id === packet.id}
                        onDiffSelect={(pkt) => {
                            if (!diffLeft) {
                                setDiffLeft(pkt);
                            } else if (diffLeft.id === pkt.id) {
                                setDiffLeft(null);
                            } else {
                                setDiffRight(pkt);
                                setDiffOpen(true);
                            }
                        }}
                        onQuickBlock={(pattern: string) => {
                            if (!confirm(`Заблокировать паттерн "${pattern.slice(0, 50)}"?`)) return;
                            const isHex = /^[0-9a-f]+$/i.test(pattern) && pattern.length % 2 === 0 && pattern.length > 2;
                            fetch('/api/tag/names', {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({
                                    name: `block-${pattern.slice(0, 20)}`,
                                    color: 0,
                                    template_type: isHex ? 'bsubstr' : 'substr',
                                    template: pattern,
                                    direction: 'both',
                                    special_type: 'block'
                                })
                            });
                        }}
                    />
                ))}
            </div>

            {diffOpen && diffLeft && diffRight && (
                <DiffView
                    leftData={diffLeft.packet_data}
                    rightData={diffRight.packet_data}
                    leftLabel={`Пакет #${diffLeft.id} (${diffLeft.direction === 'orig' ? 'исх' : 'вх'})`}
                    rightLabel={`Пакет #${diffRight.id} (${diffRight.direction === 'orig' ? 'исх' : 'вх'})`}
                    onClose={() => { setDiffOpen(false); setDiffLeft(null); setDiffRight(null); }}
                />
            )}
        </div>
    );
}
