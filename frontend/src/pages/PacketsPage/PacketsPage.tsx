import { ReactElement } from "react";
import './PacketsPage.css'
import { useState, useEffect, useRef, useMemo } from 'react';
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
    const [filterMode, setFilterMode] = useState<'and' | 'or'>('and');
    const [startTime, setStartTime] = useState<number | null>(null);
    const [endTime, setEndTime] = useState<number | null>(null);
    const timelineClearRef = useRef<(() => void) | null>(null);
    const clearTimeSelection = () => { setStartTime(null); setEndTime(null); timelineClearRef.current?.(); };
    const [headerFilters, setHeaderFilters] = useState<{name: string, value: string, exclude?: boolean}[]>([]);
    const [headerSearchResults, setHeaderSearchResults] = useState<{stream_ids: number[], total: number} | null>(null);
    const [headerExcludeIds, setHeaderExcludeIds] = useState<Set<number>>(new Set());
    const [headerSearching, setHeaderSearching] = useState(false);
    const [diffLeft, setDiffLeft] = useState<any | null>(null);
    const [diffRight, setDiffRight] = useState<any | null>(null);
    const [diffOpen, setDiffOpen] = useState(false);
    const [shareCopied, setShareCopied] = useState(false);
    const [exportMenuOpen, setExportMenuOpen] = useState(false);
    const [showFavoritesOnly, setShowFavoritesOnly] = useState(false);

    // Получаем состояние RedEye из localStorage
    const isRedEyeActive = localStorage.getItem('redEyeActive') === 'true';

    const streamTags = useMemo(() => filterMode === 'and' ? usedTags : [], [filterMode, usedTags]);
    const streamServices = useMemo(() => filterMode === 'and' ? usedServices : [], [filterMode, usedServices]);

    const {
        streams: streamsArray,
        isLoadingMore,
        hasMoreStreams,
        scrollRef: streamsContainerRef,
        refresh,
        updateStream,
        clearStreams,
        loadStreams,
    } = useStreams({
        tags: streamTags,
        services: streamServices,
        startTime,
        endTime,
        enabled: !isRedEyeActive,
    });

    const {
        packetData,
        selectedStreamId,
        selectedStreamIds,
        isMultiSelect,
        selectedStreamL7,
        mergeOverride,
        shouldMerge,
        mergedPackets,
        setMergeOverride,
        handleStreamClick,
        loadMultipleStreams,
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
                const ids = streamParam.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n));
                if (ids.length === 1) {
                    setTimeout(() => handleStreamClick(ids[0]), 500);
                } else if (ids.length > 1) {
                    setTimeout(() => loadMultipleStreams(ids), 500);
                }
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

    const filteredStreams = (() => {
        let streams = headerSearchResults ? headerFilteredStreams : streamsArray;
        if (headerExcludeIds.size > 0) {
            streams = streams.filter((s: any) => !headerExcludeIds.has(s.id));
        }
        if (showFavoritesOnly) {
            streams = streams.filter((s: any) => s.favorite);
        }
        if (filterMode === 'or' && (usedTags.length > 0 || usedServices.length > 0)) {
            // usedTags contains tag IDs (strings), s.tags contains tag names — convert IDs to names
            const selectedTagNames = usedTags.map((id: string) => {
                const tag = tagsArray.find((t: any) => String(t.id) === id);
                return tag?.name;
            }).filter(Boolean);
            streams = streams.filter((s: any) => {
                const matchesTag = selectedTagNames.length > 0 && s.tags?.some((t: string) => selectedTagNames.includes(t));
                const matchesService = usedServices.length > 0 && usedServices.includes(String(s.service_id));
                return matchesTag || matchesService;
            });
        }
        return streams;
    })();

    const addHeaderFilter = (name: string, value: string, exclude: boolean = false) => {
        const exists = headerFilters.some(f => f.name === name && f.value === value && f.exclude === exclude);
        if (!exists) {
            const updated = [...headerFilters, { name, value, exclude }];
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
            setHeaderExcludeIds(new Set());
        }
    };

    const runHeaderSearch = async (filters: {name: string, value: string, exclude?: boolean}[]) => {
        setHeaderSearching(true);
        try {
            const includeFilters = filters.filter(f => !f.exclude);
            const excludeFilters = filters.filter(f => f.exclude);

            let includeResult = null;
            if (includeFilters.length > 0) {
                includeResult = await fetch('/api/http/search/multi', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(includeFilters.map(f => ({ name: f.name, value: f.value }))),
                }).then(r => r.json());
            }
            setHeaderSearchResults(includeResult);

            if (excludeFilters.length > 0) {
                const excludeResult = await fetch('/api/http/search/multi', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(excludeFilters.map(f => ({ name: f.name, value: f.value }))),
                }).then(r => r.json());
                setHeaderExcludeIds(new Set(excludeResult?.stream_ids ?? []));
            } else {
                setHeaderExcludeIds(new Set());
            }
        } catch {
            setHeaderSearchResults(null);
            setHeaderExcludeIds(new Set());
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

    // Share: copy link + auto-favorite all selected
    const shareStream = async () => {
        if (selectedStreamIds.length === 0) return;
        const streamParam = selectedStreamIds.join(',');
        const url = `${window.location.origin}/packets?stream=${streamParam}`;
        copyText(url);
        // Auto-favorite all selected streams
        for (const id of selectedStreamIds) {
            const stream = streamsArray.find((s: any) => s.id === id);
            if (stream && !stream.favorite) {
                handleFavoriteToggle(id, false);
            }
        }
        setShareCopied(true);
        setTimeout(() => setShareCopied(false), 2000);
    };

    // Export generators
    const exportPwntools = (): string => {
        if (!packetData || packetData.length === 0) return '';
        const lines = [`from pwn import *\n`];

        if (isMultiSelect) {
            // Create a remote per stream
            for (const sid of selectedStreamIds) {
                lines.push(`r_${sid} = remote('TARGET_IP', '????')  # stream #${sid}`);
            }
            lines.push('');
            for (const pkt of packetData) {
                const sid = pkt._stream_id ?? pkt.stream_id;
                const varName = `r_${sid}`;
                const raw = atob(pkt.packet_data);
                const pyBytes = Array.from(raw).map((c: string) => {
                    const code = c.charCodeAt(0);
                    if (code >= 0x20 && code <= 0x7e && c !== '\\' && c !== "'") return c;
                    return `\\x${code.toString(16).padStart(2, '0')}`;
                }).join('');
                if (pkt.direction === 'dest') {
                    lines.push(`${varName}.send(b'${pyBytes}')`);
                } else {
                    lines.push(`${varName}.recv(${raw.length})  # ${raw.length} bytes`);
                }
            }
        } else {
            lines.push(`r = remote('TARGET_IP', '????')\n`);
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
        }
        return lines.join('\n');
    };

    const exportRequests = (): string => {
        if (!packetData || packetData.length === 0) return '';
        const lines = [`import requests\n`, `s = requests.Session()`, `BASE = 'http://TARGET_IP:PORT'\n`];
        let reqIdx = 0;
        for (const pkt of mergedPackets) {
            if (pkt.direction !== 'dest') continue;
            const text = atob(pkt.packet_data);
            const methodMatch = text.match(/^(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+(\S+)\s+HTTP/);
            if (!methodMatch) continue;
            const [, method, path] = methodMatch;
            const headerSection = text.split('\r\n\r\n')[0];
            const headerLines = headerSection.split('\r\n').slice(1);
            const body = text.split('\r\n\r\n').slice(1).join('\r\n\r\n');

            const headerEntries: string[] = [];
            const cookieEntries: string[] = [];
            for (const line of headerLines) {
                const idx = line.indexOf(':');
                if (idx <= 0) continue;
                const name = line.slice(0, idx).trim();
                const value = line.slice(idx + 1).trim();
                if (['Host', 'Content-Length', 'Connection'].includes(name)) continue;
                if (name.toLowerCase() === 'cookie') {
                    const cookies = value.split(';').map(c => c.trim()).filter(c => c.includes('='));
                    for (const cookie of cookies) {
                        const [k, ...v] = cookie.split('=');
                        cookieEntries.push(`    '${k.trim()}': '${v.join('=')}'`);
                    }
                } else {
                    headerEntries.push(`    '${name}': '${value}'`);
                }
            }

            const suffix = reqIdx > 0 ? `_${reqIdx}` : '';
            if (headerEntries.length > 0) {
                lines.push(`headers${suffix} = {\n${headerEntries.join(',\n')}\n}`);
            }
            if (cookieEntries.length > 0) {
                lines.push(`cookies${suffix} = {\n${cookieEntries.join(',\n')}\n}`);
            }

            let call = `resp = s.${method.toLowerCase()}(f'{BASE}${path}'`;
            if (headerEntries.length > 0) call += `, headers=headers${suffix}`;
            if (cookieEntries.length > 0) call += `, cookies=cookies${suffix}`;
            if (body && method !== 'GET') call += `, data=${JSON.stringify(body)}`;
            call += ')';
            lines.push(call);
            lines.push(`print(resp.status_code, resp.text[:200])\n`);
            reqIdx++;
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
        const isAlt = e.altKey;
        setUsedTags(prev => {
            if (prev.includes(tagName)) {
                const next = prev.filter(t => t !== tagName);
                if (next.length === 0) setFilterMode('and');
                return next;
            }
            if (prev.length === 0 && usedServices.length === 0) {
                setFilterMode(isAlt ? 'or' : 'and');
            }
            return [...prev, tagName];
        });
    };
    const handleServiceClick = (e) => {
        const serviceName = e.target.dataset.service;
        const isAlt = e.altKey;
        setUsedServices(prev => {
            if (prev.includes(serviceName)) {
                const next = prev.filter(s => s !== serviceName);
                if (next.length === 0 && usedTags.length === 0) setFilterMode('and');
                return next;
            }
            if (prev.length === 0 && usedTags.length === 0) {
                setFilterMode(isAlt ? 'or' : 'and');
            }
            return [...prev, serviceName];
        });
    };

    return (
        <div className="packetsPage">
            <div className="filterTape">
                <div className="filter-buttons-row">
                    <button
                      className={`filter-action-btn ${showFavoritesOnly ? 'filter-action-btn--active' : ''}`}
                      onClick={() => setShowFavoritesOnly(prev => !prev)}
                    >
                      ★ Избранные
                    </button>
                    <button className="filter-action-btn" onClick={() => { clearStreams(); setUsedTags([]); setUsedServices([]); setFilterMode('and'); setShowFavoritesOnly(false); }}>
                        Сброс
                    </button>
                </div>
                {(usedTags.length > 0 || usedServices.length > 0) && (
                    <div className={`filter-mode-badge ${filterMode === 'or' ? 'filter-mode-or' : 'filter-mode-and'}`}>
                        {filterMode === 'or' ? 'OR' : 'AND'}
                    </div>
                )}
                <div className={`tag-filter d-inline-block${filterMode === 'or' ? ' filter-or-mode' : ''}`}>
                    {tagsArray.map((service, index) => {
                        return < ServiceCard service={service} onTagClick={handleTagClick}/>
                    })}
                    </div>
                <div className={`service-filter d-inline-block${filterMode === 'or' ? ' filter-or-mode' : ''}`}>
                    {servicesArray.map((service, index) => {
                        return < ServiceCard service={service} onTagClick={handleServiceClick}/>
                    })}
                </div>
            </div>
            <div className="streams-wrapper">
                <TimelineBar
                    onSelect={(start, end) => { setStartTime(Math.floor(start)); setEndTime(Math.ceil(end)); }}
                    onClear={clearTimeSelection}
                    clearRef={timelineClearRef}
                />
                {/* Filter indicator — above scroll area */}
                {(usedTags.length > 0 || usedServices.length > 0 || headerFilters.length > 0 || startTime != null || showFavoritesOnly) && (
                    <div className="filter-indicator">
                        <span>Фильтр активен</span>
                        {startTime != null && (
                            <div style={{ color: 'var(--re-text-secondary)', fontSize: '11px', marginTop: '4px' }}>
                                {new Date(startTime * 1000).toLocaleTimeString()} — {new Date(endTime! * 1000).toLocaleTimeString()}
                                <button onClick={clearTimeSelection} style={{ background: 'none', border: 'none', color: 'var(--re-text-muted)', cursor: 'pointer', marginLeft: '6px', fontSize: '11px', padding: '0' }}>✕</button>
                            </div>
                        )}
                        {headerFilters.length > 0 && (
                            <div className="filter-indicator-chips">
                                {headerFilters.map((f, i) => (
                                    <span key={i} className="filter-indicator-chip" style={f.exclude ? { borderColor: 'var(--re-text-dim)' } : undefined}>
                                        {f.exclude ? '⊘ ' : ''}{f.name}: {f.value.length > 25 ? f.value.slice(0, 25) + '...' : f.value}
                                        <button onClick={() => removeHeaderFilter(i)}>✕</button>
                                    </span>
                                ))}
                            </div>
                        )}
                        {headerFilters.length > 0 && (
                            <button className="filter-clear-btn" onClick={() => { setHeaderFilters([]); setHeaderSearchResults(null); setHeaderExcludeIds(new Set()); }}>
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
                        selected={selectedStreamIds.includes(stream.id)}
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
                            {isMultiSelect && (
                                <span style={{ fontSize: '11px', color: 'var(--re-text-muted)', marginRight: '4px' }}>{selectedStreamIds.length} streams</span>
                            )}
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
                        key={`${packet._stream_id ?? packet.stream_id}-${packet.id}`}
                        {...packet}
                        showStreamId={isMultiSelect}
                        isHttp={selectedStreamL7 === 'http1'}
                        isGrpc={selectedStreamL7 === 'grpc'}
                        onHeaderSearch={addHeaderFilter}
                        onHeaderExclude={(name: string, value: string) => addHeaderFilter(name, value, true)}
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
