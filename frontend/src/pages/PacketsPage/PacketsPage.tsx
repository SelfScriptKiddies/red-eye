import { useCallback, CSSProperties } from "react";
import './PacketsPage.css'
import { useState, useEffect, useRef, useMemo } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { List as VirtualList, useListRef } from 'react-window';
import { StreamCard } from '../../components/StreamCard'
import { ServiceCard } from '../../components/ServiceCard'
import { PacketCard } from '../../components/PacketCard';
import { DiffView } from '../../components/DiffView';

import { TimelineBar } from '../../components/TimelineBar/TimelineBar';
import { apiFetch, copyText } from '../../lib/api';
import { shq, pyStr } from '../../lib/exportEscaping';
import { useStreams } from '../../lib/useStreams';
import { usePackets } from '../../lib/usePackets';
import { useRealtimeUpdates } from '../../lib/useRealtimeUpdates';
import { useStableRowHeight } from '../../lib/useStableRowHeight';
import { useRedEye } from '../../context/RedEyeContext';


// Row component for virtual stream list
interface StreamRowProps {
    streams: any[];
    selectedStreamIds: number[];
    handleStreamClick: (_id: number) => void;
    handleFavoriteToggle: (_id: number, _fav: boolean) => void;
    addHeaderFilter: (_name: string, _value: string) => void;
    tagColors: Record<string, number>;
}

// Stream src/dst ips come CIDR-styled from the API ("10.0.0.1/32"); the
// ban endpoint (and the agent's nftables) wants a bare address.
const stripCidr = (ip?: unknown): string => String(ip ?? '').replace(/\/\d+$/, '');

// index/style/ariaAttributes are injected by react-window at render time;
// they must NOT be part of the rowProps object the library type-checks, so
// they are declared optional here.
function StreamRow({ index = 0, style, streams, selectedStreamIds, handleStreamClick, handleFavoriteToggle, addHeaderFilter, tagColors }: StreamRowProps & { index?: number; style?: CSSProperties; ariaAttributes?: object }) {
    const stream = streams[index];
    if (!stream) return null;
    return (
        <div style={style}>
            <StreamCard
                streamObj={stream}
                selected={selectedStreamIds.includes(stream.id)}
                onStreamClick={handleStreamClick}
                onFavoriteToggle={handleFavoriteToggle}
                onIpFilter={(ip: string) => addHeaderFilter('_ip', ip)}
                onIpBan={async (ip: string) => {
                    const bare = stripCidr(ip);
                    if (!confirm(`Забанить IP ${bare} на всех агентах?`)) return;
                    try {
                        const response = await fetch('/api/protect/ip-ban', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ agent_id: '_all', ip: bare, ban: true })
                        });
                        if (!response.ok) {
                            alert(`Не удалось забанить ${bare}: ${await response.text()}`);
                            return;
                        }
                        const result = await response.json().catch(() => null);
                        if (result && typeof result.agents === 'number' && result.agents === 0) {
                            alert(`IP ${bare} отправлен, но онлайн-агентов нет — бан нигде не применился.`);
                        }
                    } catch (e) {
                        alert(`Не удалось забанить ${bare}: ${e}`);
                    }
                }}
                tagColors={tagColors}
            />
        </div>
    );
}

// Row component for virtual packet list
interface PacketRowProps {
    packets: any[];
    isMultiSelect: boolean;
    selectedStreamL7: string | null;
    diffLeft: any | null;
    addHeaderFilter: (_name: string, _value: string, _exclude?: boolean) => void;
    onHeaderExclude: (_name: string, _value: string) => void;
    onQuickBlock: (_pattern: string) => void;
    onDiffSelect: (_p: any) => void;
}

// Stable per-packet row identity: heights and effects are keyed by it, not
// by array index (see useStableRowHeight).
const packetRowKey = (packet: any): string => `${packet._stream_id ?? packet.stream_id}:${packet.id}`;

const PacketRow = ({ index = 0, style, packets, isMultiSelect, selectedStreamL7, diffLeft, addHeaderFilter, onHeaderExclude, onQuickBlock, onDiffSelect }: PacketRowProps & { index?: number; style?: CSSProperties; ariaAttributes?: object }) => {
    const packet = packets[index];
    if (!packet) return null;
    const rowKey = packetRowKey(packet);
    return (
        <div style={style} data-row-key={rowKey}>
            <PacketCard
                key={rowKey}
                {...packet}
                showStreamId={isMultiSelect}
                isHttp={selectedStreamL7 === 'http1'}
                isGrpc={selectedStreamL7 === 'grpc'}
                onHeaderSearch={addHeaderFilter}
                onHeaderExclude={onHeaderExclude}
                diffSelected={diffLeft?.id === packet.id}
                onDiffSelect={onDiffSelect}
                onQuickBlock={onQuickBlock}
            />
        </div>
    );
};

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
    const [, setHeaderSearching] = useState(false);
    const [diffLeft, setDiffLeft] = useState<any | null>(null);
    const [diffRight, setDiffRight] = useState<any | null>(null);
    const [diffOpen, setDiffOpen] = useState(false);
    const [shareCopied, setShareCopied] = useState(false);
    const [exportMenuOpen, setExportMenuOpen] = useState(false);
    const [streamInfoOpen, setStreamInfoOpen] = useState(false);
    const [showFavoritesOnly, setShowFavoritesOnly] = useState(false);
    // Live toggle (default OFF): packets refresh in 5s batches + the streams
    // list gently catches up while the user is at its top. OFF freezes the
    // views; whatever piles up is counted on the Update button.
    const [liveEnabled, setLiveEnabled] = useState(false);

    const queryClient = useQueryClient();

    // Получаем состояние RedEye из контекста
    const { isActive: isRedEyeActive } = useRedEye();

    // Live composes with the existing RedEye gate.
    const live = liveEnabled && !isRedEyeActive;

    const streamTags = useMemo(() => filterMode === 'and' ? usedTags : [], [filterMode, usedTags]);
    const streamServices = useMemo(() => filterMode === 'and' ? usedServices : [], [filterMode, usedServices]);

    const {
        streams: streamsArray,
        newStreamsCount,
        scrollToken,
        isLoadingMore,
        hasMoreStreams,
        updateStream,
        clearStreams,
        loadMore,
        acceptNew,
        onActivity,
    } = useStreams({
        tags: streamTags,
        services: streamServices,
        startTime,
        endTime,
        // Always enabled: the first page must load with Live OFF too, and the
        // freeze keeps background refetches from touching the visible list —
        // pending ids land on the Update badge instead. Live only switches
        // on the 5s apply-batches and the in-place count refresh.
        enabled: true,
        // Live OFF freezes the visible list solid: WS deltas and background
        // refetches only grow the Update badge, nothing ticks in place.
        live,
    });

    const {
        packetData,
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

    // Live OFF keeps the WS subscription alive: it only feeds the freeze
    // counter (new-streams badge on Update) and in-place count patches —
    // the frozen list itself is never touched. Streams list refetches are
    // what Live toggles, so nothing auto-applies until the user hits Update.
    useRealtimeUpdates(!isRedEyeActive, onActivity, selectedStreamIds);

    // One-shot manual refresh: apply the pending «N новых», pull newest
    // streams + currently visible packets.
    const handleManualUpdate = useCallback(() => {
        acceptNew();
        queryClient.invalidateQueries({ queryKey: ['streams'] });
        selectedStreamIds.forEach(id => queryClient.invalidateQueries({ queryKey: ['packets', id] }));
        if (selectedStreamIds.length > 0) loadMultipleStreams(selectedStreamIds);
    }, [queryClient, acceptNew, selectedStreamIds, loadMultipleStreams]);

    const getServices = async () => {
        if (servicesLoaded) return;
        setServicesLoaded(true);
        const data = await apiFetch<any[]>("/api/service");
        if (data) setServices(Object.values(data));
    };
    const getTags = async () => {
        if (tagsLoaded) return;
        setTagsLoaded(true);
        const data = await apiFetch<any[]>("/api/tag/names");
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
            apiFetch<any[]>(`/api/stream?limit=200&offset=0`)
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

    // Combined filtering: the header-search results (when active) form the
    // base list INSTEAD of the server-filtered one, so the tag/service
    // selection must be re-applied client-side on top of it — otherwise
    // toggling a tag while a header search is open silently does nothing
    // (the "reset doesn't reset" bug). In 'and' mode every selected tag
    // name AND service must match; in 'or' mode any of them.
    const filteredStreams = useMemo(() => {
        let streams = headerSearchResults ? headerFilteredStreams : streamsArray;
        if (headerExcludeIds.size > 0) {
            streams = streams.filter((s: any) => !headerExcludeIds.has(s.id));
        }
        if (showFavoritesOnly) {
            streams = streams.filter((s: any) => s.favorite);
        }
        const hasTagServiceFilters = usedTags.length > 0 || usedServices.length > 0;
        if (hasTagServiceFilters && (filterMode === 'or' || headerSearchResults)) {
            // usedTags contains tag IDs (strings), s.tags contains tag names — convert IDs to names
            const selectedTagNames = usedTags.map((id: string) => {
                const tag = tagsArray.find((t: any) => String(t.id) === id);
                return tag?.name;
            }).filter(Boolean);
            streams = streams.filter((s: any) => {
                const matchesTag = selectedTagNames.length > 0 && s.tags?.some((t: string) => selectedTagNames.includes(t));
                const matchesService = usedServices.length > 0 && usedServices.includes(String(s.service_id));
                return filterMode === 'or'
                    ? (matchesTag || matchesService)
                    : (matchesTag || selectedTagNames.length === 0) && (matchesService || usedServices.length === 0);
            });
        }
        return streams;
    }, [headerSearchResults, headerFilteredStreams, streamsArray, headerExcludeIds, showFavoritesOnly, filterMode, usedTags, usedServices, tagsArray]);

    // Stable callbacks so memoized rowProps (below) don't churn on every render.
    const runHeaderSearch = useCallback(async (filters: {name: string, value: string, exclude?: boolean}[]) => {
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
    }, []);

    const addHeaderFilter = useCallback((name: string, value: string, exclude: boolean = false) => {
        setHeaderFilters(prev => {
            const exists = prev.some(f => f.name === name && f.value === value && f.exclude === exclude);
            if (exists) return prev;
            const updated = [...prev, { name, value, exclude }];
            runHeaderSearch(updated);
            return updated;
        });
    }, [runHeaderSearch]);

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

    const handleFavoriteToggle = useCallback(async (streamId, currentFavorite) => {
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
    }, [updateStream]);

    // Infinite scroll callback for virtual stream list
    const handleStreamsRowsRendered = useCallback(
        (visibleRows: { startIndex: number; stopIndex: number }) => {
            if (visibleRows.stopIndex >= filteredStreams.length - 3 && hasMoreStreams && !isLoadingMore) {
                loadMore();
            }
        },
        [filteredStreams.length, hasMoreStreams, isLoadingMore, loadMore]
    );

    // Stable rowProps so react-window rows (and the React.memo'd cards) don't
    // re-render on unrelated parent updates.
    const streamRowProps = useMemo(() => ({
        streams: filteredStreams,
        selectedStreamIds,
        handleStreamClick,
        handleFavoriteToggle,
        addHeaderFilter,
        tagColors,
    }), [filteredStreams, selectedStreamIds, handleStreamClick, handleFavoriteToggle, addHeaderFilter, tagColors]);

    // Stabilized packet-card callbacks. Previously these were inline arrows
    // inside PacketRow — new identities every render broke memo(PacketCard),
    // so every mounted card re-rendered (and re-ran its async loads) on any
    // parent update.
    const onHeaderExclude = useCallback((name: string, value: string) => {
        addHeaderFilter(name, value, true);
    }, [addHeaderFilter]);

    const onQuickBlock = useCallback(async (pattern: string) => {
        if (!confirm(`Заблокировать паттерн "${pattern.slice(0, 50)}"?`)) return;
        const isHex = /^[0-9a-f]+$/i.test(pattern) && pattern.length % 2 === 0 && pattern.length > 2;
        try {
            const response = await fetch('/api/tag/names', {
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
            if (!response.ok) {
                alert(`Не удалось создать блокирующий фильтр: ${await response.text()}`);
            }
        } catch (e) {
            alert(`Не удалось создать блокирующий фильтр: ${e}`);
        }
    }, []);

    const onDiffSelect = useCallback((pkt: any) => {
        if (!diffLeft) {
            setDiffLeft(pkt);
        } else if (diffLeft.id === pkt.id) {
            setDiffLeft(null);
        } else {
            setDiffRight(pkt);
            setDiffOpen(true);
        }
    }, [diffLeft]);

    const packetRowProps = useMemo(() => ({
        packets: mergedPackets,
        isMultiSelect,
        selectedStreamL7,
        diffLeft,
        addHeaderFilter,
        onHeaderExclude,
        onQuickBlock,
        onDiffSelect,
    }), [mergedPackets, isMultiSelect, selectedStreamL7, diffLeft, addHeaderFilter, onHeaderExclude, onQuickBlock, onDiffSelect]);

    // Packet cards expand on click, so their height is variable. A fixed rowHeight
    // makes an expanded card overlap the rows below it — measure each row instead.
    // Heights are cached by stable packet identity (stream_id:packet_id), not by
    // array index: switching streams or re-merging must not leak the previous
    // list's heights onto new cards (they rendered wrong and jumped).
    const packetRowHeight = useStableRowHeight({ defaultRowHeight: 120, items: mergedPackets, getKey: packetRowKey });

    // Streams list imperative handle: scroll to top when the user accepts
    // the «N новых» plaque (the frozen snapshot is replaced by the fresh
    // page — without this the viewport would stay where the user scrolled).
    const streamsListRef = useListRef();
    const lastScrollTokenRef = useRef(scrollToken);
    useEffect(() => {
        if (scrollToken === lastScrollTokenRef.current) return;
        lastScrollTokenRef.current = scrollToken;
        if (scrollToken === 0) return;
        streamsListRef.current?.scrollToRow({ index: 0, align: 'start', behavior: 'auto' });
    }, [scrollToken, streamsListRef]);

    // ---- Live follow (batched) ----
    // One batch every 5s: reload the selected streams' packets (scrolling to
    // the newest row only when the user is already near the bottom) and
    // gently apply accumulated new streams when the list is at its top —
    // instead of reacting to every WS activity flush.
    const packetsListRef = useListRef();
    useEffect(() => {
        if (!liveEnabled || selectedStreamIds.length === 0) return;
        const timer = setInterval(() => {
            loadMultipleStreams(selectedStreamIds);
            const streamsEl = streamsListRef.current?.element;
            if (streamsEl && streamsEl.scrollTop < 40) {
                acceptNew(false);
            }
        }, 5000);
        return () => clearInterval(timer);
    }, [liveEnabled, selectedStreamIds, loadMultipleStreams, acceptNew, streamsListRef]);

    const lastPacketCountRef = useRef(0);
    useEffect(() => {
        const grew = mergedPackets.length > lastPacketCountRef.current;
        lastPacketCountRef.current = mergedPackets.length;
        if (!liveEnabled || !grew || mergedPackets.length === 0) return;
        // Follow the tail only when the user is already near the bottom —
        // jumping to the newest row while someone reads an old packet is
        // exactly the jerkiness live mode must not have.
        const el = packetsListRef.current?.element;
        if (el && el.scrollHeight - el.scrollTop - el.clientHeight > 80) return;
        packetsListRef.current?.scrollToRow({ index: mergedPackets.length - 1, align: 'end', behavior: 'auto' });
    }, [mergedPackets.length, liveEnabled, packetsListRef]);

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
    // Stream src/dst ips come CIDR-styled from the API; stripCidr lives at
    // module level (shared with the IP-ban handler).
    const servicePortFor = (streamId: number): number | undefined => {
        const st = streamsArray.find((s: any) => s.id === streamId);
        const svc = servicesArray.find((s: any) => s.id === st?.service_id);
        return svc?.port;
    };

    const exportPwntools = (): string => {
        if (!packetData || packetData.length === 0) return '';
        const lines = [`from pwn import *\n`];

        if (isMultiSelect) {
            // Create a remote per stream
            for (const sid of selectedStreamIds) {
                const st = streamsArray.find((s: any) => s.id === sid);
                const port = servicePortFor(sid);
                const portArg = port !== undefined ? String(port) : "'PORT'";
                lines.push(`r_${sid} = remote('TARGET_IP', ${portArg})  # ${st?.service_name ?? `stream #${sid}`}: ${stripCidr(st?.src_ip)} -> ${stripCidr(st?.dst_ip)}`);
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
            const sid = selectedStreamIds[0];
            const st = streamsArray.find((s: any) => s.id === sid);
            const port = servicePortFor(sid);
            const portArg = port !== undefined ? String(port) : "'PORT'";
            lines.push(`r = remote('TARGET_IP', ${portArg})  # ${st?.service_name ?? ''}: ${stripCidr(st?.src_ip)} -> ${stripCidr(st?.dst_ip)}\n`);
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
        const port = servicePortFor(selectedStreamIds[0]);
        const basePort = port !== undefined ? String(port) : 'PORT';
        const lines = [`import requests\n`, `s = requests.Session()`, `BASE = 'http://TARGET_IP:${basePort}'\n`];
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
                        cookieEntries.push(`    ${pyStr(k.trim())}: ${pyStr(v.join('='))}`);
                    }
                } else {
                    headerEntries.push(`    ${pyStr(name)}: ${pyStr(value)}`);
                }
            }

            const suffix = reqIdx > 0 ? `_${reqIdx}` : '';
            if (headerEntries.length > 0) {
                lines.push(`headers${suffix} = {\n${headerEntries.join(',\n')}\n}`);
            }
            if (cookieEntries.length > 0) {
                lines.push(`cookies${suffix} = {\n${cookieEntries.join(',\n')}\n}`);
            }

            // path is attacker-controlled (request line of a captured packet):
            // it must land in the snippet as an escaped Python literal, never
            // interpolated raw into the f-string.
            let call = `resp = s.${method.toLowerCase()}(BASE + ${pyStr(path)}`;
            if (headerEntries.length > 0) call += `, headers=headers${suffix}`;
            if (cookieEntries.length > 0) call += `, cookies=cookies${suffix}`;
            if (body && method !== 'GET') call += `, data=${pyStr(body)}`;
            call += ')';
            lines.push(call);
            lines.push(`print(resp.status_code, resp.text[:200])\n`);
            reqIdx++;
        }
        return lines.join('\n');
    };

    const exportCurl = (): string => {
        if (!packetData || packetData.length === 0) return '';
        const curlPort = servicePortFor(selectedStreamIds[0]);
        const curlPortStr = curlPort !== undefined ? String(curlPort) : 'PORT';
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

            // URL, headers and body all contain attacker-controlled packet
            // data — every interpolation goes through shq() so a payload like
            // `'; rm -rf /; '` or `$(reboot)` stays literal shell text.
            const parts = [`curl -s -X ${method} ${shq(`http://TARGET_IP:${curlPortStr}${path}`)}`];
            for (const line of headerLines) {
                const idx = line.indexOf(':');
                if (idx > 0) {
                    const name = line.slice(0, idx).trim();
                    if (!['Host', 'Content-Length', 'Connection'].includes(name)) {
                        parts.push(`  -H ${shq(line.trim())}`);
                    }
                }
            }
            if (body && method !== 'GET') {
                parts.push(`  -d ${shq(body)}`);
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
                    <button className="filter-action-btn" onClick={() => {
                        // Full reset: filters, favorites, time range AND the
                        // header/path search — leaving the latter behind made
                        // the button look broken ("reset doesn't reset").
                        clearStreams();
                        setUsedTags([]); setUsedServices([]); setFilterMode('and'); setShowFavoritesOnly(false);
                        clearTimeSelection();
                        setHeaderFilters([]); setHeaderSearchResults(null); setHeaderExcludeIds(new Set());
                    }}>
                        Сброс
                    </button>
                </div>
                {(usedTags.length > 0 || usedServices.length > 0) && (
                    <div className={`filter-mode-badge ${filterMode === 'or' ? 'filter-mode-or' : 'filter-mode-and'}`}>
                        {filterMode === 'or' ? 'OR' : 'AND'}
                    </div>
                )}
                <div className={`tag-filter d-inline-block${filterMode === 'or' ? ' filter-or-mode' : ''}`}>
                    {tagsArray.map((service) => {
                        return < ServiceCard key={service.id} service={service} onTagClick={handleTagClick}/>
                    })}
                    </div>
                <div className={`service-filter d-inline-block${filterMode === 'or' ? ' filter-or-mode' : ''}`}>
                    {servicesArray.map((service) => {
                        return < ServiceCard key={service.id} service={service} onTagClick={handleServiceClick}/>
                    })}
                </div>
            </div>
            <div className="streams-wrapper">
                <TimelineBar
                    onSelect={(start, end) => { setStartTime(Math.floor(start)); setEndTime(Math.ceil(end)); }}
                    onClear={clearTimeSelection}
                    clearRef={timelineClearRef}
                />
                {/* Live controls: between the histogram and the streams list.
                    Live = batched auto-follow; Update applies what piled up
                    while Live was off (badge shows how much). */}
                <div className="packets-live-strip">
                    <button
                        className={`merge-toggle-btn ${liveEnabled ? 'active' : ''}`}
                        onClick={() => setLiveEnabled(prev => !prev)}
                        title={liveEnabled ? 'Автообновление включено: стримы и пакеты обновляются пачками' : 'Автообновление выключено — нажмите Update, чтобы применить накопленное'}
                    >
                        {liveEnabled ? '● Live' : '○ Live'}
                    </button>
                    <button className="toolbar-btn" onClick={handleManualUpdate} title="Обновить вручную (применить накопленное)">
                        ⟳ Update
                        {!liveEnabled && newStreamsCount > 0 && (
                            <span className="live-strip-badge">{newStreamsCount}</span>
                        )}
                    </button>
                </div>
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
                {/* No «⏸ N новых» plaque: with Live ON the 5s batch applies
                    pending streams gently (when the list is at top); with
                    Live OFF the same count lives on the Update badge. */}
                <div id="streams" className="streams">
                    {filteredStreams.length > 0 ? (
                        <VirtualList
                            rowComponent={StreamRow}
                            rowCount={filteredStreams.length}
                            rowHeight={80}
                            rowProps={streamRowProps}
                            onRowsRendered={handleStreamsRowsRendered}
                            defaultHeight={400}
                            listRef={streamsListRef}
                        />
                    ) : null}
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
                            <div className="stream-info-wrap">
                                <button
                                    className="toolbar-btn"
                                    onClick={() => setStreamInfoOpen(v => !v)}
                                    title="Информация о выбранных стримах (src/dst, порт сервиса)"
                                >
                                    ⓘ Info
                                </button>
                                {streamInfoOpen && (
                                    <>
                                        <div className="stream-info-backdrop" onClick={() => setStreamInfoOpen(false)} />
                                        <div className="stream-info-popover">
                                            {selectedStreamIds.map(id => {
                                                const st = streamsArray.find((s: any) => s.id === id);
                                                const svc = servicesArray.find((s: any) => s.id === st?.service_id);
                                                return (
                                                    <div key={id} className="stream-info-row">
                                                        <div className="stream-info-title">{st?.service_name ?? 'stream'} #{id} · порт {svc?.port ?? '—'}</div>
                                                        <div className="stream-info-addr">{stripCidr(st?.src_ip)} → {stripCidr(st?.dst_ip)}</div>
                                                        <div className="stream-info-meta">
                                                            {String(st?.l7_protocol ?? '')} · {st?.packet_count ?? 0} pkt
                                                        </div>
                                                    </div>
                                                );
                                            })}
                                        </div>
                                    </>
                                )}
                            </div>
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
                <div className="packets-list">
                    {mergedPackets.length > 0 && (
                        <VirtualList
                            listRef={packetsListRef}
                            rowComponent={PacketRow}
                            rowCount={mergedPackets.length}
                            rowHeight={packetRowHeight}
                            rowProps={packetRowProps}
                            defaultHeight={400}
                        />
                    )}
                </div>
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
