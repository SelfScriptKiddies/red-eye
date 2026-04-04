import { useState, useEffect, useCallback } from 'react';
import './TimelinePage.css';
import { useStreams } from '../../lib/useStreams';
import { usePackets } from '../../lib/usePackets';
import { useRealtimeUpdates } from '../../lib/useRealtimeUpdates';
import { fetchApi, copyText } from '../../lib/api';
import { TimelineBar } from '../../components/TimelineBar/TimelineBar';
import { StreamCard } from '../../components/StreamCard';
import { ServiceCard } from '../../components/ServiceCard';
import { PacketCard } from '../../components/PacketCard';
import { DiffView } from '../../components/DiffView';

export function TimelinePage() {
    // Filter state
    const [usedTags, setUsedTags] = useState<string[]>([]);
    const [usedServices, setUsedServices] = useState<string[]>([]);
    const [startTime, setStartTime] = useState<number | null>(null);
    const [endTime, setEndTime] = useState<number | null>(null);

    // Services & tags for filter tape
    const [servicesArray, setServices] = useState<any[]>([]);
    const [tagsArray, setTags] = useState<any[]>([]);
    const [tagColors, setTagColors] = useState<Record<string, string>>({});

    const isRedEyeActive = localStorage.getItem('redEyeActive') === 'true';

    // Load services & tags once
    useEffect(() => {
        fetchApi('/api/service').then(d => { if (d) setServices(Object.values(d as any)); });
        fetchApi('/api/tag/names').then(d => {
            if (d) {
                const tags = Object.values(d as any) as any[];
                setTags(tags);
                const colorMap: Record<string, string> = {};
                tags.forEach((t: any) => { colorMap[t.name] = t.color; });
                setTagColors(colorMap);
            }
        });
    }, []);

    // Streams hook with time range
    const {
        streams, isLoadingMore, hasMoreStreams, scrollRef, refresh, updateStream, clearStreams
    } = useStreams({
        tags: usedTags,
        services: usedServices,
        startTime,
        endTime,
        enabled: !isRedEyeActive,
    });

    // Packets hook
    const {
        packetData, selectedStreamId, selectedStreamL7, mergeOverride,
        shouldMerge, mergedPackets, setMergeOverride, handleStreamClick
    } = usePackets(streams, servicesArray);

    // Realtime — only refresh when no time selection is active
    useRealtimeUpdates(() => {
        if (startTime == null && endTime == null) refresh();
    }, !isRedEyeActive);

    // Timeline callbacks
    const handleTimelineSelect = useCallback((start: number, end: number) => {
        setStartTime(start);
        setEndTime(end);
    }, []);
    const handleTimelineClear = useCallback(() => {
        setStartTime(null);
        setEndTime(null);
    }, []);

    // Filter tape handlers
    const handleTagClick = (e: any) => {
        const tagName = e.target.dataset.service;
        setUsedTags(prev => prev.includes(tagName) ? prev.filter(t => t !== tagName) : [...prev, tagName]);
    };
    const handleServiceClick = (e: any) => {
        const serviceName = e.target.dataset.service;
        setUsedServices(prev => prev.includes(serviceName) ? prev.filter(s => s !== serviceName) : [...prev, serviceName]);
    };

    // Favorite
    const handleFavoriteToggle = async (streamId: number, currentFavorite: boolean) => {
        try {
            const response = await fetch(`/api/stream/${streamId}/favorite`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ favorite: !currentFavorite }),
            });
            if (response.ok) updateStream(streamId, s => ({ ...s, favorite: !currentFavorite }));
        } catch (error) { console.error('Error toggling favorite:', error); }
    };

    // Share
    const [shareCopied, setShareCopied] = useState(false);
    const shareStream = () => {
        if (!selectedStreamId) return;
        const url = `${window.location.origin}/packets?stream=${selectedStreamId}`;
        copyText(url);
        const stream = streams.find((s: any) => s.id === selectedStreamId);
        if (stream && !stream.favorite) handleFavoriteToggle(selectedStreamId, false);
        setShareCopied(true);
        setTimeout(() => setShareCopied(false), 2000);
    };

    // Diff
    const [diffLeft, setDiffLeft] = useState<any>(null);
    const [diffRight, setDiffRight] = useState<any>(null);
    const [diffOpen, setDiffOpen] = useState(false);

    // Export
    const [exportMenuOpen, setExportMenuOpen] = useState(false);
    const exportPwntools = (): string => {
        if (!packetData || packetData.length === 0) return '';
        const lines = [`from pwn import *\n`, `r = remote('TARGET_IP', '????')\n`];
        for (const pkt of packetData) {
            const raw = atob(pkt.packet_data);
            const pyBytes = Array.from(raw).map((c: string) => {
                const code = c.charCodeAt(0);
                if (code >= 0x20 && code <= 0x7e && c !== '\\' && c !== "'") return c;
                return `\\x${code.toString(16).padStart(2, '0')}`;
            }).join('');
            if (pkt.direction === 'dest') lines.push(`r.send(b'${pyBytes}')`);
            else lines.push(`r.recv(${raw.length})  # ${raw.length} bytes`);
        }
        lines.push(`\nr.interactive()`);
        return lines.join('\n');
    };

    return (
        <div className="timelinePage">
            <TimelineBar onSelect={handleTimelineSelect} onClear={handleTimelineClear} />

            <div className="timelinePage-content">
                <div className="filterTape">
                    <button className="clear-button" onClick={clearStreams}>Очистить очередь</button>
                    <div className="tag-filter d-inline-block">
                        {tagsArray.map((tag: any) => (
                            <ServiceCard key={tag.id || tag.name} service={tag} onTagClick={handleTagClick} />
                        ))}
                    </div>
                    <div className="service-filter d-inline-block">
                        {servicesArray.map((svc: any) => (
                            <ServiceCard key={svc.id || svc.name} service={svc} onTagClick={handleServiceClick} />
                        ))}
                    </div>
                </div>

                <div className="streams-wrapper">
                    {(startTime != null || usedTags.length > 0 || usedServices.length > 0) && (
                        <div className="filter-indicator" style={{ background: 'var(--re-bg-highlight)', border: '1px solid var(--re-accent)', borderRadius: '8px', margin: '10px', padding: '10px 14px', fontSize: '12px' }}>
                            <span style={{ color: 'var(--re-accent)', fontWeight: 600, fontSize: '13px' }}>Фильтр активен</span>
                            {startTime != null && (
                                <div style={{ color: 'var(--re-text-secondary)', fontSize: '11px', marginTop: '4px' }}>
                                    {new Date(startTime * 1000).toLocaleTimeString()} — {new Date(endTime! * 1000).toLocaleTimeString()}
                                    <button onClick={handleTimelineClear} style={{ background: 'none', border: 'none', color: 'var(--re-text-muted)', cursor: 'pointer', marginLeft: '6px', fontSize: '11px' }}>✕</button>
                                </div>
                            )}
                        </div>
                    )}
                    <div className="streams" ref={scrollRef}>
                        {streams.map((stream: any) => (
                            <StreamCard key={stream.id} streamObj={stream} onStreamClick={handleStreamClick} onFavoriteToggle={handleFavoriteToggle} onIpFilter={() => {}} onIpBan={() => {}} tagColors={tagColors} />
                        ))}
                        {isLoadingMore && <div style={{ padding: '20px', textAlign: 'center', color: '#ccc' }}>Загрузка...</div>}
                        {!hasMoreStreams && streams.length > 0 && <div style={{ padding: '20px', textAlign: 'center', color: '#888' }}>Все стримы загружены</div>}
                    </div>
                </div>

                <div className="packetData">
                    {packetData.length > 0 && (
                        <div className="packet-toolbar">
                            <div className="toolbar-left">
                                <button className={`merge-toggle-btn ${shouldMerge ? 'active' : ''}`} onClick={() => setMergeOverride(shouldMerge ? false : true)}>
                                    {shouldMerge ? 'Склейка вкл' : 'Склейка выкл'}
                                </button>
                                {mergeOverride !== null && <button className="merge-reset-btn" onClick={() => setMergeOverride(null)}>авто</button>}
                            </div>
                            <div className="toolbar-right">
                                <button className="toolbar-btn" onClick={shareStream}>{shareCopied ? '✓ Скопировано' : 'Share'}</button>
                                <div className="export-dropdown">
                                    <button className="toolbar-btn" onClick={() => setExportMenuOpen(!exportMenuOpen)}>Export ▾</button>
                                    {exportMenuOpen && (
                                        <div className="export-menu">
                                            <button onClick={() => { copyText(exportPwntools()); setExportMenuOpen(false); }}>pwntools</button>
                                        </div>
                                    )}
                                </div>
                            </div>
                        </div>
                    )}
                    {mergedPackets.map((packet: any) => (
                        <PacketCard key={packet.id} {...packet} isHttp={selectedStreamL7 === 'http1'} isGrpc={selectedStreamL7 === 'grpc'} onHeaderSearch={() => {}} diffSelected={diffLeft?.id === packet.id}
                            onDiffSelect={(pkt: any) => {
                                if (!diffLeft) setDiffLeft(pkt);
                                else if (diffLeft.id === pkt.id) setDiffLeft(null);
                                else { setDiffRight(pkt); setDiffOpen(true); }
                            }}
                            onQuickBlock={(pattern: string) => {
                                if (!confirm(`Заблокировать паттерн "${pattern.slice(0, 50)}"?`)) return;
                                const isHex = /^[0-9a-f]+$/i.test(pattern) && pattern.length % 2 === 0 && pattern.length > 2;
                                fetch('/api/tag/names', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: `block-${pattern.slice(0, 20)}`, color: 0, template_type: isHex ? 'bsubstr' : 'substr', template: pattern, direction: 'both', special_type: 'block' }) });
                            }}
                        />
                    ))}
                </div>
            </div>

            {diffOpen && diffLeft && diffRight && (
                <DiffView leftData={diffLeft.packet_data} rightData={diffRight.packet_data}
                    leftLabel={`Пакет #${diffLeft.id} (${diffLeft.direction === 'orig' ? 'исх' : 'вх'})`}
                    rightLabel={`Пакет #${diffRight.id} (${diffRight.direction === 'orig' ? 'исх' : 'вх'})`}
                    onClose={() => { setDiffOpen(false); setDiffLeft(null); setDiffRight(null); }}
                />
            )}
        </div>
    );
}
