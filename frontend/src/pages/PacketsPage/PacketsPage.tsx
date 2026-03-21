import { ReactElement } from "react";
import './PacketsPage.css'
import { useState, useEffect, useRef } from 'react';
import { StreamCard } from '../../components/StreamCard'
import { ServiceCard } from '../../components/ServiceCard'
import { PacketCard } from '../../components/PacketCard';
import { DiffView } from '../../components/DiffView';

interface PacketEvent {
    packet_id: number;
    stream_id: number;
    timestamp_seconds: number;
    timestamp_microseconds: number;
    direction: string;
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

export function PacketsPage() {
    const [streamsArray, setStreams] = useState([]);
    const [servicesArray, setServices] = useState([]);
    const [tagsArray, setTags] = useState([]);
    const [tagColors, setTagColors] = useState({});
    const [packetData, setPacketData] = useState<any[]>([]);
    const [selectedStreamL7, setSelectedStreamL7] = useState<string>('unknown');
    const [selectedStreamId, setSelectedStreamId] = useState<number | null>(null);
    const [pageLoaded, setPageLoaded] = useState(null);
    const [servicesLoaded, setServicesLoaded] = useState(false);
    const [tagsLoaded, setTagsLoaded] = useState(false);
    const [usedTags, setUsedTags] = useState([]);
    const [usedServices, setUsedServices] = useState([]);
    const [currentOffset, setCurrentOffset] = useState(0);
    const [isLoadingMore, setIsLoadingMore] = useState(false);
    const [hasMoreStreams, setHasMoreStreams] = useState(true);
    const [headerFilters, setHeaderFilters] = useState<{name: string, value: string}[]>([]);
    const [headerSearchResults, setHeaderSearchResults] = useState<{stream_ids: number[], total: number} | null>(null);
    const [headerSearching, setHeaderSearching] = useState(false);
    const [mergeOverride, setMergeOverride] = useState<boolean | null>(null); // null = auto
    const [diffLeft, setDiffLeft] = useState<any | null>(null);
    const [diffRight, setDiffRight] = useState<any | null>(null);
    const [diffOpen, setDiffOpen] = useState(false);
    const wsRef = useRef<WebSocket | null>(null);
    const selectedStreamIdRef = useRef<number | null>(null);
    const streamsContainerRef = useRef<HTMLDivElement | null>(null);
    
    // Получаем состояние RedEye из localStorage
    const isRedEyeActive = localStorage.getItem('redEyeActive') === 'true';

    const getUrl = async (url) => {
        const data = await fetch(url, {
            method: 'GET'
        })
            .catch((reason) => {
                document.querySelector(".errorMessage").innerText = `Fuck you, lol. Reason: ${reason}`;
            })
            .then((response) => {
                if (parseInt(response.status / 100) == 5) {
                    document.querySelector(".errorMessage").innerText = "Что-то с сервером";
                    return;
                }
                else
                    return response.json();
            });
        return data;
    };
    const getStreams = async () => {
        let data = await getUrl("/api/stream?limit=20&offset=0");
        // console.log(data);
        const streams = Object.values(data);
        await setStreams(streams);
        setCurrentOffset(20);
        setHasMoreStreams(streams.length === 20);
    };
    const getServices = async () => {
        if (servicesLoaded) return; // Prevent multiple calls
        setServicesLoaded(true);
        let data = await getUrl("/api/service");
        await setServices(Object.values(data));
    };
    const getTags = async () => {
        if (tagsLoaded) return; // Prevent multiple calls
        setTagsLoaded(true);
        let data = await getUrl("/api/tag/names");
        const tagsData = Object.values(data);
        await setTags(tagsData);
        
        // Create a map of tag names to colors for easy lookup
        const colorMap = {};
        tagsData.forEach(tag => {
            colorMap[tag.name] = tag.color;
        });
        setTagColors(colorMap);
    };
    const getFilteredStreams = async (reset: boolean = false) => {
        const offset = reset ? 0 : currentOffset;
        let req_url = `/api/stream?limit=20&offset=${offset}`;
        // let usedTags = this.state.usedTags;
        if (usedTags && usedTags.length > 0)
            req_url += "&tags="+usedTags.join(',')
        if (usedServices && usedServices.length > 0)
            req_url += "&services="+usedServices.join(',')
        
        setIsLoadingMore(true);
        try {
            let data = await getUrl(req_url);
            const newStreams = Object.values(data);
            
            if (reset) {
                await setStreams(newStreams);
                setCurrentOffset(20);
            } else {
                await setStreams((prevStreams: any[]) => [...prevStreams, ...newStreams]);
                setCurrentOffset(prev => prev + 20);
            }
            
            // If we got less than 20 streams, there are no more to load
            setHasMoreStreams(newStreams.length === 20);
        } catch (error) {
            console.error('Error loading streams:', error);
        } finally {
            setIsLoadingMore(false);
        }
    };
    
    // Handle scroll event for infinite scroll
    useEffect(() => {
        const handleScroll = () => {
            if (!streamsContainerRef.current || isLoadingMore || !hasMoreStreams || isRedEyeActive) return;
            
            const container = streamsContainerRef.current;
            const scrollTop = container.scrollTop;
            const scrollHeight = container.scrollHeight;
            const clientHeight = container.clientHeight;
            
            // Load more when user is within 200px of the bottom
            if (scrollTop + clientHeight >= scrollHeight - 200) {
                getFilteredStreams(false);
            }
        };
        
        const container = streamsContainerRef.current;
        if (container) {
            container.addEventListener('scroll', handleScroll);
            return () => container.removeEventListener('scroll', handleScroll);
        }
    }, [isLoadingMore, hasMoreStreams, isRedEyeActive, currentOffset, usedTags, usedServices]);
    // Initial data loading + handle ?stream=ID from URL
    useEffect(() => {
        if (isRedEyeActive) return;

        if (!pageLoaded) {
            setPageLoaded(true);
            getStreams();

            // Auto-select stream from URL query param
            const params = new URLSearchParams(window.location.search);
            const streamParam = params.get('stream');
            if (streamParam) {
                setTimeout(() => handleStreamClick(parseInt(streamParam)), 500);
            }
        }

        if (!servicesLoaded) {
            getServices();
        }

        if (!tagsLoaded) {
            getTags();
        }
    }, [isRedEyeActive, pageLoaded, servicesLoaded, tagsLoaded]);

    // WebSocket connection for real-time packet updates
    useEffect(() => {
        if (isRedEyeActive) {
            return; // Don't connect if RedEye is not active
        }

        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/api/ws`;
        const ws = new WebSocket(wsUrl);
        wsRef.current = ws;

        ws.onopen = () => {
            console.log('WebSocket connected');
        };

        ws.onmessage = async (event) => {
            try {
                const packetEvent: PacketEvent = JSON.parse(event.data);
                console.log('Received packet event:', packetEvent);
                
                // Update streams list when new packet arrives
                // This will refresh the stream list to show updated timestamps and move active streams to top
                if (!isRedEyeActive) {
                    setCurrentOffset(0);
                    setHasMoreStreams(true);
                    getFilteredStreams(true);
                }
            } catch (error) {
                console.error('Error parsing WebSocket message:', error);
            }
        };

        ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };

        ws.onclose = () => {
            console.log('WebSocket disconnected, reconnecting...');
            // Reconnect after 3 seconds
            setTimeout(() => {
                if (!isRedEyeActive) {
                    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                    const wsUrl = `${protocol}//${window.location.host}/api/ws`;
                    wsRef.current = new WebSocket(wsUrl);
                }
            }, 3000);
        };

        return () => {
            if (wsRef.current) {
                wsRef.current.close();
                wsRef.current = null;
            }
        };
    }, [isRedEyeActive]);

    // Determine if merge should be active
    const shouldMerge = (() => {
        if (mergeOverride !== null) return mergeOverride;
        // Auto: merge for HTTP services, or if service has merge_adjacent_packets
        if (selectedStreamL7 === 'http1') return true;
        // Check service flags from the selected stream
        if (selectedStreamId) {
            const numId = typeof selectedStreamId === 'string' ? parseInt(selectedStreamId as string) : selectedStreamId;
            const stream = streamsArray.find((s: any) => s.id === numId);
            if (stream) {
                const svc = servicesArray.find((s: any) => s.id === stream.service_id);
                if (svc?.merge_adjacent_packets) return true;
            }
        }
        return false;
    })();

    // Merge adjacent packets with the same direction (when enabled)
    const mergedPackets = (() => {
        if (!packetData || packetData.length === 0) return [];
        if (!shouldMerge) return packetData.map(p => ({ ...p, _merged_count: 1 }));
        const groups: any[] = [];
        for (const pkt of packetData) {
            const last = groups[groups.length - 1];
            if (last && last.direction === pkt.direction) {
                const a = atob(last.packet_data);
                const b = atob(pkt.packet_data);
                const merged = new Uint8Array(a.length + b.length);
                for (let i = 0; i < a.length; i++) merged[i] = a.charCodeAt(i);
                for (let i = 0; i < b.length; i++) merged[a.length + i] = b.charCodeAt(i);
                let binary = '';
                for (let i = 0; i < merged.length; i++) binary += String.fromCharCode(merged[i]);
                last.packet_data = btoa(binary);
                last._merged_count = (last._merged_count || 1) + 1;
            } else {
                groups.push({ ...pkt, _merged_count: 1 });
            }
        }
        return groups;
    })();

    // Filter streams by header search results
    const filteredStreams = headerSearchResults
        ? streamsArray.filter((s: any) => headerSearchResults.stream_ids.includes(s.id))
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

    const handleStreamClick = async (streamId) => {
        selectedStreamIdRef.current = streamId;
        setSelectedStreamId(streamId);
        setMergeOverride(null); // Reset to auto on stream change

        // Check stream's L7 protocol (streamId comes as string from dataset)
        const numericId = typeof streamId === 'string' ? parseInt(streamId) : streamId;
        const stream = streamsArray.find((s: any) => s.id === numericId);
        setSelectedStreamL7(stream?.l7_protocol || 'unknown');

        try {
            const data = await getUrl(`/api/packet/stream_id/${streamId}`);
            const packets = Object.values(data);
            console.log('Loaded packets for stream:', packets.length);
            setPacketData(packets);
        } catch (error) {
            console.error('Ошибка при получении данных пакета:', error);
        }
    };

    const handleStreamNavigate = (streamId: number) => {
        handleStreamClick(streamId);
        // Scroll to stream in list
        const streamEl = document.querySelector(`[data-stream-id="${streamId}"]`);
        if (streamEl) streamEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
    };

    const handleFavoriteToggle = async (streamId, currentFavorite) => {
        try {
            const response = await fetch(`/api/stream/${streamId}/favorite`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ favorite: !currentFavorite })
            });

            if (!response.ok) {
                console.error('Не удалось обновить избранное:', await response.text());
                return;
            }

            setStreams((prevStreams: any[]) =>
                prevStreams.map((stream: any) =>
                    stream.id === streamId ? { ...stream, favorite: !currentFavorite } : stream
                )
            );
        } catch (error) {
            console.error('Ошибка при обновлении избранного:', error);
        }
    };

    // Share: copy link + auto-favorite
    const shareStream = async () => {
        if (!selectedStreamId) return;
        const url = `${window.location.origin}/packets?stream=${selectedStreamId}`;
        copyText(url);
        // Auto-favorite so it survives cleanup
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
        // Parse merged packets for HTTP messages
        for (const pkt of mergedPackets) {
            if (pkt.direction !== 'dest') continue;
            const text = atob(pkt.packet_data);
            const methodMatch = text.match(/^(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+(\S+)\s+HTTP/);
            if (!methodMatch) continue;
            const [, method, path] = methodMatch;
            // Extract headers
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

    const [shareCopied, setShareCopied] = useState(false);
    const [exportMenuOpen, setExportMenuOpen] = useState(false);

    const handleClearStreams = () => {
        setStreams([]);
        setCurrentOffset(0);
        setHasMoreStreams(true);
    };
    const handleTagClick = (e) => {
        let tag_name = e.target.dataset.service;
        let tmp = usedTags;
        if(usedTags.includes(tag_name))
        {
            let idx_tag = tmp.indexOf(tag_name); 
            tmp.splice(idx_tag, 1); 
            console.log("NEW ARRAY:", tmp);
        }
        else
        {
            tmp.push(e.target.dataset.service);
        }
        setUsedTags(tmp);
        setCurrentOffset(0);
        setHasMoreStreams(true);
        getFilteredStreams(true);
    };
    const handleServiceClick = (e) => {
        let service_name = e.target.dataset.service;
        let tmp = usedServices;
        if(usedServices.includes(service_name))
        {
            let idx_tag = tmp.indexOf(service_name); 
            tmp.splice(idx_tag, 1); 
            console.log("NEW ARRAY:", tmp);
        }
        else
        {
            tmp.push(e.target.dataset.service);
        }
        setUsedServices(tmp);
        setCurrentOffset(0);
        setHasMoreStreams(true);
        getFilteredStreams(true);
    };

    return (
        <div className="packetsPage">
            <div className="filterTape">
                <button className="clear-button" onClick={handleClearStreams}>
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
            <div id="streams" className="streams" ref={streamsContainerRef}>
                {/* Filter indicator */}
                {(usedTags.length > 0 || usedServices.length > 0 || headerFilters.length > 0) && (
                    <div className="filter-indicator">
                        <span>Фильтр активен</span>
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
                {filteredStreams.map((stream, index) => {
                    return < StreamCard
                        key={stream.id}
                        streamObj={stream}
                        onStreamClick={handleStreamClick}
                        onFavoriteToggle={handleFavoriteToggle}
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
            </div>
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
                            fetch('/api/tag/names', {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({
                                    name: `block-${pattern.slice(0, 20)}`,
                                    color: 0,
                                    template_type: 'substr',
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