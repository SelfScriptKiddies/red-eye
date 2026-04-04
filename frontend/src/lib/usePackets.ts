import { useState, useMemo, useCallback } from 'react';
import { fetchApi } from './api';

const MAX_PACKETS = 500;

export function usePackets(streamsArray: any[], servicesArray: any[]) {
    const [packetsByStream, setPacketsByStream] = useState<Record<number, any[]>>({});
    const [selectedStreamIds, setSelectedStreamIds] = useState<number[]>([]);
    const [mergeOverride, setMergeOverride] = useState<boolean | null>(null);

    const selectedStreamId = selectedStreamIds.length >= 1 ? selectedStreamIds[0] : null;
    const isMultiSelect = selectedStreamIds.length > 1;

    // Derive L7 protocol from all selected streams — if all same, use it; if mixed, 'unknown'
    const selectedStreamL7 = useMemo(() => {
        if (selectedStreamIds.length === 0) return 'unknown';
        const protocols = selectedStreamIds.map(id => {
            const s = streamsArray.find((s: any) => s.id === id);
            return s?.l7_protocol || 'unknown';
        });
        const unique = [...new Set(protocols)];
        return unique.length === 1 ? unique[0] : 'unknown';
    }, [selectedStreamIds, streamsArray]);

    const loadPacketsForStream = useCallback(async (streamId: number): Promise<any[]> => {
        try {
            const data = await fetchApi(`/api/packet/stream_id/${streamId}`);
            if (data) {
                const packets = Object.values(data) as any[];
                return packets.slice(0, MAX_PACKETS).map(p => ({ ...p, _stream_id: streamId }));
            }
        } catch (error) { console.error('Error fetching packets:', error); }
        return [];
    }, []);

    const handleStreamClick = useCallback(async (streamId: number | string, event?: any) => {
        const numericId = typeof streamId === 'string' ? parseInt(streamId) : streamId;
        const shiftKey = event?.shiftKey ?? false;

        if (shiftKey && selectedStreamIds.length > 0) {
            // Shift+click: toggle stream in multi-selection
            const alreadySelected = selectedStreamIds.includes(numericId);
            if (alreadySelected) {
                const newIds = selectedStreamIds.filter(id => id !== numericId);
                setSelectedStreamIds(newIds);
                setPacketsByStream(prev => {
                    const next = { ...prev };
                    delete next[numericId];
                    return next;
                });
            } else {
                setSelectedStreamIds(prev => [...prev, numericId]);
                const packets = await loadPacketsForStream(numericId);
                setPacketsByStream(prev => ({ ...prev, [numericId]: packets }));
            }
            setMergeOverride(null);
        } else {
            // Normal click: single select
            setSelectedStreamIds([numericId]);
            setMergeOverride(null);
            const packets = await loadPacketsForStream(numericId);
            setPacketsByStream({ [numericId]: packets });
        }
    }, [streamsArray, selectedStreamIds, loadPacketsForStream]);

    // Combine packets — single stream keeps server order, multi-select sorts by time+id
    const packetData = useMemo(() => {
        if (selectedStreamIds.length === 1) {
            return (packetsByStream[selectedStreamIds[0]] ?? []).slice(0, MAX_PACKETS);
        }
        const allPackets: any[] = [];
        for (const id of selectedStreamIds) {
            const packets = packetsByStream[id];
            if (packets) allPackets.push(...packets);
        }
        allPackets.sort((a, b) => {
            const tsDiff = (a.timestamp ?? 0) - (b.timestamp ?? 0);
            if (tsDiff !== 0) return tsDiff;
            return (a.id ?? 0) - (b.id ?? 0);
        });
        return allPackets.slice(0, MAX_PACKETS);
    }, [packetsByStream, selectedStreamIds]);

    const shouldMerge = (() => {
        if (isMultiSelect) return false; // Don't merge across streams
        if (mergeOverride !== null) return mergeOverride;
        if (selectedStreamL7 === 'http1') return true;
        if (selectedStreamId) {
            const stream = streamsArray.find((s: any) => s.id === selectedStreamId);
            if (stream) {
                const svc = servicesArray.find((s: any) => s.id === stream.service_id);
                if (svc?.merge_adjacent_packets) return true;
            }
        }
        return false;
    })();

    const mergedPackets = useMemo(() => {
        if (!packetData || packetData.length === 0) return [];
        if (!shouldMerge) return packetData.map(p => ({ ...p, _merged_count: 1 }));
        const groups: any[] = [];
        for (const pkt of packetData) {
            const last = groups[groups.length - 1];
            if (last && last.direction === pkt.direction && last._stream_id === pkt._stream_id) {
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
    }, [packetData, shouldMerge]);

    return {
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
    };
}
