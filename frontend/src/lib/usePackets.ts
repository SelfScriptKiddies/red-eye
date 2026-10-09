import { useState, useMemo, useCallback } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { apiFetch } from './api';
import { mergeAdjacentPackets, splitPackets } from './mergePackets';

const MAX_PACKETS = 500;
const MAX_SELECTED_STREAMS = 10;

export function usePackets(streamsArray: any[], servicesArray: any[]) {
    const queryClient = useQueryClient();
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
            const data = await queryClient.fetchQuery({
                queryKey: ['packets', streamId],
                queryFn: () => apiFetch<any>(`/api/packet/stream_id/${streamId}`),
                staleTime: 3_000,
            });
            if (data) {
                const packets = Object.values(data) as any[];
                return packets.slice(0, MAX_PACKETS).map(p => ({ ...p, _stream_id: streamId }));
            }
        } catch (error) { console.error('Error fetching packets:', error); }
        return [];
    }, [queryClient]);

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
                // Evict oldest if at limit
                const updated = [...selectedStreamIds, numericId];
                if (updated.length > MAX_SELECTED_STREAMS) {
                    const evicted = updated[0];
                    setPacketsByStream(p => {
                        const next = { ...p };
                        delete next[evicted];
                        return next;
                    });
                    setSelectedStreamIds(updated.slice(1));
                } else {
                    setSelectedStreamIds(updated);
                }
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
    }, [selectedStreamIds, loadPacketsForStream]);

    // Load multiple streams at once (for shared URLs and periodic refreshes).
    // Deliberately does NOT touch mergeOverride: this runs from the Live
    // batch timer, and resetting the user's merge toggle every few seconds
    // made it look like merge "turns itself off".
    const loadMultipleStreams = useCallback(async (ids: number[]) => {
        if (ids.length === 0) return;
        const limited = ids.slice(0, MAX_SELECTED_STREAMS);
        setSelectedStreamIds(limited);
        const results: Record<number, any[]> = {};
        await Promise.all(limited.map(async (id) => {
            results[id] = await loadPacketsForStream(id);
        }));
        setPacketsByStream(results);
    }, [loadPacketsForStream]);

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
            const usDiff = (a.timestamp_microseconds ?? 0) - (b.timestamp_microseconds ?? 0);
            if (usDiff !== 0) return usDiff;
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
        // Pure, linear merge (lib/mergePackets.ts — unit-tested there);
        // the old inline loop re-encoded the accumulated payload per
        // appended packet, O(bytes × run) instead of O(bytes).
        return shouldMerge
            ? mergeAdjacentPackets(packetData)
            : splitPackets(packetData);
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
        loadMultipleStreams,
    };
}
