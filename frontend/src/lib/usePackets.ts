import { useState, useMemo, useCallback } from 'react';
import { fetchApi } from './api';

const MAX_PACKETS = 500;

export function usePackets(streamsArray: any[], servicesArray: any[]) {
    const [packetData, setPacketData] = useState<any[]>([]);
    const [selectedStreamId, setSelectedStreamId] = useState<number | null>(null);
    const [selectedStreamL7, setSelectedStreamL7] = useState<string>('unknown');
    const [mergeOverride, setMergeOverride] = useState<boolean | null>(null);

    const handleStreamClick = useCallback(async (streamId: number) => {
        setSelectedStreamId(streamId);
        setMergeOverride(null);
        const numericId = typeof streamId === 'string' ? parseInt(streamId as any) : streamId;
        const localStream = streamsArray.find((s: any) => s.id === numericId);
        if (localStream?.l7_protocol) {
            setSelectedStreamL7(localStream.l7_protocol);
        } else {
            try {
                const streamData = await fetchApi(`/api/stream/${numericId}`);
                setSelectedStreamL7(streamData?.l7_protocol || 'unknown');
            } catch { setSelectedStreamL7('unknown'); }
        }
        try {
            const data = await fetchApi(`/api/packet/stream_id/${streamId}`);
            if (data) {
                const packets = Object.values(data);
                setPacketData(packets.slice(0, MAX_PACKETS) as any[]);
            }
        } catch (error) { console.error('Error fetching packets:', error); }
    }, [streamsArray]);

    const shouldMerge = (() => {
        if (mergeOverride !== null) return mergeOverride;
        if (selectedStreamL7 === 'http1') return true;
        if (selectedStreamId) {
            const numId = typeof selectedStreamId === 'string' ? parseInt(selectedStreamId as any) : selectedStreamId;
            const stream = streamsArray.find((s: any) => s.id === numId);
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
    }, [packetData, shouldMerge]);

    return { packetData, selectedStreamId, selectedStreamL7, mergeOverride, shouldMerge, mergedPackets, setMergeOverride, handleStreamClick };
}
