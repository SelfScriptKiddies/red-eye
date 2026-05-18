import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useCallback, useRef, useState } from 'react';
import { apiFetch } from './api';

const MAX_STREAMS = 200;
const PAGE_SIZE = 20;

interface UseStreamsOptions {
    tags: string[];
    services: string[];
    startTime?: number | null;
    endTime?: number | null;
    enabled?: boolean;
    refetchInterval?: number | false;
}

function buildUrl(offset: number, tags: string[], services: string[], startTime?: number | null, endTime?: number | null) {
    let url = `/api/stream?limit=${PAGE_SIZE}&offset=${offset}`;
    if (tags.length > 0) url += `&tags=${tags.join(',')}`;
    if (services.length > 0) url += `&services=${services.join(',')}`;
    if (startTime != null) url += `&start_time=${startTime}`;
    if (endTime != null) url += `&end_time=${endTime}`;
    return url;
}

export function useStreams({ tags, services, startTime, endTime, enabled = true, refetchInterval = false }: UseStreamsOptions) {
    const queryClient = useQueryClient();
    const [allStreams, setAllStreams] = useState<any[]>([]);
    const [currentOffset, setCurrentOffset] = useState(0);
    const [hasMoreStreams, setHasMoreStreams] = useState(true);
    const scrollRef = useRef<HTMLDivElement | null>(null);

    const { isLoading: isLoadingMore } = useQuery({
        queryKey: ['streams', { tags, services, startTime, endTime, offset: 0 }],
        queryFn: ({ signal }) => apiFetch<any[]>(buildUrl(0, tags, services, startTime, endTime), signal),
        enabled,
        refetchInterval,
        select: (data) => {
            if (data) {
                const newStreams = Object.values(data);
                setAllStreams(newStreams.slice(0, MAX_STREAMS));
                setCurrentOffset(PAGE_SIZE);
                setHasMoreStreams(newStreams.length === PAGE_SIZE);
            }
            return data;
        },
    });

    const loadMore = useCallback(async () => {
        if (!enabled || !hasMoreStreams) return;
        const data = await apiFetch<any[]>(buildUrl(currentOffset, tags, services, startTime, endTime));
        if (!data) return;
        const newStreams = Object.values(data);
        setAllStreams(prev => [...prev, ...newStreams].slice(0, MAX_STREAMS));
        setCurrentOffset(prev => prev + PAGE_SIZE);
        setHasMoreStreams(newStreams.length === PAGE_SIZE);
    }, [enabled, hasMoreStreams, currentOffset, tags, services, startTime, endTime]);

    const refresh = useCallback(() => {
        queryClient.invalidateQueries({ queryKey: ['streams'] });
    }, [queryClient]);

    const updateStream = useCallback((streamId: number, updater: (s: any) => any) => {
        setAllStreams(prev => prev.map(s => s.id === streamId ? updater(s) : s));
    }, []);

    const patchStreams = useCallback((patches: Map<number, any>) => {
        setAllStreams(prev => prev.map(s => {
            const patch = patches.get(s.id);
            if (patch) {
                return { ...s, packet_count: patch.count ?? s.packet_count, last_time: patch.last_time ?? s.last_time };
            }
            return s;
        }));
    }, []);

    const clearStreams = useCallback(() => {
        setAllStreams([]);
        setCurrentOffset(0);
        setHasMoreStreams(true);
    }, []);

    return {
        streams: allStreams, isLoadingMore, hasMoreStreams, scrollRef,
        refresh, updateStream, clearStreams, loadMore, patchStreams,
    };
}
