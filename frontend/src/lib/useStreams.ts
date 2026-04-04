import { useState, useEffect, useRef, useCallback } from 'react';
import { fetchApi } from './api';

const MAX_STREAMS = 200;
const PAGE_SIZE = 20;

interface UseStreamsOptions {
    tags: string[];
    services: string[];
    startTime?: number | null;
    endTime?: number | null;
    enabled?: boolean;
}

export function useStreams({ tags, services, startTime, endTime, enabled = true }: UseStreamsOptions) {
    const [streams, setStreams] = useState<any[]>([]);
    const [currentOffset, setCurrentOffset] = useState(0);
    const [isLoadingMore, setIsLoadingMore] = useState(false);
    const [hasMoreStreams, setHasMoreStreams] = useState(true);
    const [initialLoaded, setInitialLoaded] = useState(false);
    const scrollRef = useRef<HTMLDivElement | null>(null);

    const buildUrl = useCallback((offset: number) => {
        let url = `/api/stream?limit=${PAGE_SIZE}&offset=${offset}`;
        if (tags.length > 0) url += `&tags=${tags.join(',')}`;
        if (services.length > 0) url += `&services=${services.join(',')}`;
        if (startTime != null) url += `&start_time=${startTime}`;
        if (endTime != null) url += `&end_time=${endTime}`;
        return url;
    }, [tags, services, startTime, endTime]);

    const loadStreams = useCallback(async (reset: boolean) => {
        if (!enabled) return;
        const offset = reset ? 0 : currentOffset;
        setIsLoadingMore(true);
        try {
            const data = await fetchApi<any[]>(buildUrl(offset));
            if (!data) return;
            const newStreams = Object.values(data);
            if (reset) {
                setStreams(newStreams.slice(0, MAX_STREAMS));
                setCurrentOffset(PAGE_SIZE);
            } else {
                setStreams(prev => [...prev, ...newStreams].slice(0, MAX_STREAMS));
                setCurrentOffset(prev => prev + PAGE_SIZE);
            }
            setHasMoreStreams(newStreams.length === PAGE_SIZE);
        } catch (error) {
            console.error('Error loading streams:', error);
        } finally {
            setIsLoadingMore(false);
        }
    }, [enabled, currentOffset, buildUrl]);

    // Initial load
    useEffect(() => {
        if (!enabled || initialLoaded) return;
        setInitialLoaded(true);
        loadStreams(true);
    }, [enabled]);

    // Reload when filters change
    const filtersVersion = useRef(0);
    useEffect(() => {
        if (filtersVersion.current === 0) { filtersVersion.current = 1; return; }
        setCurrentOffset(0);
        setHasMoreStreams(true);
        loadStreams(true);
    }, [tags, services, startTime, endTime]);

    // Infinite scroll
    useEffect(() => {
        const container = scrollRef.current;
        if (!container) return;
        const handleScroll = () => {
            if (isLoadingMore || !hasMoreStreams || !enabled) return;
            const { scrollTop, scrollHeight, clientHeight } = container;
            if (scrollTop + clientHeight >= scrollHeight - 200) loadStreams(false);
        };
        container.addEventListener('scroll', handleScroll);
        return () => container.removeEventListener('scroll', handleScroll);
    }, [isLoadingMore, hasMoreStreams, enabled, loadStreams]);

    const refresh = useCallback(() => {
        setCurrentOffset(0);
        setHasMoreStreams(true);
        loadStreams(true);
    }, [loadStreams]);

    const updateStream = useCallback((streamId: number, updater: (s: any) => any) => {
        setStreams(prev => prev.map(s => s.id === streamId ? updater(s) : s));
    }, []);

    const clearStreams = useCallback(() => {
        setStreams([]);
        setCurrentOffset(0);
        setHasMoreStreams(true);
    }, []);

    return { streams, isLoadingMore, hasMoreStreams, scrollRef, refresh, updateStream, clearStreams, loadStreams };
}
