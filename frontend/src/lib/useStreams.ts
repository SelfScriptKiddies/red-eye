import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from 'react';
import { apiFetch } from './api';
import { freezeReducer, initialFreezeState } from './streamsFreeze';
import type { ActivityFlush } from './useWebSocket';

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

interface StreamItem {
    id: number;
    last_packet_time?: number;
    packet_count?: number;
    [key: string]: unknown;
}

function buildUrl(cursor: string | null, tags: string[], services: string[], startTime?: number | null, endTime?: number | null) {
    // Keyset pagination: page 1 has no cursor; loadMore continues strictly
    // older than the last visible row ("last_packet_time:id"), so a page
    // fetched while traffic flows never re-serves rows the list already has
    // (the old OFFSET path shifted with every last_time update).
    let url = `/api/stream?limit=${PAGE_SIZE}${cursor ? `&cursor=${cursor}` : '&offset=0'}`;
    if (tags.length > 0) url += `&tags=${tags.join(',')}`;
    if (services.length > 0) url += `&services=${services.join(',')}`;
    if (startTime != null) url += `&start_time=${startTime}`;
    if (endTime != null) url += `&end_time=${endTime}`;
    return url;
}

const cursorOf = (item: StreamItem): string => `${item.last_packet_time ?? 0}:${item.id}`;

export function useStreams({ tags, services, startTime, endTime, enabled = true, refetchInterval = false }: UseStreamsOptions) {
    const queryClient = useQueryClient();
    const [freeze, dispatch] = useReducer(freezeReducer<StreamItem>, undefined, initialFreezeState<StreamItem>);
    const [hasMoreStreams, setHasMoreStreams] = useState(true);
    const loadingMoreRef = useRef(false);

    // Identity of the filters the query runs under. The freeze reducer
    // applies data wholesale when this changes (filters switched) and
    // freezes the visible snapshot otherwise.
    const queryKey = useMemo(() => JSON.stringify({ tags, services, startTime, endTime }), [tags, services, startTime, endTime]);

    const { data: queryData, isLoading: isLoadingMore } = useQuery({
        queryKey: ['streams', { tags, services, startTime, endTime, offset: 0 }],
        queryFn: ({ signal }) => apiFetch<StreamItem[]>(buildUrl(null, tags, services, startTime, endTime), signal),
        enabled,
        refetchInterval,
        // Window-focus refetches are deliberately kept: the frozen view
        // (streamsFreeze.ts) decides what the user sees — a background
        // refetch can only refresh visible rows in place or grow the
        // «N новых» plaque, never reorder the list or yank the scroll.
        refetchOnWindowFocus: true,
    });

    useEffect(() => {
        if (queryData) {
            const items = (Array.isArray(queryData) ? queryData : Object.values(queryData)) as StreamItem[];
            dispatch({ type: 'query_data', data: items, queryKey, maxVisible: MAX_STREAMS });
        }
    }, [queryData, queryKey]);

    // loadMore pages BEHIND the frozen snapshot tail via a keyset cursor.
    const loadMore = useCallback(async () => {
        if (!enabled || !hasMoreStreams || loadingMoreRef.current) return;
        const last = freeze.visible[freeze.visible.length - 1];
        if (!last) return;
        loadingMoreRef.current = true;
        try {
            const data = await apiFetch<StreamItem[]>(buildUrl(cursorOf(last), tags, services, startTime, endTime));
            if (data) {
                const items = (Array.isArray(data) ? data : Object.values(data)) as StreamItem[];
                dispatch({ type: 'append', items, maxVisible: MAX_STREAMS });
                setHasMoreStreams(items.length === PAGE_SIZE);
            }
        } finally {
            loadingMoreRef.current = false;
        }
    }, [enabled, hasMoreStreams, freeze.visible, tags, services, startTime, endTime]);

    const refresh = useCallback(() => {
        queryClient.invalidateQueries({ queryKey: ['streams'] });
    }, [queryClient]);

    // Unfreeze — the pending count clears immediately, the next refetch
    // (forced below, even when the query is disabled) replaces the snapshot
    // wholesale. `scroll=false` is used by Live's auto-apply so the batch
    // refresh does not yank the view to the top.
    const acceptNew = useCallback((scroll = true) => {
        dispatch({ type: 'accept', scroll });
        setHasMoreStreams(true);
        queryClient.refetchQueries({ queryKey: ['streams'], type: 'all' });
    }, [queryClient]);

    const updateStream = useCallback((streamId: number, updater: (_s: StreamItem) => StreamItem) => {
        dispatch({ type: 'update_stream', id: streamId, updater });
    }, []);

    // WS activity flush: additive count deltas for visible streams +
    // off-page deltas / first-seen ids accumulating into the plaque.
    const onActivity = useCallback((flush: ActivityFlush) => {
        dispatch({ type: 'activity', patches: flush.patches, newStreamIds: flush.newStreamIds });
    }, []);

    const clearStreams = useCallback(() => {
        dispatch({ type: 'reset' });
        setHasMoreStreams(true);
    }, []);

    return {
        streams: freeze.visible,
        newStreamsCount: freeze.pendingNewIds.length,
        scrollToken: freeze.scrollToken,
        isLoadingMore, hasMoreStreams, refresh, updateStream, clearStreams, loadMore,
        acceptNew, onActivity,
    };
}
