import { useEffect, useRef, useState, useCallback } from 'react';
import { queryClient } from './queryClient';
import { subscribeWs, type WsState } from './wsBus';
import {
    isActivityFrame,
    mergeActivityFrames,
    type ActivityFrame,
    type StreamPatch,
} from './activityEvents';

export type { WsState } from './wsBus';

/** One flush of buffered activity frames, already merged per stream. */
export interface ActivityFlush {
    /** Additive per-stream count deltas (see activityEvents.ts). */
    patches: Map<number, StreamPatch>;
    /** First-seen stream ids from this flush (for the «N новых» plaque). */
    newStreamIds: number[];
}

const WS_BATCH_INTERVAL_MS = 300;
const WS_BATCH_MAX_SIZE = 50;
// new_streams → 'streams' query invalidation is throttled: frames arrive at
// most once per second, and refetching the whole list more often than every
// 2s is wasted work — per-window count deltas already keep the list fresh
// between refetches.
const STREAMS_INVALIDATE_THROTTLE_MS = 2000;

export function useWebSocket(
    enabled: boolean,
    onActivity?: (_flush: ActivityFlush) => void,
    selectedStreamIds?: number[],
) {
    const [state, setState] = useState<WsState>('disconnected');
    const bufferRef = useRef<ActivityFrame[]>([]);
    const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const onActivityRef = useRef(onActivity);
    const selectedStreamIdsRef = useRef(selectedStreamIds);
    const lastStreamsInvalidateRef = useRef(0);

    onActivityRef.current = onActivity;
    selectedStreamIdsRef.current = selectedStreamIds;

    const flush = useCallback(() => {
        const frames = bufferRef.current;
        bufferRef.current = [];
        if (timerRef.current) { clearTimeout(timerRef.current); timerRef.current = null; }
        if (frames.length === 0) return;

        const { patches, hasNewStreams, newStreamIds, packetsAvailable } = mergeActivityFrames(frames);

        // Additive patch: the server sends per-second count DELTAS, the
        // consumer adds them to the displayed counts (authoritative totals
        // come from the 'streams' query refetch below). newStreamIds (and
        // deltas for streams not on the current page) feed the «N новых»
        // plaque — see streamsFreeze.ts.
        if ((patches.size > 0 || newStreamIds.length > 0) && onActivityRef.current) {
            onActivityRef.current({ patches, newStreamIds });
        }

        if (hasNewStreams) {
            const now = Date.now();
            if (now - lastStreamsInvalidateRef.current >= STREAMS_INVALIDATE_THROTTLE_MS) {
                lastStreamsInvalidateRef.current = now;
                queryClient.invalidateQueries({ queryKey: ['streams'] });
            }
        }

        // Server does not know which streams the tab has open — it reports
        // every stream with fresh packets; only invalidate the selected ones.
        for (const id of packetsAvailable) {
            if (selectedStreamIdsRef.current?.includes(id)) {
                queryClient.invalidateQueries({ queryKey: ['packets', id] });
            }
        }
    }, []);

    const scheduleFlush = useCallback(() => {
        if (!timerRef.current) {
            timerRef.current = setTimeout(flush, WS_BATCH_INTERVAL_MS);
        }
    }, [flush]);

    useEffect(() => {
        if (!enabled) return;

        // All consumers share ONE socket per tab via the bus (see wsBus.ts).
        // Unsubscribing here drops our refcount; when it hits zero the bus
        // closes the socket and cannot reconnect — no zombie sockets after
        // Live toggles or page switches.
        const unsubscribe = subscribeWs({
            onMessage: (data) => {
                // Only activity frames are consumed here. Alert frames ride
                // the same bus and are handled by their own subscribers
                // (AlertToast); anything else is ignored.
                if (!isActivityFrame(data)) return;
                bufferRef.current.push(data);
                if (bufferRef.current.length >= WS_BATCH_MAX_SIZE) {
                    flush();
                } else {
                    scheduleFlush();
                }
            },
            onState: setState,
        });

        return () => {
            unsubscribe();
            if (timerRef.current) { clearTimeout(timerRef.current); timerRef.current = null; }
            bufferRef.current = [];
        };
    }, [enabled, flush, scheduleFlush]);

    return state;
}
