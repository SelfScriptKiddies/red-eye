import { useEffect, useRef, useState, useCallback } from 'react';
import { queryClient } from './queryClient';
import { subscribeWs, type WsState } from './wsBus';

export type { WsState } from './wsBus';

interface WsEvent {
    type: string;
    stream_id?: number;
    count?: number;
    last_time?: number;
    service_id?: number;
    service_name?: string;
    packet_id?: number;
    direction?: string;
}

const WS_BATCH_INTERVAL_MS = 300;
const WS_BATCH_MAX_SIZE = 50;

export function useWebSocket(
    enabled: boolean,
    onPatchStreams?: (_patches: Map<number, Partial<{ count: number; last_time: number }>>) => void,
    selectedStreamIds?: number[],
) {
    const [state, setState] = useState<WsState>('disconnected');
    const bufferRef = useRef<WsEvent[]>([]);
    const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const onPatchStreamsRef = useRef(onPatchStreams);
    const selectedStreamIdsRef = useRef(selectedStreamIds);

    onPatchStreamsRef.current = onPatchStreams;
    selectedStreamIdsRef.current = selectedStreamIds;

    const flush = useCallback(() => {
        const events = bufferRef.current;
        bufferRef.current = [];
        if (timerRef.current) { clearTimeout(timerRef.current); timerRef.current = null; }
        if (events.length === 0) return;

        const patches = new Map<number, { count?: number; last_time?: number }>();
        const invalidateKeys = new Set<string>();

        for (const evt of events) {
            if (evt.type === 'packet_count' && evt.stream_id != null) {
                patches.set(evt.stream_id, { count: evt.count, last_time: evt.last_time });
            } else if (evt.type === 'stream_tags_changed' && evt.stream_id != null) {
                invalidateKeys.add(`tags-${evt.stream_id}`);
                invalidateKeys.add('streams');
            } else if (evt.type === 'packets_available' && evt.stream_id != null) {
                if (selectedStreamIdsRef.current?.includes(evt.stream_id)) {
                    invalidateKeys.add(`packets-${evt.stream_id}`);
                }
            } else if (evt.type === 'new_stream') {
                invalidateKeys.add('streams');
            }
        }

        if (patches.size > 0 && onPatchStreamsRef.current) {
            onPatchStreamsRef.current(patches);
        }

        for (const key of invalidateKeys) {
            if (key === 'streams') {
                queryClient.invalidateQueries({ queryKey: ['streams'] });
            } else if (key.startsWith('tags-')) {
                const id = parseInt(key.split('-')[1]);
                queryClient.invalidateQueries({ queryKey: ['tags', id] });
            } else if (key.startsWith('packets-')) {
                const id = parseInt(key.split('-')[1]);
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
                const parsed = data as WsEvent & { events?: WsEvent[] };
                if (parsed && parsed.type === 'packet_batch') {
                    const events: WsEvent[] = parsed.events ?? [];
                    if (!Array.isArray(events) || events.length === 0) return;
                    for (const evt of events) bufferRef.current.push(evt);
                } else if (parsed) {
                    bufferRef.current.push(parsed);
                } else {
                    return;
                }
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
