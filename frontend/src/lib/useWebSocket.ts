import { useEffect, useRef, useState, useCallback } from 'react';
import { queryClient } from './queryClient';

type WsState = 'connected' | 'connecting' | 'disconnected';

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
    onPatchStreams?: (patches: Map<number, Partial<{ count: number; last_time: number }>>) => void,
    selectedStreamIds?: number[],
) {
    const [state, setState] = useState<WsState>('disconnected');
    const wsRef = useRef<WebSocket | null>(null);
    const bufferRef = useRef<WsEvent[]>([]);
    const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const reconnectRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const attemptRef = useRef(0);

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
                if (selectedStreamIds?.includes(evt.stream_id)) {
                    invalidateKeys.add(`packets-${evt.stream_id}`);
                }
            } else if (evt.type === 'new_stream') {
                invalidateKeys.add('streams');
            }
        }

        if (patches.size > 0 && onPatchStreams) {
            onPatchStreams(patches);
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
    }, [onPatchStreams, selectedStreamIds]);

    const scheduleFlush = useCallback(() => {
        if (!timerRef.current) {
            timerRef.current = setTimeout(flush, WS_BATCH_INTERVAL_MS);
        }
    }, [flush]);

    const connect = useCallback(() => {
        if (!enabled) return;
        setState('connecting');
        const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const ws = new WebSocket(`${proto}//${window.location.host}/api/ws`);
        wsRef.current = ws;

        ws.onopen = () => {
            setState('connected');
            attemptRef.current = 0;
        };

        ws.onmessage = (msg) => {
            try {
                const event: WsEvent = JSON.parse(msg.data);
                bufferRef.current.push(event);
                if (bufferRef.current.length >= WS_BATCH_MAX_SIZE) {
                    flush();
                } else {
                    scheduleFlush();
                }
            } catch {}
        };

        ws.onclose = () => {
            setState('disconnected');
            wsRef.current = null;
            if (enabled) {
                const delay = Math.min(1000 * 2 ** attemptRef.current, 30000);
                attemptRef.current++;
                reconnectRef.current = setTimeout(connect, delay);
            }
        };
    }, [enabled, flush, scheduleFlush]);

    useEffect(() => {
        if (enabled) { connect(); }
        return () => {
            wsRef.current?.close();
            if (reconnectRef.current) clearTimeout(reconnectRef.current);
            if (timerRef.current) clearTimeout(timerRef.current);
        };
    }, [enabled, connect]);

    return state;
}
