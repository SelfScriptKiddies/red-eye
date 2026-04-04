import { useEffect, useRef } from 'react';

const WS_THROTTLE_MS = 2000;

export function useRealtimeUpdates(onRefresh: () => void, enabled: boolean = true) {
    const wsRef = useRef<WebSocket | null>(null);
    const wsRefreshPending = useRef(false);
    const wsRefreshTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

    useEffect(() => {
        if (!enabled) return;
        let reconnectTimeout: ReturnType<typeof setTimeout>;

        const scheduleRefresh = () => {
            if (wsRefreshPending.current) return;
            wsRefreshPending.current = true;
            wsRefreshTimer.current = setTimeout(() => {
                wsRefreshPending.current = false;
                onRefresh();
            }, WS_THROTTLE_MS);
        };

        const connectWs = () => {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${window.location.host}/api/ws`;
            const ws = new WebSocket(wsUrl);
            wsRef.current = ws;
            ws.onmessage = () => { scheduleRefresh(); };
            ws.onclose = () => {
                reconnectTimeout = setTimeout(() => {
                    if (wsRef.current?.readyState === WebSocket.CLOSED) connectWs();
                }, 3000);
            };
        };

        connectWs();

        return () => {
            clearTimeout(reconnectTimeout);
            if (wsRefreshTimer.current) clearTimeout(wsRefreshTimer.current);
            if (wsRef.current) {
                wsRef.current.onclose = null;
                wsRef.current.close();
                wsRef.current = null;
            }
        };
    }, [enabled]);
}
