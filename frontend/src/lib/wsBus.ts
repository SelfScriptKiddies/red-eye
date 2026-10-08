// Shared WebSocket bus for /api/ws: exactly ONE socket per browser tab.
//
// Why: every useWebSocket mount used to open its own socket and its onclose
// handler reconnected even after the owning component unmounted, leaking a
// socket (and its ~1000 frames/s at game load) on every Live toggle / page
// switch. AlertToast opened yet another parallel socket to the same endpoint.
//
// This module multiplexes all consumers onto a single refcounted connection.
// The last unsubscribe tears the socket down and — crucially — the close we
// initiate ourselves never schedules a reconnect. Genuine drops (server
// restart, network flaps) still reconnect with exponential backoff.

export type WsState = 'connected' | 'connecting' | 'disconnected';

export interface WsBusSubscription {
    /** Receives every parsed JSON frame from the shared socket. */
    onMessage: (_data: unknown) => void;
    /** Optional connection-state fan-out (called with the current state on subscribe). */
    onState?: (_state: WsState) => void;
}

type Listener = WsBusSubscription;

const listeners = new Set<Listener>();

let socket: WebSocket | null = null;
let busState: WsState = 'disconnected';
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
let reconnectAttempt = 0;
let intentionalClose = false;

function wsUrl(): string {
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${proto}//${window.location.host}/api/ws`;
}

function publishState(next: WsState) {
    busState = next;
    for (const listener of listeners) {
        listener.onState?.(next);
    }
}

function clearReconnectTimer() {
    if (reconnectTimer !== null) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
    }
}

// Full teardown: close the socket, detach handlers so no callback of ours can
// fire afterwards, and drop any pending reconnect. Called when the last
// subscriber leaves — reconnecting with zero consumers is the zombie leak.
function teardown() {
    clearReconnectTimer();
    reconnectAttempt = 0;
    if (socket !== null) {
        const dead = socket;
        socket = null;
        intentionalClose = true;
        // Detach handlers BEFORE close(): the onclose fired by our own close()
        // must never reach the reconnect path.
        dead.onopen = null;
        dead.onclose = null;
        dead.onmessage = null;
        try {
            dead.close();
        } catch {
            // already closing/closed — nothing to do
        }
    }
    publishState('disconnected');
}

function connect() {
    if (socket !== null || listeners.size === 0) return;
    intentionalClose = false;
    publishState('connecting');

    const ws = new WebSocket(wsUrl());
    socket = ws;

    ws.onopen = () => {
        if (socket !== ws) return;
        reconnectAttempt = 0;
        publishState('connected');
    };

    ws.onmessage = (msg) => {
        if (socket !== ws) return;
        let data: unknown;
        try {
            data = JSON.parse(msg.data as string);
        } catch {
            return;
        }
        for (const listener of [...listeners]) {
            listener.onMessage(data);
        }
    };

    ws.onclose = () => {
        if (socket !== ws) return; // stale event from a replaced/torn-down socket
        socket = null;
        publishState('disconnected');
        if (intentionalClose) return; // we closed it — never reconnect
        if (listeners.size === 0) return; // nobody is listening anymore
        // Genuine drop: retry with exponential backoff (1s → 30s cap).
        const delay = Math.min(1000 * 2 ** reconnectAttempt, 30000);
        reconnectAttempt++;
        reconnectTimer = setTimeout(connect, delay);
    };
}

/**
 * Subscribe to the shared /api/ws socket. The socket is opened on the first
 * subscription and closed when the last subscriber unsubscribes.
 *
 * @returns an unsubscribe function — MUST be called from effect cleanup.
 */
export function subscribeWs(sub: WsBusSubscription): () => void {
    listeners.add(sub);
    sub.onState?.(busState);
    if (listeners.size === 1) connect();
    return () => {
        listeners.delete(sub);
        if (listeners.size === 0) teardown();
    };
}
