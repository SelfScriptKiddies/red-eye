// Pure helpers for the backend's ONE-frame-per-second WebSocket activity
// protocol (backend/src/ws_activity.rs). The server aggregates per-packet
// NATS events into:
//
//   {"type":"activity",
//    "streams":[{"id":123,"count":7,"last_time":1791464509}],
//    "new_streams":[456],
//    "packets_available":[123,789]}
//
// `count` is the stream's packet-count DELTA of that window (not the
// total), so the displayed count must be PATCHED ADDITIVELY; the
// react-query `streams` refetch (invalidated by `new_streams`) brings
// authoritative totals and replaces local state wholesale.

export interface ActivityStreamDelta {
    id: number;
    count: number;
    last_time: number;
}

export interface ActivityFrame {
    type: 'activity';
    streams?: ActivityStreamDelta[];
    new_streams?: number[];
    packets_available?: number[];
}

export interface StreamPatch {
    /** Packets added since the previous frame — ADD to the displayed count. */
    count_delta: number;
    /** Latest packet timestamp (seconds) — replaces the displayed value. */
    last_time?: number;
}

export interface ActivityEffects {
    /** Per-stream patches, deltas merged across all frames of one flush. */
    patches: Map<number, StreamPatch>;
    /** True when any frame reported first-seen stream ids. */
    hasNewStreams: boolean;
    /** Stream ids that received packets (union across frames). */
    packetsAvailable: number[];
}

export function isActivityFrame(data: unknown): data is ActivityFrame {
    return (
        typeof data === 'object' &&
        data !== null &&
        (data as { type?: unknown }).type === 'activity'
    );
}

/**
 * Merge the activity frames accumulated in one flush buffer into a single
 * effect set. Deltas for the same stream sum up (a reconnect or a burst can
 * deliver several frames between flushes); `last_time` keeps the max;
 * `packets_available` is deduplicated.
 */
export function mergeActivityFrames(frames: ActivityFrame[]): ActivityEffects {
    const patches = new Map<number, StreamPatch>();
    let hasNewStreams = false;
    const packetsAvailable = new Set<number>();

    for (const frame of frames) {
        for (const s of frame.streams ?? []) {
            const existing = patches.get(s.id);
            if (existing) {
                existing.count_delta += s.count;
                if (s.last_time > (existing.last_time ?? 0)) {
                    existing.last_time = s.last_time;
                }
            } else {
                patches.set(s.id, { count_delta: s.count, last_time: s.last_time });
            }
        }
        if ((frame.new_streams ?? []).length > 0) {
            hasNewStreams = true;
        }
        for (const id of frame.packets_available ?? []) {
            packetsAvailable.add(id);
        }
    }

    return { patches, hasNewStreams, packetsAvailable: [...packetsAvailable] };
}

/**
 * Apply additive count patches to a streams array (as displayed by the list
 * UI). Returns the SAME array reference when no patch matches any stream —
 * React state setters then bail out of a re-render.
 */
export function applyStreamPatches<
    T extends { id: number; packet_count?: number; last_time?: number },
>(streams: T[], patches: Map<number, StreamPatch>): T[] {
    if (patches.size === 0) return streams;
    let changed = false;
    const next = streams.map((s) => {
        const patch = patches.get(s.id);
        if (!patch) return s;
        changed = true;
        return {
            ...s,
            packet_count: (s.packet_count ?? 0) + (patch.count_delta ?? 0),
            last_time: patch.last_time ?? s.last_time,
        };
    });
    return changed ? next : streams;
}
