import { describe, it, expect } from 'vitest';
import {
    isActivityFrame,
    mergeActivityFrames,
    applyStreamPatches,
    type ActivityFrame,
} from './activityEvents';

function frame(partial: Partial<ActivityFrame>): ActivityFrame {
    return { type: 'activity', ...partial };
}

describe('isActivityFrame', () => {
    it('accepts activity frames', () => {
        expect(isActivityFrame(frame({}))).toBe(true);
        expect(isActivityFrame({ type: 'activity', streams: [] })).toBe(true);
    });

    it('rejects non-activity payloads sharing the bus', () => {
        expect(isActivityFrame({ type: 'alert', tag_name: 'x' })).toBe(false);
        expect(isActivityFrame({ type: 'packet_batch', events: [] })).toBe(false);
        expect(isActivityFrame(null)).toBe(false);
        expect(isActivityFrame('activity')).toBe(false);
    });
});

describe('mergeActivityFrames', () => {
    it('turns one frame into per-stream patches', () => {
        const { patches, hasNewStreams, packetsAvailable } = mergeActivityFrames([
            frame({
                streams: [{ id: 123, count: 7, last_time: 1791464509 }],
                new_streams: [456],
                packets_available: [123, 456],
            }),
        ]);
        expect(patches.get(123)).toEqual({ count_delta: 7, last_time: 1791464509 });
        expect(patches.size).toBe(1);
        expect(hasNewStreams).toBe(true);
        expect(packetsAvailable).toEqual([123, 456]);
    });

    it('sums deltas and keeps max last_time across frames for the same stream', () => {
        const { patches } = mergeActivityFrames([
            frame({ streams: [{ id: 1, count: 3, last_time: 100 }] }),
            frame({ streams: [{ id: 1, count: 4, last_time: 90 }] }),
            frame({ streams: [{ id: 1, count: 1, last_time: 200 }] }),
        ]);
        expect(patches.get(1)).toEqual({ count_delta: 8, last_time: 200 });
    });

    it('keeps distinct streams apart', () => {
        const { patches, packetsAvailable } = mergeActivityFrames([
            frame({
                streams: [
                    { id: 7, count: 2, last_time: 10 },
                    { id: 9, count: 5, last_time: 11 },
                ],
                packets_available: [7, 9],
            }),
        ]);
        expect(patches.get(7)).toEqual({ count_delta: 2, last_time: 10 });
        expect(patches.get(9)).toEqual({ count_delta: 5, last_time: 11 });
        expect(packetsAvailable).toEqual([7, 9]);
    });

    it('unions and dedupes packets_available across frames', () => {
        const { packetsAvailable } = mergeActivityFrames([
            frame({ packets_available: [1, 2] }),
            frame({ packets_available: [2, 3] }),
        ]);
        expect(packetsAvailable).toEqual([1, 2, 3]);
    });

    it('flags new streams only when a frame actually lists them', () => {
        const without = mergeActivityFrames([
            frame({ streams: [{ id: 1, count: 1, last_time: 1 }], new_streams: [] }),
        ]);
        expect(without.hasNewStreams).toBe(false);

        const withNew = mergeActivityFrames([
            frame({ new_streams: [] }),
            frame({ new_streams: [5] }),
        ]);
        expect(withNew.hasNewStreams).toBe(true);
    });

    it('tolerates frames with missing arrays', () => {
        const { patches, hasNewStreams, packetsAvailable } = mergeActivityFrames([
            frame({}),
        ]);
        expect(patches.size).toBe(0);
        expect(hasNewStreams).toBe(false);
        expect(packetsAvailable).toEqual([]);
    });

    it('returns empty effects for no frames', () => {
        expect(mergeActivityFrames([])).toEqual({
            patches: new Map(),
            hasNewStreams: false,
            newStreamIds: [],
            packetsAvailable: [],
        });
    });

    it('unions new_stream ids across frames of one flush', () => {
        const { newStreamIds } = mergeActivityFrames([
            frame({ new_streams: [5, 6] }),
            frame({ new_streams: [6, 7] }),
        ]);
        expect(newStreamIds).toEqual([5, 6, 7]);
    });
});

describe('applyStreamPatches', () => {
    const streams = () => [
        { id: 1, packet_count: 100, last_time: 50 },
        { id: 2, packet_count: 5, last_time: 60 },
    ];

    it('ADDS count_delta instead of replacing (per-second deltas)', () => {
        const patched = applyStreamPatches(streams(), new Map([[1, { count_delta: 7 }]]));
        expect(patched[0].packet_count).toBe(107);
        // untouched stream stays identical
        expect(patched[1]).toEqual({ id: 2, packet_count: 5, last_time: 60 });
    });

    it('replaces last_time when provided', () => {
        const patched = applyStreamPatches(
            streams(),
            new Map([[1, { count_delta: 1, last_time: 99 }]]),
        );
        expect(patched[0].last_time).toBe(99);
    });

    it('keeps last_time when the patch omits it', () => {
        const patched = applyStreamPatches(streams(), new Map([[1, { count_delta: 2 }]]));
        expect(patched[0].last_time).toBe(50);
    });

    it('returns the same array reference when nothing matches', () => {
        const input = streams();
        expect(applyStreamPatches(input, new Map([[42, { count_delta: 1 }]]))).toBe(input);
        expect(applyStreamPatches(input, new Map())).toBe(input);
    });

    it('treats missing packet_count as zero', () => {
        const patched = applyStreamPatches(
            [{ id: 1 }, { id: 2, packet_count: 3 }],
            new Map([[1, { count_delta: 4 }]]),
        );
        expect(patched[0].packet_count).toBe(4);
    });
});
