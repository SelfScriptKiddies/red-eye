import { describe, it, expect } from 'vitest';
import { mergeAdjacentPackets, splitPackets } from './mergePackets';

// Node's atob/btoa operate on the same base64 alphabet the browser uses.
const b64 = (s: string): string => btoa(s);

interface P {
    id: number;
    direction: string;
    packet_data: string;
    _stream_id: number;
    timestamp?: number;
}

const pkt = (id: number, direction: string, data: string, stream = 7): P => ({
    id,
    direction,
    packet_data: b64(data),
    _stream_id: stream,
});

describe('mergeAdjacentPackets', () => {
    it('merges a run of same-direction packets of one stream', () => {
        const out = mergeAdjacentPackets([
            pkt(1, 'dest', 'GET '),
            pkt(2, 'dest', '/index'),
            pkt(3, 'dest', ' HTTP'),
        ]);
        expect(out).toHaveLength(1);
        expect(out[0].id).toBe(1);            // first packet's metadata
        expect(atob(out[0].packet_data)).toBe('GET /index HTTP');
        expect(out[0]._merged_count).toBe(3);
    });

    it('breaks the run on direction change', () => {
        const out = mergeAdjacentPackets([
            pkt(1, 'dest', 'req'),
            pkt(2, 'orig', 'resp'),
            pkt(3, 'orig', 'resp2'),
        ]);
        expect(out).toHaveLength(2);
        expect(out[0].id).toBe(1);
        expect(out[0]._merged_count).toBe(1);
        expect(out[1].id).toBe(2);
        expect(atob(out[1].packet_data)).toBe('respresp2');
        expect(out[1]._merged_count).toBe(2);
    });

    it('breaks the run on stream change (multi-select view)', () => {
        const out = mergeAdjacentPackets([
            pkt(1, 'dest', 'aaa', 1),
            pkt(2, 'dest', 'bbb', 2),
        ]);
        expect(out).toHaveLength(2);
        expect(out.every((p) => p._merged_count === 1)).toBe(true);
    });

    it('re-opens a run after a break (A A B A A A)', () => {
        const out = mergeAdjacentPackets([
            pkt(1, 'dest', 'a'),
            pkt(2, 'dest', 'a'),
            pkt(3, 'orig', 'b'),
            pkt(4, 'dest', 'c'),
            pkt(5, 'dest', 'c'),
            pkt(6, 'dest', 'c'),
        ]);
        expect(out.map((p) => p.id)).toEqual([1, 3, 4]);
        expect(out.map((p) => p._merged_count)).toEqual([2, 1, 3]);
    });

    it('keeps the original base64 string for single-packet groups', () => {
        const single = b64('solo');
        const out = mergeAdjacentPackets([
            { id: 9, direction: 'dest', packet_data: single, _stream_id: 1 },
        ]);
        expect(out[0].packet_data).toBe(single);
    });

    it('merges empty and binary payloads byte-exactly', () => {
        const bin = (n: number) => String.fromCharCode(...Array.from({ length: n }, (_, i) => i & 0xff));
        const out = mergeAdjacentPackets([
            pkt(1, 'dest', bin(256)),
            pkt(2, 'dest', bin(256)),
        ]);
        const decoded = atob(out[0].packet_data);
        expect(decoded).toHaveLength(512);
        expect(decoded.slice(0, 256)).toBe(bin(256));
        expect(decoded.slice(256)).toBe(bin(256));
    });

    it('handles empty input', () => {
        expect(mergeAdjacentPackets([])).toEqual([]);
    });

    it('is linear in total bytes: 1000-segment merge of 1KB each completes and is exact', () => {
        const segment = 'x'.repeat(1024);
        const input = Array.from({ length: 1000 }, (_, i) => pkt(i + 1, 'dest', segment));
        const out = mergeAdjacentPackets(input);
        expect(out).toHaveLength(1);
        expect(out[0]._merged_count).toBe(1000);
        expect(atob(out[0].packet_data)).toHaveLength(1000 * 1024);
    });
});

describe('splitPackets', () => {
    it('adds _merged_count 1 without touching payloads', () => {
        const src = [pkt(1, 'dest', 'a'), pkt(2, 'orig', 'b')];
        const out = splitPackets(src);
        expect(out).toHaveLength(2);
        expect(out.map((p) => p._merged_count)).toEqual([1, 1]);
        expect(out[0].packet_data).toBe(src[0].packet_data);
    });
});
