// Pure auto-merge for packet cards (TODO.md §2.4 item 4).
//
// Semantics (identical to the previous inline implementation in usePackets):
// walk the packet list once; a run of CONSECUTIVE packets sharing
// `direction` AND `_stream_id` collapses into ONE card that keeps the FIRST
// packet's metadata (id/timestamp/...) and whose `packet_data` is the
// concatenation of the run's payloads (base64 in, base64 out).
// `_merged_count` records the run length (1 when not merging).
//
// Complexity: the old code re-decoded + re-encoded the accumulated payload
// for EVERY appended packet — O(total_bytes × run_length) (quadratic in run
// size, and base64 string churn on top). This version decodes each payload
// exactly once and encodes each group exactly once: O(total_bytes) linear.

export interface MergeablePacket {
    direction: string;
    packet_data: string;
    _stream_id?: number;
    stream_id?: number;
}

export interface MergedPacket<T> {
    packet: T;
    mergedCount: number;
}

const base64ToBytes = (b64: string): Uint8Array => {
    const binary = atob(b64);
    const len = binary.length;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) bytes[i] = binary.charCodeAt(i);
    return bytes;
};

// String.fromCharCode.apply is bounded by the argument count; 8k per call is
// safely below every engine's limit and keeps the loop linear.
const CHUNK = 8192;
const bytesToBase64 = (bytes: Uint8Array): string => {
    let binary = '';
    for (let i = 0; i < bytes.length; i += CHUNK) {
        binary += String.fromCharCode(...bytes.subarray(i, i + CHUNK));
    }
    return btoa(binary);
};

const streamKey = <T extends MergeablePacket>(p: T): number | undefined =>
    p._stream_id ?? p.stream_id;

/**
 * Merge adjacent same-direction packets of the same stream.
 * Single pass, no intermediate base64 round-trips per group member.
 */
export function mergeAdjacentPackets<T extends MergeablePacket>(packets: T[]): Array<T & { _merged_count: number }> {
    const result: Array<T & { _merged_count: number }> = [];

    // Buffers for the group currently being accumulated.
    let groupHead: T | null = null;
    let groupChunks: Uint8Array[] = [];
    let groupBytes = 0;
    let groupCount = 0;

    const flushGroup = () => {
        if (groupHead === null) return;
        if (groupCount === 1) {
            // Single-packet group: keep the ORIGINAL base64 string — a
            // decode/encode round-trip must not become observable for
            // non-canonical base64 inputs.
            result.push({ ...groupHead, _merged_count: 1 } as T & { _merged_count: number });
        } else {
            const merged = new Uint8Array(groupBytes);
            let offset = 0;
            for (const chunk of groupChunks) {
                merged.set(chunk, offset);
                offset += chunk.length;
            }
            result.push({
                ...groupHead,
                packet_data: bytesToBase64(merged),
                _merged_count: groupCount,
            } as T & { _merged_count: number });
        }
        groupHead = null;
        groupChunks = [];
        groupBytes = 0;
        groupCount = 0;
    };

    for (const pkt of packets) {
        if (
            groupHead !== null &&
            groupHead.direction === pkt.direction &&
            streamKey(groupHead) === streamKey(pkt)
        ) {
            const bytes = base64ToBytes(pkt.packet_data);
            groupChunks.push(bytes);
            groupBytes += bytes.length;
            groupCount += 1;
        } else {
            flushGroup();
            groupHead = pkt;
            groupChunks = [base64ToBytes(pkt.packet_data)];
            groupBytes = groupChunks[0].length;
            groupCount = 1;
        }
    }
    flushGroup();

    return result;
}

/** No-merge pass: same output shape, every run has length 1. */
export function splitPackets<T extends MergeablePacket>(packets: T[]): Array<T & { _merged_count: number }> {
    return packets.map((p) => ({ ...p, _merged_count: 1 }) as T & { _merged_count: number });
}
