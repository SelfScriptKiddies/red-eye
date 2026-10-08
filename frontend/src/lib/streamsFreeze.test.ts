import { describe, it, expect } from 'vitest';
import { freezeReducer, initialFreezeState, type FreezeAction, type FreezeState } from './streamsFreeze';
import type { StreamPatch } from './activityEvents';

interface S {
    id: number;
    packet_count: number;
    last_time: number;
    favorite?: boolean;
}

const s = (id: number, packet_count = 1, last_time = 1000): S => ({ id, packet_count, last_time });

const start = (): FreezeState<S> => initialFreezeState<S>();
const run = (state: FreezeState<S>, ...actions: FreezeAction<S>[]): FreezeState<S> =>
    actions.reduce(freezeReducer<S>, state);

describe('freezeReducer — query_data', () => {
    it('first load applies immediately', () => {
        const st = run(start(), { type: 'query_data', data: [s(1), s(2)], queryKey: 'k1', maxVisible: 200 });
        expect(st.visible.map((x) => x.id)).toEqual([1, 2]);
        expect(st.queryKey).toBe('k1');
        expect(st.pendingNewIds).toEqual([]);
    });

    it('background refetch on the same filters does NOT reorder/replace the view', () => {
        let st = run(start(), { type: 'query_data', data: [s(1), s(2), s(3)], queryKey: 'k1', maxVisible: 200 });
        // Refetch: stream 4 is new and pushed stream 3 out of page 1; the
        // server now returns [4,1,2] — the frozen view must stay [1,2,3].
        st = freezeReducer(st, { type: 'query_data', data: [s(4), s(1), s(2)], queryKey: 'k1', maxVisible: 200 });
        expect(st.visible.map((x) => x.id)).toEqual([1, 2, 3]);
        expect(st.pendingNewIds).toEqual([4]);
    });

    it('refreshes fields of visible streams in place', () => {
        let st = run(start(), { type: 'query_data', data: [s(1, 5, 100)], queryKey: 'k1', maxVisible: 200 });
        st = freezeReducer(st, { type: 'query_data', data: [s(1, 12, 200)], queryKey: 'k1', maxVisible: 200 });
        expect(st.visible).toEqual([s(1, 12, 200)]);
        expect(st.pendingNewIds).toEqual([]);
    });

    it('no-op refetch keeps the same state reference (no re-render)', () => {
        // react-query structural sharing hands back the SAME item objects
        // for unchanged rows; the reducer relies on that for the no-op path.
        const page = [s(1, 5)];
        let st = run(start(), { type: 'query_data', data: page, queryKey: 'k1', maxVisible: 200 });
        const same = freezeReducer(st, { type: 'query_data', data: page, queryKey: 'k1', maxVisible: 200 });
        expect(same).toBe(st);
    });

    it('filter change (new queryKey) applies immediately', () => {
        let st = run(start(), { type: 'query_data', data: [s(1), s(2)], queryKey: 'k1', maxVisible: 200 });
        st = freezeReducer(st, { type: 'query_data', data: [s(9)], queryKey: 'k2', maxVisible: 200 });
        expect(st.visible.map((x) => x.id)).toEqual([9]);
        expect(st.pendingNewIds).toEqual([]);
    });

    it('accept applies the next refetch wholesale and clears pending', () => {
        let st = run(start(), { type: 'query_data', data: [s(1), s(2)], queryKey: 'k1', maxVisible: 200 });
        st = freezeReducer(st, { type: 'query_data', data: [s(3), s(1)], queryKey: 'k1', maxVisible: 200 });
        expect(st.pendingNewIds).toEqual([3]);
        st = freezeReducer(st, { type: 'accept' });
        expect(st.pendingNewIds).toEqual([]);
        expect(st.awaitingAccept).toBe(true);
        expect(st.scrollToken).toBe(1);
        st = freezeReducer(st, { type: 'query_data', data: [s(3), s(1), s(2)], queryKey: 'k1', maxVisible: 200 });
        expect(st.visible.map((x) => x.id)).toEqual([3, 1, 2]);
        expect(st.awaitingAccept).toBe(false);
    });

    it('dedupes pending ids across refetches', () => {
        let st = run(start(), { type: 'query_data', data: [s(1)], queryKey: 'k1', maxVisible: 200 });
        st = freezeReducer(st, { type: 'query_data', data: [s(2), s(1)], queryKey: 'k1', maxVisible: 200 });
        st = freezeReducer(st, { type: 'query_data', data: [s(2), s(1)], queryKey: 'k1', maxVisible: 200 });
        expect(st.pendingNewIds).toEqual([2]);
    });
});

describe('freezeReducer — activity (WS frames)', () => {
    it('patches visible streams additively without pending growth', () => {
        let st = run(start(), { type: 'query_data', data: [s(1, 10), s(2, 20)], queryKey: 'k1', maxVisible: 200 });
        const patches = new Map<number, StreamPatch>([
            [1, { count_delta: 5, last_time: 1234 }],
        ]);
        st = freezeReducer(st, { type: 'activity', patches, newStreamIds: [] });
        expect(st.visible[0].packet_count).toBe(15);
        expect(st.visible[0].last_time).toBe(1234);
        expect(st.pendingNewIds).toEqual([]);
    });

    it('count deltas for streams NOT on the page land in pending', () => {
        let st = run(start(), { type: 'query_data', data: [s(1)], queryKey: 'k1', maxVisible: 200 });
        const patches = new Map<number, StreamPatch>([
            [42, { count_delta: 3 }],
        ]);
        st = freezeReducer(st, { type: 'activity', patches, newStreamIds: [] });
        expect(st.pendingNewIds).toEqual([42]);
        // same stream again — still one entry
        st = freezeReducer(st, { type: 'activity', patches: new Map([[42, { count_delta: 1 }]]), newStreamIds: [] });
        expect(st.pendingNewIds).toEqual([42]);
    });

    it('new_streams ids not visible land in pending', () => {
        let st = run(start(), { type: 'query_data', data: [s(1)], queryKey: 'k1', maxVisible: 200 });
        st = freezeReducer(st, { type: 'activity', patches: new Map(), newStreamIds: [7, 8] });
        expect(st.pendingNewIds).toEqual([7, 8]);
    });

    it('empty activity is a state no-op', () => {
        let st = run(start(), { type: 'query_data', data: [s(1)], queryKey: 'k1', maxVisible: 200 });
        const same = freezeReducer(st, { type: 'activity', patches: new Map(), newStreamIds: [] });
        expect(same).toBe(st);
    });
});

describe('freezeReducer — misc', () => {
    it('append extends the snapshot without dedup failures', () => {
        let st = run(start(), { type: 'query_data', data: [s(1)], queryKey: 'k1', maxVisible: 200 });
        st = freezeReducer(st, { type: 'append', items: [s(2), s(3)], maxVisible: 200 });
        expect(st.visible.map((x) => x.id)).toEqual([1, 2, 3]);
        // overlapping append (cursor hiccup) must not duplicate rows
        st = freezeReducer(st, { type: 'append', items: [s(3), s(4)], maxVisible: 200 });
        expect(st.visible.map((x) => x.id)).toEqual([1, 2, 3, 4]);
    });

    it('append caps at maxVisible', () => {
        let st = run(start(), { type: 'query_data', data: [s(1)], queryKey: 'k1', maxVisible: 3 });
        st = freezeReducer(st, { type: 'append', items: [s(2), s(3), s(4), s(5)], maxVisible: 3 });
        expect(st.visible.map((x) => x.id)).toEqual([1, 2, 3]);
    });

    it('update_stream edits one visible stream (favorite toggle)', () => {
        let st = run(start(), { type: 'query_data', data: [s(1), s(2)], queryKey: 'k1', maxVisible: 200 });
        st = freezeReducer(st, { type: 'update_stream', id: 2, updater: (x) => ({ ...x, favorite: true }) });
        expect(st.visible[1].favorite).toBe(true);
        expect(st.visible[0].favorite).toBeUndefined();
    });

    it('reset empties everything but keeps scrollToken monotonic', () => {
        let st = run(start(), { type: 'query_data', data: [s(1)], queryKey: 'k1', maxVisible: 200 });
        st = freezeReducer(st, { type: 'query_data', data: [s(2), s(1)], queryKey: 'k1', maxVisible: 200 });
        st = freezeReducer(st, { type: 'accept' });
        expect(st.scrollToken).toBe(1);
        st = freezeReducer(st, { type: 'reset' });
        expect(st.visible).toEqual([]);
        expect(st.pendingNewIds).toEqual([]);
        expect(st.queryKey).toBe('');
        expect(st.scrollToken).toBe(1);
    });

    it('accept with nothing pending still arms the wholesale apply', () => {
        // Manual refresh path: arm first, the next query_data applies.
        let st = run(start(), { type: 'query_data', data: [s(1)], queryKey: 'k1', maxVisible: 200 });
        st = freezeReducer(st, { type: 'accept' });
        expect(st.awaitingAccept).toBe(true);
        expect(st.scrollToken).toBe(1);
        st = freezeReducer(st, { type: 'query_data', data: [s(2)], queryKey: 'k1', maxVisible: 200 });
        expect(st.visible.map((x) => x.id)).toEqual([2]);
        expect(st.awaitingAccept).toBe(false);
    });
});
