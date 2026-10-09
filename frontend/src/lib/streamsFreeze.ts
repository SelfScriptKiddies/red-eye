// Pure reducer behind the frozen streams list + «⏸ N новых» plaque
// (TODO.md §2.4 item 3).
//
// CONTRACT (what the user sees):
//   * The react-query 'streams' query keeps refetching page 1 (interval,
//     window focus, WS new-streams invalidation) — but the VISIBLE list is a
//     FROZEN SNAPSHOT: a background refetch never reorders/inserts/removes
//     rows under the user, so neither the list nor the scroll position
//     jumps while Live is on.
//   * Fresh pages still refresh fields IN PLACE for streams already visible
//     (packet_count / last_time / tags stay current).
//   * Streams that appeared in the fresh page (or arrived via WS activity:
//     new_streams ids and count deltas for streams NOT on the current page)
//     accumulate as DISTINCT ids in `pendingNewIds` — rendered as the
//     «⏸ N новых» plaque button.
//   * UNFREEZE: clicking the plaque ('accept') clears the plaque, scrolls
//     to top and applies the NEXT refetch result wholesale (awaitingAccept).
//     Filters changing (queryKey change) or the first load also apply
//     wholesale; 'reset' empties everything.
//   * loadMore appends below the frozen snapshot (never reorders it).

import { applyStreamPatches, type StreamPatch } from './activityEvents';

export interface FreezeState<T extends { id: number }> {
    /** Frozen snapshot — the array the list actually renders. */
    visible: T[];
    /** Distinct ids of streams the user hasn't accepted yet (plaque count). */
    pendingNewIds: number[];
    /** Filter identity the snapshot was built under ('' = nothing yet). */
    queryKey: string;
    /** Set by 'accept': the next 'query_data' replaces the snapshot. */
    awaitingAccept: boolean;
    /** Increments whenever the view must scroll to the top. */
    scrollToken: number;
}

export type FreezeAction<T extends { id: number }> =
    /** A page-1 fetch/refresh completed under `queryKey`. */
    | { type: 'query_data'; data: T[]; queryKey: string; maxVisible: number }
    /** WS activity flush: per-stream deltas + first-seen stream ids. */
    | { type: 'activity'; patches: Map<number, StreamPatch>; newStreamIds: number[] }
    /** loadMore result: append below the snapshot. */
    | { type: 'append'; items: T[]; maxVisible: number }
    /** Local edit of one visible stream (favorite toggle). */
    | { type: 'update_stream'; id: number; updater: (stream: T) => T }
    /** Unfreeze on next refetch; scroll to top unless `scroll` is false
     * (auto-apply from Live mode must not yank the scroll position). */
    | { type: 'accept'; scroll?: boolean }
    /** Clear all (filter reset). */
    | { type: 'reset' };

export function initialFreezeState<T extends { id: number }>(): FreezeState<T> {
    return { visible: [], pendingNewIds: [], queryKey: '', awaitingAccept: false, scrollToken: 0 };
}

const unionIds = (current: number[], additions: Iterable<number>): number[] => {
    const seen = new Set(current);
    const next = current.slice();
    for (const id of additions) {
        if (!seen.has(id)) {
            seen.add(id);
            next.push(id);
        }
    }
    return next;
};

export function freezeReducer<T extends { id: number }>(
    state: FreezeState<T>,
    action: FreezeAction<T>,
): FreezeState<T> {
    switch (action.type) {
        case 'query_data': {
            const { data, queryKey, maxVisible } = action;

            // Different filters (or first load after reset): brand-new
            // dataset — apply immediately, never freeze stale rows.
            if (queryKey !== state.queryKey) {
                return {
                    ...state,
                    visible: data.slice(0, maxVisible),
                    pendingNewIds: [],
                    queryKey,
                    awaitingAccept: false,
                };
            }

            // Plaque was clicked (or nothing is loaded yet): apply the
            // fresh page wholesale and stop freezing.
            if (state.awaitingAccept || state.visible.length === 0) {
                return {
                    ...state,
                    visible: data.slice(0, maxVisible),
                    pendingNewIds: [],
                    awaitingAccept: false,
                };
            }

            // Background refetch on the SAME filters: freeze the view.
            // Refresh in place what is visible; accumulate the rest.
            const freshById = new Map(data.map((s) => [s.id, s]));
            const visibleIds = new Set(state.visible.map((s) => s.id));
            let changed = false;

            const visible = state.visible.map((s) => {
                const fresh = freshById.get(s.id);
                if (fresh !== undefined && fresh !== s) {
                    changed = true;
                    return fresh;
                }
                return s;
            });

            const newIds = data.filter((s) => !visibleIds.has(s.id)).map((s) => s.id);
            const pendingNewIds = newIds.length > 0 ? unionIds(state.pendingNewIds, newIds) : state.pendingNewIds;
            if (pendingNewIds !== state.pendingNewIds) changed = true;

            // Reference-stable no-op: a refetch that changes nothing must
            // not re-render the list.
            if (!changed) return state;
            return { ...state, visible, pendingNewIds };
        }

        case 'activity': {
            const { patches, newStreamIds } = action;
            if (patches.size === 0 && newStreamIds.length === 0) return state;

            const visible = applyStreamPatches(state.visible, patches);
            const visibleIds = new Set(visible.map((s) => s.id));

            // Deltas for streams NOT on the current page + first-seen ids:
            // activity the frozen view does not show yet → plaque.
            const offPage = [...patches.keys()].filter((id) => !visibleIds.has(id));
            const fresh = newStreamIds.filter((id) => !visibleIds.has(id));

            if (visible === state.visible && offPage.length === 0 && fresh.length === 0) return state;
            return {
                ...state,
                visible,
                pendingNewIds: unionIds(state.pendingNewIds, [...offPage, ...fresh]),
            };
        }

        case 'append': {
            if (action.items.length === 0) return state;
            const existing = new Set(state.visible.map((s) => s.id));
            const additions = action.items.filter((s) => !existing.has(s.id));
            return {
                ...state,
                visible: [...state.visible, ...additions].slice(0, action.maxVisible),
            };
        }

        case 'update_stream': {
            const visible = state.visible.map((s) =>
                s.id === action.id ? action.updater(s) : s,
            );
            return { ...state, visible };
        }

        case 'accept':
            // Arm the wholesale apply. Manual apply (Update) also scrolls to
            // top; Live's auto-apply passes scroll:false — yanking the user's
            // scroll position every batch is exactly the jerk to avoid.
            return {
                ...state,
                pendingNewIds: [],
                awaitingAccept: true,
                scrollToken: action.scroll === false ? state.scrollToken : state.scrollToken + 1,
            };

        case 'reset':
            return { ...initialFreezeState<T>(), scrollToken: state.scrollToken };
    }
}
