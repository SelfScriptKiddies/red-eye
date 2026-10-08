import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { DynamicRowHeight } from 'react-window';

// react-window's built-in `useDynamicRowHeight` caches measured heights by
// ROW INDEX. The packets list reuses indexes across streams (selecting
// another stream, merge toggling, refetches), so a fresh card inherited the
// height of whatever was at that index before — cards rendered with wrong
// heights and jumped once the ResizeObserver re-measured (TODO.md §2.4
// item 4).
//
// This variant keys the cache by a STABLE item identity (the caller's
// `getKey`, e.g. `${stream_id}:${packet_id}`). Rows carry the key in a
// `data-row-key` attribute; the ResizeObserver reads it from the element,
// so a measurement is attributed to the right card even if the list
// shifted between render and callback.
//
// The returned object fulfills react-window's DynamicRowHeight interface
// (duck-typed by the List: getRowHeight/getAverageRowHeight/observeRowElements).

const ROW_KEY_ATTR = 'data-row-key';

export function useStableRowHeight<T>({
    defaultRowHeight,
    items,
    getKey,
}: {
    defaultRowHeight: number;
    items: T[];
    getKey: (_item: T, _index: number) => string;
}): DynamicRowHeight {
    // Latest items/getKey for index→key resolution during render-time
    // measurement (before the next effect flushes refs is fine — refs are
    // assigned during render).
    const itemsRef = useRef(items);
    const getKeyRef = useRef(getKey);
    itemsRef.current = items;
    getKeyRef.current = getKey;

    const [heights, setHeights] = useState<ReadonlyMap<string, number>>(() => new Map());

    const keyOfIndex = useCallback((index: number): string | undefined => {
        const item = itemsRef.current[index];
        if (item === undefined) return undefined;
        return getKeyRef.current(item, index);
    }, []);

    const getRowHeight = useCallback(
        (index: number): number | undefined => {
            const key = keyOfIndex(index);
            return key === undefined ? undefined : heights.get(key);
        },
        [heights, keyOfIndex],
    );

    const getAverageRowHeight = useCallback((): number => {
        if (heights.size === 0) return defaultRowHeight;
        let total = 0;
        heights.forEach((h) => { total += h; });
        return total / heights.size;
    }, [defaultRowHeight, heights]);

    // Conforms to react-window's DynamicRowHeight#setRowHeight(index, size).
    // A numeric argument is treated as a row INDEX and resolved to the
    // current item's key (the List itself never calls this — manual use).
    const setRowHeight = useCallback(
        (indexOrKey: number | string, size: number) => {
            const key = typeof indexOrKey === 'number' ? keyOfIndex(indexOrKey) : indexOrKey;
            if (key === undefined) return;
            setHeights((prev) => {
                if (prev.get(key) === size) return prev;
                const next = new Map(prev);
                next.set(key, size);
                return next;
            });
        },
        [keyOfIndex],
    );

    const [observer] = useState<ResizeObserver | undefined>(() => {
        if (typeof ResizeObserver === 'undefined') return undefined;
        return new ResizeObserver((entries) => {
            for (const entry of entries) {
                const el = entry.target as HTMLElement;
                const key = el.getAttribute(ROW_KEY_ATTR);
                const blockSize = entry.borderBoxSize?.[0]?.blockSize ?? el.offsetHeight;
                if (key != null && blockSize > 0) setRowHeight(key, blockSize);
            }
        });
    });

    useEffect(() => () => observer?.disconnect(), [observer]);

    const observeRowElements = useCallback(
        (elements: Element[] | NodeListOf<Element>) => {
            if (!observer) return () => {};
            const rows = Array.from(elements);
            rows.forEach((el) => observer.observe(el));
            return () => rows.forEach((el) => observer.unobserve(el));
        },
        [observer],
    );

    return useMemo(
        () => ({ getAverageRowHeight, getRowHeight, setRowHeight, observeRowElements }),
        [getAverageRowHeight, getRowHeight, setRowHeight, observeRowElements],
    );
}
