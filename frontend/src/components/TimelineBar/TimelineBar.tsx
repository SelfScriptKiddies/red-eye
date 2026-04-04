import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { fetchApi } from '../../lib/api';
import './TimelineBar.css';

const SERVICE_COLORS = [
    'var(--re-accent)',
    'var(--re-success)',
    '#0089ce',
    'var(--re-warning)',
    '#9b59b6',
    '#e74c3c',
    '#1abc9c',
    '#f39c12',
];

interface HistogramBucket {
    timestamp: number;
    service_id: number;
    service_name: string;
    count: number;
}

interface TimelineBarProps {
    onSelect: (startTime: number, endTime: number) => void;
    onClear: () => void;
}

type DragState =
    | { kind: 'idle' }
    | { kind: 'dragging'; startPct: number }
    | { kind: 'selected'; leftPct: number; rightPct: number }
    | { kind: 'moving'; leftPct: number; rightPct: number; grabOffset: number }
    | { kind: 'resizing'; edge: 'left' | 'right'; leftPct: number; rightPct: number };

function formatHHMM(ts: number): string {
    const d = new Date(ts * 1000);
    return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
}

export function TimelineBar({ onSelect, onClear }: TimelineBarProps) {
    const [data, setData] = useState<HistogramBucket[]>([]);
    const [collapsed, setCollapsed] = useState<boolean>(() => {
        try {
            return localStorage.getItem('timeline-collapsed') === 'true';
        } catch {
            return false;
        }
    });
    const [drag, setDrag] = useState<DragState>({ kind: 'idle' });
    const chartRef = useRef<HTMLDivElement>(null);

    // Fetch histogram data
    useEffect(() => {
        let cancelled = false;
        const load = async () => {
            const result = await fetchApi<HistogramBucket[]>('/api/stream/histogram?bucket_secs=60');
            if (!cancelled && result) setData(result);
        };
        load();
        const interval = setInterval(load, 10_000);
        return () => {
            cancelled = true;
            clearInterval(interval);
        };
    }, []);

    // Persist collapsed state
    const toggleCollapsed = useCallback(() => {
        setCollapsed(prev => {
            const next = !prev;
            try { localStorage.setItem('timeline-collapsed', String(next)); } catch { /* ignore */ }
            return next;
        });
    }, []);

    // Build chart data
    const chartData = useMemo(() => {
        if (data.length === 0) return null;

        // Collect all timestamps and service ids
        const timestamps = [...new Set(data.map(d => d.timestamp))].sort((a, b) => a - b);
        const serviceIds = [...new Set(data.map(d => d.service_id))];
        const serviceNames = new Map<number, string>();
        for (const d of data) serviceNames.set(d.service_id, d.service_name);

        const minTime = timestamps[0];
        const maxTime = timestamps[timestamps.length - 1];
        const range = Math.max(maxTime - minTime, 1);

        // Count per (timestamp, service_id)
        const countMap = new Map<string, number>();
        for (const d of data) {
            countMap.set(`${d.timestamp}_${d.service_id}`, d.count);
        }

        // Max stacked count (for Y scale)
        let maxStack = 0;
        for (const ts of timestamps) {
            let total = 0;
            for (const sid of serviceIds) {
                total += countMap.get(`${ts}_${sid}`) ?? 0;
            }
            if (total > maxStack) maxStack = total;
        }
        if (maxStack === 0) maxStack = 1;

        // Build SVG paths per service (stacked area)
        // Each path goes: left-to-right top edge, then right-to-left bottom edge
        const paths: { color: string; d: string }[] = [];

        for (let si = 0; si < serviceIds.length; si++) {
            const sid = serviceIds[si];
            const color = SERVICE_COLORS[si % SERVICE_COLORS.length];

            // For each timestamp, compute bottom and top of this service's band
            const points: { x: number; bottom: number; top: number }[] = [];

            for (const ts of timestamps) {
                const x = ((ts - minTime) / range) * 100;
                let bottom = 0;
                for (let prev = 0; prev < si; prev++) {
                    bottom += countMap.get(`${ts}_${serviceIds[prev]}`) ?? 0;
                }
                const own = countMap.get(`${ts}_${sid}`) ?? 0;
                const top = bottom + own;

                // Convert to SVG Y coords (100 = bottom, 0 = top)
                const svgBottom = 100 - (bottom / maxStack) * 100;
                const svgTop = 100 - (top / maxStack) * 100;

                points.push({ x, bottom: svgBottom, top: svgTop });
            }

            if (points.length === 0) continue;

            // Build path: forward along top edge, backward along bottom edge
            let d = `M ${points[0].x} ${points[0].bottom}`;
            d += ` L ${points[0].x} ${points[0].top}`;
            for (let i = 1; i < points.length; i++) {
                d += ` L ${points[i].x} ${points[i].top}`;
            }
            for (let i = points.length - 1; i >= 0; i--) {
                d += ` L ${points[i].x} ${points[i].bottom}`;
            }
            d += ' Z';

            paths.push({ color, d });
        }

        // Time axis ticks
        const tickCount = Math.min(8, Math.max(3, Math.floor(range / 60)));
        const tickStep = range / tickCount;
        const ticks: { time: number; pct: number }[] = [];
        for (let i = 0; i <= tickCount; i++) {
            const t = minTime + i * tickStep;
            ticks.push({ time: t, pct: ((t - minTime) / range) * 100 });
        }

        return { paths, ticks, minTime, maxTime, range };
    }, [data]);

    // Convert client X to percentage
    const pxToPct = useCallback((clientX: number): number => {
        if (!chartRef.current) return 0;
        const rect = chartRef.current.getBoundingClientRect();
        return Math.max(0, Math.min(100, ((clientX - rect.left) / rect.width) * 100));
    }, []);

    // Convert percentage to time
    const pctToTime = useCallback((pct: number): number => {
        if (!chartData) return 0;
        return chartData.minTime + (pct / 100) * chartData.range;
    }, [chartData]);

    // Fire onSelect with current selection bounds
    const fireSelect = useCallback((leftPct: number, rightPct: number) => {
        onSelect(pctToTime(leftPct), pctToTime(rightPct));
    }, [onSelect, pctToTime]);

    // Mouse down on chart area
    const handleMouseDown = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
        if (e.button !== 0) return;
        e.preventDefault();

        const pct = pxToPct(e.clientX);

        if (drag.kind === 'selected') {
            const { leftPct, rightPct } = drag;
            // Check if near left edge (within 2%)
            if (Math.abs(pct - leftPct) <= 2) {
                setDrag({ kind: 'resizing', edge: 'left', leftPct, rightPct });
                return;
            }
            // Check if near right edge (within 2%)
            if (Math.abs(pct - rightPct) <= 2) {
                setDrag({ kind: 'resizing', edge: 'right', leftPct, rightPct });
                return;
            }
            // Check if inside selection
            if (pct >= leftPct && pct <= rightPct) {
                const grabOffset = pct - leftPct;
                setDrag({ kind: 'moving', leftPct, rightPct, grabOffset });
                return;
            }
            // Outside: clear and start new drag
            onClear();
        }

        setDrag({ kind: 'dragging', startPct: pct });
    }, [drag, pxToPct, onClear]);

    // Window mouse events for drag/move/resize
    useEffect(() => {
        if (drag.kind === 'idle' || drag.kind === 'selected') return;

        const handleMove = (e: MouseEvent) => {
            const pct = pxToPct(e.clientX);

            if (drag.kind === 'dragging') {
                // Just update visual, no state change needed — handled in render
                // We need to force re-render with current mouse position
                // Use a synthetic selected state for preview
                const left = Math.min(drag.startPct, pct);
                const right = Math.max(drag.startPct, pct);
                // We update drag to carry current position for preview
                setDrag({ kind: 'dragging', startPct: drag.startPct, ...{ currentPct: pct } } as any);
                void left; void right;
            } else if (drag.kind === 'moving') {
                const { rightPct, leftPct, grabOffset } = drag;
                const width = rightPct - leftPct;
                const newLeft = Math.max(0, Math.min(100 - width, pct - grabOffset));
                setDrag({ ...drag, leftPct: newLeft, rightPct: newLeft + width });
            } else if (drag.kind === 'resizing') {
                const { edge, leftPct, rightPct } = drag;
                if (edge === 'left') {
                    const newLeft = Math.max(0, Math.min(rightPct - 1, pct));
                    setDrag({ ...drag, leftPct: newLeft });
                } else {
                    const newRight = Math.min(100, Math.max(leftPct + 1, pct));
                    setDrag({ ...drag, rightPct: newRight });
                }
            }
        };

        const handleUp = (e: MouseEvent) => {
            const pct = pxToPct(e.clientX);

            if (drag.kind === 'dragging') {
                const left = Math.min(drag.startPct, pct);
                const right = Math.max(drag.startPct, pct);
                if (right - left > 1) {
                    setDrag({ kind: 'selected', leftPct: left, rightPct: right });
                    fireSelect(left, right);
                } else {
                    setDrag({ kind: 'idle' });
                    onClear();
                }
            } else if (drag.kind === 'moving') {
                const { leftPct, rightPct } = drag;
                setDrag({ kind: 'selected', leftPct, rightPct });
                fireSelect(leftPct, rightPct);
            } else if (drag.kind === 'resizing') {
                const { leftPct, rightPct } = drag;
                setDrag({ kind: 'selected', leftPct, rightPct });
                fireSelect(leftPct, rightPct);
            }
        };

        window.addEventListener('mousemove', handleMove);
        window.addEventListener('mouseup', handleUp);
        return () => {
            window.removeEventListener('mousemove', handleMove);
            window.removeEventListener('mouseup', handleUp);
        };
    }, [drag, pxToPct, fireSelect, onClear]);

    // Compute selection rect for rendering
    const selectionRect = useMemo((): { left: number; right: number; interactive: boolean } | null => {
        if (drag.kind === 'selected') {
            return { left: drag.leftPct, right: drag.rightPct, interactive: true };
        }
        if (drag.kind === 'dragging') {
            const d = drag as any;
            const cur = d.currentPct ?? drag.startPct;
            const left = Math.min(drag.startPct, cur);
            const right = Math.max(drag.startPct, cur);
            if (right - left < 0.1) return null;
            return { left, right, interactive: false };
        }
        if (drag.kind === 'moving' || drag.kind === 'resizing') {
            return { left: drag.leftPct, right: drag.rightPct, interactive: false };
        }
        return null;
    }, [drag]);

    return (
        <>
            <div className={`timeline-bar-wrapper${collapsed ? ' collapsed' : ''}`}>
                <div
                    className="timeline-bar-container"
                    ref={chartRef}
                    onMouseDown={handleMouseDown}
                >
                    <button
                        className="timeline-bar-toggle"
                        onMouseDown={e => e.stopPropagation()}
                        onClick={toggleCollapsed}
                        title="Collapse timeline"
                    >
                        ▲
                    </button>

                    <div className="timeline-bar-chart">
                        {chartData && chartData.paths.length > 0 && (
                            <svg
                                width="100%"
                                height="100%"
                                viewBox="0 0 100 100"
                                preserveAspectRatio="none"
                            >
                                {chartData.paths.map((p, i) => (
                                    <path
                                        key={i}
                                        d={p.d}
                                        fill={p.color}
                                        opacity={0.7}
                                    />
                                ))}
                            </svg>
                        )}

                        {selectionRect && (
                            <div
                                className={`timeline-bar-selection${selectionRect.interactive ? ' interactive' : ''}`}
                                style={{
                                    left: `${selectionRect.left}%`,
                                    width: `${selectionRect.right - selectionRect.left}%`,
                                }}
                                onMouseDown={selectionRect.interactive ? (e) => {
                                    // Let the parent handleMouseDown take over
                                    // (it checks if we're inside selection)
                                } : undefined}
                            >
                                {selectionRect.interactive && (
                                    <>
                                        <div
                                            className="timeline-bar-handle timeline-bar-handle--left"
                                            onMouseDown={e => { e.stopPropagation(); if (!chartData) return; const pct = (drag as any).leftPct ?? 0; setDrag({ kind: 'resizing', edge: 'left', leftPct: (drag as any).leftPct, rightPct: (drag as any).rightPct }); }}
                                        />
                                        <div
                                            className="timeline-bar-handle timeline-bar-handle--right"
                                            onMouseDown={e => { e.stopPropagation(); setDrag({ kind: 'resizing', edge: 'right', leftPct: (drag as any).leftPct, rightPct: (drag as any).rightPct }); }}
                                        />
                                    </>
                                )}
                            </div>
                        )}
                    </div>

                    <div className="timeline-bar-axis">
                        {chartData?.ticks.map((tick, i) => (
                            <span
                                key={i}
                                className="timeline-bar-tick"
                                style={{ left: `${tick.pct}%` }}
                            >
                                {formatHHMM(tick.time)}
                            </span>
                        ))}
                    </div>
                </div>
            </div>

            {collapsed && (
                <div className="timeline-bar-expand-btn">
                    <button onClick={toggleCollapsed}>▼ Timeline</button>
                </div>
            )}
        </>
    );
}
