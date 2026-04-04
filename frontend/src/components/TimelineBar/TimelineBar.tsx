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

        // Build stacked bar rects per bucket
        const bucketSecs = 60;
        const barWidthPct = Math.max(0.5, (bucketSecs / range) * 100 * 0.8); // 80% of bucket width
        const bars: { x: number; y: number; height: number; color: string }[] = [];

        for (const ts of timestamps) {
            const centerX = ((ts - minTime) / range) * 100;
            let bottom = 0;

            for (let si = 0; si < serviceIds.length; si++) {
                const sid = serviceIds[si];
                const count = countMap.get(`${ts}_${sid}`) ?? 0;
                if (count === 0) continue;

                const color = SERVICE_COLORS[si % SERVICE_COLORS.length];
                const barHeight = (count / maxStack) * 100;
                const y = 100 - bottom - barHeight; // SVG y = top of rect

                bars.push({
                    x: centerX - barWidthPct / 2,
                    y,
                    height: barHeight,
                    color,
                });

                bottom += barHeight;
            }
        }

        // Time axis ticks
        const tickCount = Math.min(8, Math.max(3, Math.floor(range / 60)));
        const tickStep = range / tickCount;
        const ticks: { time: number; pct: number }[] = [];
        for (let i = 0; i <= tickCount; i++) {
            const t = minTime + i * tickStep;
            ticks.push({ time: t, pct: ((t - minTime) / range) * 100 });
        }

        return { bars, barWidthPct, ticks, minTime, maxTime, range };
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

    // Live drag position stored in ref to avoid listener churn
    const dragRef = useRef(drag);
    dragRef.current = drag;
    const [selectionRect, setSelectionRect] = useState<{ left: number; right: number; interactive: boolean } | null>(null);

    // Update selection rect from drag state
    useEffect(() => {
        if (drag.kind === 'selected') {
            setSelectionRect({ left: drag.leftPct, right: drag.rightPct, interactive: true });
        } else if (drag.kind === 'idle') {
            setSelectionRect(null);
        }
    }, [drag]);

    // Window mouse events for drag/move/resize — attaches once per active drag
    useEffect(() => {
        if (drag.kind === 'idle' || drag.kind === 'selected') return;

        const handleMove = (e: MouseEvent) => {
            const pct = pxToPct(e.clientX);
            const d = dragRef.current;

            if (d.kind === 'dragging') {
                const left = Math.min(d.startPct, pct);
                const right = Math.max(d.startPct, pct);
                if (right - left >= 0.1) {
                    setSelectionRect({ left, right, interactive: false });
                }
            } else if (d.kind === 'moving') {
                const width = d.rightPct - d.leftPct;
                const newLeft = Math.max(0, Math.min(100 - width, pct - d.grabOffset));
                dragRef.current = { ...d, leftPct: newLeft, rightPct: newLeft + width };
                setSelectionRect({ left: newLeft, right: newLeft + width, interactive: false });
            } else if (d.kind === 'resizing') {
                if (d.edge === 'left') {
                    const newLeft = Math.max(0, Math.min(d.rightPct - 1, pct));
                    dragRef.current = { ...d, leftPct: newLeft };
                    setSelectionRect({ left: newLeft, right: d.rightPct, interactive: false });
                } else {
                    const newRight = Math.min(100, Math.max(d.leftPct + 1, pct));
                    dragRef.current = { ...d, rightPct: newRight };
                    setSelectionRect({ left: d.leftPct, right: newRight, interactive: false });
                }
            }
        };

        const handleUp = (e: MouseEvent) => {
            const pct = pxToPct(e.clientX);
            const d = dragRef.current;

            if (d.kind === 'dragging') {
                const left = Math.min(d.startPct, pct);
                const right = Math.max(d.startPct, pct);
                if (right - left > 1) {
                    setDrag({ kind: 'selected', leftPct: left, rightPct: right });
                    fireSelect(left, right);
                } else {
                    setDrag({ kind: 'idle' });
                    onClear();
                }
            } else if (d.kind === 'moving' || d.kind === 'resizing') {
                const { leftPct, rightPct } = d;
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
    // Only re-attach when drag *kind* changes (idle↔dragging↔moving↔resizing), not on every position update
    }, [drag.kind]);

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
                        {chartData && chartData.bars.length > 0 && (
                            <svg
                                width="100%"
                                height="100%"
                                viewBox="0 0 100 100"
                                preserveAspectRatio="none"
                            >
                                {chartData.bars.map((bar, i) => (
                                    <rect
                                        key={i}
                                        x={bar.x}
                                        y={bar.y}
                                        width={chartData.barWidthPct}
                                        height={bar.height}
                                        fill={bar.color}
                                        opacity={0.7}
                                        rx={0.3}
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
                                            onMouseDown={e => { e.stopPropagation(); if (drag.kind === 'selected') setDrag({ kind: 'resizing', edge: 'left', leftPct: drag.leftPct, rightPct: drag.rightPct }); }}
                                        />
                                        <div
                                            className="timeline-bar-handle timeline-bar-handle--right"
                                            onMouseDown={e => { e.stopPropagation(); if (drag.kind === 'selected') setDrag({ kind: 'resizing', edge: 'right', leftPct: drag.leftPct, rightPct: drag.rightPct }); }}
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
