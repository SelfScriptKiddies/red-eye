import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { apiFetch } from '../../lib/api';
import './TimelineBar.css';

const PACKETS_COLOR = '#0089ce';   // синий — пакеты
const FLAGS_COLOR = '#e74c3c';     // красный — флаги
/// Nonzero values must stay visible even against a huge counterpart:
/// floor for the scaled bar height (viewBox units of 0..100).
const MIN_BAR_HEIGHT = 2.5;

interface HistogramBucket {
    timestamp: number;
    packet_count: number;
    flag_count: number;
}

interface TimelineBarProps {
    onSelect: (_startTime: number, _endTime: number) => void;
    onClear: () => void;
    clearRef?: React.MutableRefObject<(() => void) | null>;
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

export function TimelineBar({ onSelect, onClear, clearRef }: TimelineBarProps) {
    const { data = [] } = useQuery({
        queryKey: ['histogram', 60],
        queryFn: ({ signal }) => apiFetch<HistogramBucket[]>('/api/stream/histogram?bucket_secs=60', signal),
        refetchInterval: 10_000,
    });
    const [collapsed, setCollapsed] = useState<boolean>(() => {
        try {
            return localStorage.getItem('timeline-collapsed') === 'true';
        } catch {
            return false;
        }
    });
    const [drag, setDrag] = useState<DragState>({ kind: 'idle' });
    const [viewOffset, setViewOffset] = useState(0); // seconds to shift viewport left (negative = past)
    const chartRef = useRef<HTMLDivElement>(null);
    const MIN_WINDOW_SECS = 30 * 60; // 30 minutes minimum viewport
    const MAX_BAR_WIDTH_PCT = 6; // max bar width in % of chart

    // Persist collapsed state
    const toggleCollapsed = useCallback(() => {
        setCollapsed(prev => {
            const next = !prev;
            try { localStorage.setItem('timeline-collapsed', String(next)); } catch { /* ignore */ }
            return next;
        });
    }, []);

    // Build chart data with viewport windowing
    const chartData = useMemo(() => {
        if (data.length === 0) return null;

        const allTimestamps = [...new Set(data.map(d => d.timestamp))].sort((a, b) => a - b);

        const dataMin = allTimestamps[0];
        const dataMax = allTimestamps[allTimestamps.length - 1];
        const dataRange = dataMax - dataMin;

        // Viewport: at least MIN_WINDOW_SECS, right edge = now + viewOffset
        const now = Math.floor(Date.now() / 1000);
        const viewRight = now + viewOffset;
        const viewWindow = Math.max(MIN_WINDOW_SECS, dataRange + 120); // pad 2 min
        const viewLeft = viewRight - viewWindow;

        // Filter timestamps to viewport
        const timestamps = allTimestamps.filter(ts => ts >= viewLeft && ts <= viewRight);

        // Per-bucket totals (all services together — no per-service split)
        const bucketMap = new Map<number, HistogramBucket>();
        for (const d of data) bucketMap.set(d.timestamp, d);

        // ONE shared Y scale across both series (max of either), so bar
        // heights are directly comparable — packets next to flags.
        let maxBar = 0;
        for (const ts of allTimestamps) {
            const b = bucketMap.get(ts);
            if (!b) continue;
            maxBar = Math.max(maxBar, b.packet_count, b.flag_count);
        }
        if (maxBar === 0) maxBar = 1;

        // Two OVERLAID bars per bucket: the taller one behind, the shorter
        // in front (drawn later = painted on top in SVG). Packets — blue,
        // flags — red. Nonzero values get the MIN_BAR_HEIGHT floor so a
        // handful of flags stays visible next to thousands of packets.
        // `front` marks the bar painted last (dense) — for a pair it's the
        // shorter one; a lone bar has nothing behind it, so it's front too.
        const bucketSecs = 60;
        const barWidthPct = Math.min(MAX_BAR_WIDTH_PCT, Math.max(0.5, (bucketSecs / viewWindow) * 100 * 0.8));
        const bars: { x: number; y: number; height: number; color: string; front: boolean }[] = [];
        const tooltipBuckets = new Map<number, { packets: number; flags: number }>();

        const barHeight = (value: number) => {
            if (value <= 0) return 0;
            return Math.max((value / maxBar) * 100, MIN_BAR_HEIGHT);
        };

        for (const ts of timestamps) {
            const b = bucketMap.get(ts);
            if (!b || (b.packet_count === 0 && b.flag_count === 0)) continue;

            const x = ((ts - viewLeft) / viewWindow) * 100 - barWidthPct / 2;
            const packetsHeight = barHeight(b.packet_count);
            const flagsHeight = barHeight(b.flag_count);

            const packetsFront = b.flag_count === 0 || packetsHeight < flagsHeight;
            if (packetsHeight > 0) {
                bars.push({ x, y: 100 - packetsHeight, height: packetsHeight, color: PACKETS_COLOR, front: packetsFront });
            }
            if (flagsHeight > 0) {
                bars.push({ x, y: 100 - flagsHeight, height: flagsHeight, color: FLAGS_COLOR, front: !packetsFront || b.packet_count === 0 });
            }
            tooltipBuckets.set(ts, { packets: b.packet_count, flags: b.flag_count });
        }

        // Time axis ticks
        const tickCount = Math.min(8, Math.max(3, Math.floor(viewWindow / 300))); // tick every ~5 min
        const tickStep = viewWindow / tickCount;
        const ticks: { time: number; pct: number }[] = [];
        for (let i = 0; i <= tickCount; i++) {
            const t = viewLeft + i * tickStep;
            ticks.push({ time: t, pct: (i / tickCount) * 100 });
        }

        return { bars, barWidthPct, ticks, viewLeft, viewRight, viewWindow, tooltipBuckets, maxBar };
    }, [data, viewOffset]);

    // Pan buttons — shift viewport by 5 minutes
    const scrollLeft = useCallback(() => setViewOffset(prev => prev - 300), []);
    const scrollRight = useCallback(() => setViewOffset(prev => prev + 300), []);
    const resetToLive = useCallback(() => setViewOffset(0), []);
    const isLive = viewOffset === 0;

    // Middle-click drag panning
    const panRef = useRef<{ startX: number; startOffset: number } | null>(null);

    useEffect(() => {
        const chart = chartRef.current;
        if (!chart) return;

        const handleMidDown = (e: MouseEvent) => {
            if (e.button !== 1) return; // middle click only
            e.preventDefault();
            panRef.current = { startX: e.clientX, startOffset: viewOffset };
        };

        const handleMidMove = (e: MouseEvent) => {
            if (!panRef.current || !chartData) return;
            const dx = e.clientX - panRef.current.startX;
            const chartWidth = chart.getBoundingClientRect().width;
            const pctShift = (dx / chartWidth) * 100;
            const timeShift = (pctShift / 100) * chartData.viewWindow;
            setViewOffset(panRef.current.startOffset - timeShift);
        };

        const handleMidUp = () => { panRef.current = null; };

        chart.addEventListener('mousedown', handleMidDown);
        window.addEventListener('mousemove', handleMidMove);
        window.addEventListener('mouseup', handleMidUp);
        return () => {
            chart.removeEventListener('mousedown', handleMidDown);
            window.removeEventListener('mousemove', handleMidMove);
            window.removeEventListener('mouseup', handleMidUp);
        };
    }, [viewOffset, chartData]);

    // Convert client X to percentage
    const pxToPct = useCallback((clientX: number): number => {
        if (!chartRef.current) return 0;
        const rect = chartRef.current.getBoundingClientRect();
        return Math.max(0, Math.min(100, ((clientX - rect.left) / rect.width) * 100));
    }, []);

    // Convert percentage to time
    const pctToTime = useCallback((pct: number): number => {
        if (!chartData) return 0;
        return chartData.viewLeft + (pct / 100) * chartData.viewWindow;
    }, [chartData]);

    // Hover tooltip: nearest bucket under the cursor + its per-service counts
    const [hover, setHover] = useState<{ x: number; y: number; ts: number } | null>(null);

    const handleChartMouseMove = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
        if (!chartData || chartData.tooltipBuckets.size === 0) { setHover(null); return; }
        const time = pctToTime(pxToPct(e.clientX));
        let bestTs: number | null = null;
        let bestDist = Infinity;
        for (const ts of chartData.tooltipBuckets.keys()) {
            const d = Math.abs(ts + 30 - time); // bucket covers [ts, ts+60)
            if (d < bestDist) { bestDist = d; bestTs = ts; }
        }
        if (bestTs === null || bestDist > 90) { setHover(null); return; }
        setHover({ x: e.clientX, y: e.clientY, ts: bestTs });
    }, [chartData, pxToPct, pctToTime]);

    const handleChartMouseLeave = useCallback(() => setHover(null), []);

    const hoverEntries = hover && chartData ? chartData.tooltipBuckets.get(hover.ts) : undefined;

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

    // Expose clear function to parent
    useEffect(() => {
        if (clearRef) clearRef.current = () => { setDrag({ kind: 'idle' }); setSelectionRect(null); };
        return () => { if (clearRef) clearRef.current = null; };
    }, [clearRef]);

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
                    onMouseMove={handleChartMouseMove}
                    onMouseLeave={handleChartMouseLeave}
                >
                    <div className="timeline-bar-controls" onMouseDown={e => e.stopPropagation()}>
                        <button className="timeline-bar-scroll" onClick={scrollLeft} title="Scroll left 5 min">◀</button>
                        {!isLive && <button className="timeline-bar-live" onClick={resetToLive} title="Reset to live">Live</button>}
                        <button className="timeline-bar-scroll" onClick={scrollRight} title="Scroll right 5 min">▶</button>
                        <button className="timeline-bar-toggle" onClick={toggleCollapsed} title="Collapse timeline">▲</button>
                    </div>

                    <div className="timeline-bar-chart">
                        {chartData && chartData.bars.length > 0 && (
                            <>
                                <svg
                                    width="100%"
                                    height="100%"
                                    viewBox="0 0 100 100"
                                    preserveAspectRatio="none"
                                >
                                    {/* Back bars of the pairs — light */}
                                    {chartData.bars.filter(b => !b.front).map((bar, i) => (
                                        <rect
                                            key={`back${i}`}
                                            x={bar.x}
                                            y={bar.y}
                                            width={chartData.barWidthPct}
                                            height={bar.height}
                                            fill={bar.color}
                                            opacity={0.45}
                                            rx={0.3}
                                        />
                                    ))}
                                    {/* Front bars — dense, painted over the back ones */}
                                    {chartData.bars.filter(b => b.front).map((bar, i) => (
                                        <rect
                                            key={`front${i}`}
                                            x={bar.x}
                                            y={bar.y}
                                            width={chartData.barWidthPct}
                                            height={bar.height}
                                            fill={bar.color}
                                            opacity={0.95}
                                            rx={0.3}
                                        />
                                    ))}
                                </svg>
                                <div className="timeline-bar-legend" onMouseDown={e => e.stopPropagation()}>
                                    <span><i className="legend-swatch legend-packets" /> пакеты</span>
                                    <span><i className="legend-swatch legend-flags" /> флаги</span>
                                </div>
                            </>
                        )}

                        {selectionRect && (
                            <div
                                className={`timeline-bar-selection${selectionRect.interactive ? ' interactive' : ''}`}
                                style={{
                                    left: `${selectionRect.left}%`,
                                    width: `${selectionRect.right - selectionRect.left}%`,
                                }}
                                onMouseDown={selectionRect.interactive ? () => {
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

            {hoverEntries && hover && (() => {
                const W = 200;
                const x = Math.max(8, Math.min(hover.x + 14, window.innerWidth - W - 8));
                const y = Math.max(8, hover.y + 16);
                const d = new Date(hover.ts * 1000);
                const hhmm = `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
                return (
                    <div className="timeline-bar-tooltip" style={{ left: x, top: y }}>
                        <div className="timeline-bar-tooltip-time">{hhmm}</div>
                        <div className="timeline-bar-tooltip-row">
                            <span className="timeline-bar-tooltip-dot" style={{ background: PACKETS_COLOR }} />
                            <span className="timeline-bar-tooltip-name">пакеты</span>
                            <span className="timeline-bar-tooltip-count">{hoverEntries.packets}</span>
                        </div>
                        <div className="timeline-bar-tooltip-row">
                            <span className="timeline-bar-tooltip-dot" style={{ background: FLAGS_COLOR }} />
                            <span className="timeline-bar-tooltip-name">флаги</span>
                            <span className="timeline-bar-tooltip-count">{hoverEntries.flags}</span>
                        </div>
                    </div>
                );
            })()}
        </>
    );
}
