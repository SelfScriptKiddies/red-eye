import { useMemo } from 'react';
import './Timeline.css';

interface Stream {
    id: number;
    first_packet_time: number;
    last_packet_time: number;
    l7_protocol?: string;
    tags?: string[];
    packet_count?: number;
}

interface TimelineProps {
    streams: Stream[];
    selectedStreamId?: number;
    onStreamClick: (_streamId: string) => void;
}

export function Timeline({ streams, selectedStreamId, onStreamClick }: TimelineProps) {
    const layout = useMemo(() => {
        if (streams.length === 0) return null;

        const allTimes = streams.flatMap(s => [s.first_packet_time, s.last_packet_time]);
        const minTime = Math.min(...allTimes);
        const maxTime = Math.max(...allTimes);
        const range = Math.max(maxTime - minTime, 1);

        // Generate time ticks (5-8 ticks)
        const tickCount = Math.min(8, Math.max(3, Math.floor(range / 30)));
        const tickStep = range / tickCount;
        const ticks: { time: number; pct: number }[] = [];
        for (let i = 0; i <= tickCount; i++) {
            const t = minTime + i * tickStep;
            ticks.push({ time: t, pct: ((t - minTime) / range) * 100 });
        }

        // Lay out bars in rows to avoid overlap
        const bars: { stream: Stream; left: number; width: number; row: number; cls: string }[] = [];
        const rowEnds: number[] = []; // tracks where each row ends (in %)

        const sorted = [...streams].sort((a, b) => a.first_packet_time - b.first_packet_time);

        for (const s of sorted) {
            const left = ((s.first_packet_time - minTime) / range) * 100;
            const width = Math.max(((s.last_packet_time - s.first_packet_time) / range) * 100, 0.5);

            // Find first row where this bar fits
            let row = 0;
            while (row < rowEnds.length && rowEnds[row] > left - 0.3) {
                row++;
            }
            if (row >= rowEnds.length) rowEnds.push(0);
            rowEnds[row] = left + width;

            const hasFlag = s.tags?.some(t => t.toLowerCase().includes('flag'));
            const proto = s.l7_protocol || 'unknown';
            const cls = hasFlag ? 'flag' : proto;

            bars.push({ stream: s, left, width, row, cls });
        }

        return { ticks, bars, rowCount: rowEnds.length, minTime, maxTime };
    }, [streams]);

    if (!layout || streams.length === 0) {
        return (
            <div className="timeline-container">
                <div className="timeline-empty">No streams</div>
            </div>
        );
    }

    const formatTime = (ts: number) => {
        const d = new Date(ts * 1000);
        return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')}`;
    };

    const barAreaHeight = 56; // px (80 total - 20 axis - 4 top padding)
    const barHeight = Math.min(6, Math.max(3, barAreaHeight / Math.max(layout.rowCount, 1) - 1));
    const rowHeight = barHeight + 2;

    return (
        <div className="timeline-container">
            <div className="timeline-bars">
                {layout.bars.map(({ stream: s, left, width, row, cls }) => (
                    <div
                        key={s.id}
                        className={`timeline-bar timeline-bar--${cls} ${s.id === selectedStreamId ? 'timeline-bar--selected' : ''}`}
                        style={{
                            left: `${left}%`,
                            width: `${width}%`,
                            top: `${row * rowHeight}px`,
                            height: `${barHeight}px`,
                        }}
                        title={`#${s.id} ${s.l7_protocol || ''} ${s.packet_count || 0} pkt`}
                        onClick={() => onStreamClick(String(s.id))}
                    />
                ))}
            </div>
            <div className="timeline-axis">
                {layout.ticks.map((tick, i) => (
                    <span
                        key={i}
                        className="timeline-tick"
                        style={{ left: `${tick.pct}%` }}
                    >
                        {formatTime(tick.time)}
                    </span>
                ))}
            </div>
        </div>
    );
}
