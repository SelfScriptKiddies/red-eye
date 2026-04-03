import { useState, useEffect } from 'react';
import './MainPage.css';

interface Agent {
    agent_id: string;
    uptime_secs: number;
    sniffer_ports: number[];
    harpoon_rules: string[];
    active_filters: number;
    online: boolean;
}

interface TagMetric {
    tag_id: number;
    tag_name: string;
    timestamp: number;
    count: number;
}

interface StreamSummary {
    id: number;
    service_name: string;
    l7_protocol: string;
    packet_count: number;
    first_packet_time: number;
    last_packet_time: number;
    tags: string[];
    favorite: boolean;
}

interface FarmStatus {
    farm_url: string;
    farm_enabled: string;
}

export function MainPage() {
    const [agents, setAgents] = useState<Agent[]>([]);
    const [metrics, setMetrics] = useState<TagMetric[]>([]);
    const [streams, setStreams] = useState<StreamSummary[]>([]);
    const [farmStatus, setFarmStatus] = useState<FarmStatus>({ farm_url: '', farm_enabled: 'false' });

    const fetchAll = () => {
        fetch('/api/agent').then(r => r.json()).then(d => setAgents(Array.isArray(d) ? d : [])).catch(() => {});
        fetch('/api/tag/metrics?minutes=30').then(r => r.json()).then(d => setMetrics(Array.isArray(d) ? d : [])).catch(() => {});
        fetch('/api/stream?limit=10&offset=0').then(r => r.json()).then(d => setStreams(Array.isArray(d) ? d : [])).catch(() => {});
        fetch('/api/settings').then(r => r.json()).then(d => setFarmStatus(d)).catch(() => {});
    };

    useEffect(() => {
        fetchAll();
        const interval = setInterval(fetchAll, 10000);
        return () => clearInterval(interval);
    }, []);

    const onlineAgents = agents.filter(a => a.online);
    const totalProxyRules = agents.reduce((sum, a) => sum + a.harpoon_rules.length, 0);
    const totalFilters = agents.reduce((sum, a) => sum + a.active_filters, 0);
    const farmActive = farmStatus.farm_enabled === 'true' && farmStatus.farm_url;

    // Aggregate metrics by tag name for sparkline
    const tagTotals: Record<string, number> = {};
    for (const m of metrics) {
        tagTotals[m.tag_name] = (tagTotals[m.tag_name] || 0) + m.count;
    }
    const sortedTags = Object.entries(tagTotals).sort((a, b) => b[1] - a[1]);

    // Build sparkline data per tag (last 30 minutes, per-minute buckets)
    const buildSparkline = (tagName: string): number[] => {
        const now = Math.floor(Date.now() / 1000);
        const buckets = new Array(30).fill(0);
        for (const m of metrics) {
            if (m.tag_name !== tagName) continue;
            const minuteAgo = Math.floor((now - m.timestamp) / 60);
            if (minuteAgo >= 0 && minuteAgo < 30) {
                buckets[29 - minuteAgo] += m.count;
            }
        }
        return buckets;
    };

    const renderSparkline = (data: number[]) => {
        const max = Math.max(...data, 1);
        const h = 24;
        const w = 120;
        const step = w / data.length;
        const points = data.map((v, i) => `${i * step},${h - (v / max) * h}`).join(' ');
        return (
            <svg width={w} height={h} className="sparkline">
                <polyline points={points} fill="none" stroke="#646cff" strokeWidth="1.5" />
            </svg>
        );
    };

    const formatTime = (ts: number) => new Date(ts * 1000).toLocaleTimeString();

    return (
        <div className="MainPage">
            {/* Stats Row */}
            <div className="stats-row">
                <div className="stat-card">
                    <div className="stat-value">{onlineAgents.length}<span className="stat-total">/{agents.length}</span></div>
                    <div className="stat-label">Агенты онлайн</div>
                </div>
                <div className="stat-card">
                    <div className="stat-value">{totalProxyRules}</div>
                    <div className="stat-label">Proxy правила</div>
                </div>
                <div className="stat-card">
                    <div className="stat-value">{totalFilters}</div>
                    <div className="stat-label">Активных фильтров</div>
                </div>
                <div className="stat-card">
                    <div className={`stat-value ${farmActive ? 'stat-active' : 'stat-inactive'}`}>
                        {farmActive ? 'ON' : 'OFF'}
                    </div>
                    <div className="stat-label">Flag Farm</div>
                </div>
            </div>

            <div className="dashboard-columns">
                {/* Tag Metrics */}
                <div className="dashboard-column">
                    <h3 className="dashboard-section-title">Активность тегов (30 мин)</h3>
                    {sortedTags.length > 0 ? (
                        <div className="tag-metrics-list">
                            {sortedTags.map(([name, total]) => (
                                <div key={name} className="tag-metric-row">
                                    <span className="tag-metric-name">{name}</span>
                                    {renderSparkline(buildSparkline(name))}
                                    <span className="tag-metric-count">{total}</span>
                                </div>
                            ))}
                        </div>
                    ) : (
                        <div className="empty-state">Нет срабатываний</div>
                    )}
                </div>

                {/* Recent Streams */}
                <div className="dashboard-column">
                    <h3 className="dashboard-section-title">Последние стримы</h3>
                    {streams.length > 0 ? (
                        <div className="recent-streams-list">
                            {streams.map(s => (
                                <div key={s.id} className="recent-stream-row">
                                    <div className="recent-stream-info">
                                        <span className="recent-stream-service">{s.service_name}</span>
                                        {s.l7_protocol !== 'unknown' && (
                                            <span className="badge info-badge" style={{ fontSize: '10px', padding: '1px 5px' }}>
                                                {s.l7_protocol}
                                            </span>
                                        )}
                                        {s.tags.map((t, i) => (
                                            <span key={i} className="badge option-badge" style={{ fontSize: '10px', padding: '1px 5px' }}>
                                                {t}
                                            </span>
                                        ))}
                                    </div>
                                    <div className="recent-stream-meta">
                                        <span>{s.packet_count} pkt</span>
                                        <span className="recent-stream-time">{formatTime(s.last_packet_time)}</span>
                                    </div>
                                </div>
                            ))}
                        </div>
                    ) : (
                        <div className="empty-state">Нет стримов</div>
                    )}
                </div>

                {/* Agents */}
                <div className="dashboard-column">
                    <h3 className="dashboard-section-title">Агенты</h3>
                    {agents.map(a => (
                        <div key={a.agent_id} className="agent-summary-row">
                            <div className="agent-summary-left">
                                <span className={`status-dot ${a.online ? 'online' : 'offline'}`} />
                                <span className="agent-summary-name">{a.agent_id}</span>
                            </div>
                            <div className="agent-summary-right">
                                {a.harpoon_rules.length > 0 && (
                                    <span className="badge option-badge" style={{ fontSize: '10px', padding: '1px 5px' }}>
                                        {a.harpoon_rules.length} proxy
                                    </span>
                                )}
                                <span className="agent-summary-ports">
                                    {a.sniffer_ports.length > 0 ? a.sniffer_ports.map(p => `:${p}`).join(' ') : ''}
                                </span>
                            </div>
                        </div>
                    ))}
                    {agents.length === 0 && <div className="empty-state">Нет агентов</div>}
                </div>
            </div>
        </div>
    );
}
