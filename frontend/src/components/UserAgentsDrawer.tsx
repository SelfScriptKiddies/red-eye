import { useCallback, useEffect, useRef, useState } from 'react';
import {
    copyText,
    fetchUserAgents,
    UserAgentItem,
    UserAgentsOrder,
    UserAgentsSort,
} from '../lib/api';
import './UserAgentsDrawer.css';

interface DrawerAgent {
    agent_id: string;
}

interface UserAgentsDrawerProps {
    serviceId: number;
    serviceName: string;
    agents: DrawerAgent[];
    onClose: () => void;
    /** Ordinary value click: exact drill-down on the packets page. */
    onDrillDown: (value: string, agentId?: string) => void;
}

/** Refresh floor: the listing polls no more often than this while open. */
const REFRESH_INTERVAL_MS = 5000;

function formatTime(ts: number): string {
    if (!ts) return '-';
    return new Date(ts * 1000).toLocaleString();
}

function OverflowRow({ item }: { item: UserAgentItem }) {
    return (
        <div className="ua-row ua-row--overflow" title="Наблюдаемы, но не показываются как точные значения">
            <div className="ua-value ua-value--overflow">
                Другие или слишком длинные User-Agent (переполнение)
            </div>
            <div className="ua-meta">
                <span className="ua-count">{item.message_count}</span>
                <span className="ua-time" title="Первое появление">{formatTime(item.first_seen)}</span>
                <span className="ua-time" title="Последнее появление">{formatTime(item.last_seen)}</span>
                <span className="ua-agents">{item.agent_ids.join(', ') || '-'}</span>
            </div>
        </div>
    );
}

export function UserAgentsDrawer({ serviceId, serviceName, agents, onClose, onDrillDown }: UserAgentsDrawerProps) {
    const [items, setItems] = useState<UserAgentItem[]>([]);
    const [nextCursor, setNextCursor] = useState<string | null>(null);
    const [hasOverflow, setHasOverflow] = useState(false);
    const [aggregationStarted, setAggregationStarted] = useState<number | null>(null);
    const [sort, setSort] = useState<UserAgentsSort>('last_seen');
    const [order, setOrder] = useState<UserAgentsOrder>('desc');
    const [agentFilter, setAgentFilter] = useState<string>('');
    const [loading, setLoading] = useState(true);
    const [refreshing, setRefreshing] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [copied, setCopied] = useState<string | null>(null);

    const controllerRef = useRef<AbortController | null>(null);
    // Guards against out-of-order page responses after a filter switch:
    // only the newest request may touch the list.
    const requestIdRef = useRef(0);

    const load = useCallback(async (cursor: string | null, append: boolean) => {
        controllerRef.current?.abort();
        const controller = new AbortController();
        controllerRef.current = controller;
        const requestId = ++requestIdRef.current;
        if (append) {
            setRefreshing(true);
        } else {
            setLoading(true);
            setRefreshing(true);
        }
        try {
            const data = await fetchUserAgents(
                serviceId,
                { agentId: agentFilter || undefined, limit: 100, cursor, sort, order },
                controller.signal
            );
            if (requestId !== requestIdRef.current) return;
            setItems(prev => (append ? [...prev, ...data.items] : data.items));
            setNextCursor(data.next_cursor);
            setHasOverflow(data.has_overflow);
            setAggregationStarted(data.aggregation_started);
            setError(null);
        } catch (e) {
            if (controller.signal.aborted || requestId !== requestIdRef.current) return;
            // Keep the last successful listing visible (stale + error badge);
            // never clear or reorder it on a slow or failed query.
            setError(String(e));
        } finally {
            if (requestId === requestIdRef.current) {
                setLoading(false);
                setRefreshing(false);
            }
        }
    }, [serviceId, agentFilter, sort, order]);

    // Initial load + reload when the view parameters change.
    useEffect(() => {
        load(null, false);
    }, [load]);

    // Bounded refresh: at most one poll per 5 seconds, only while the
    // drawer is open (unmount = closed). Pending requests are aborted on
    // close, so no zombie refresh survives the drawer.
    useEffect(() => {
        const timer = setInterval(() => load(null, false), REFRESH_INTERVAL_MS);
        return () => {
            clearInterval(timer);
            controllerRef.current?.abort();
        };
    }, [load]);

    const ordinaryItems = items.filter(i => !i.is_overflow);
    const overflowItem = items.find(i => i.is_overflow);
    const showOverflowRow = hasOverflow || overflowItem != null;
    const emptyNoData = !error && !loading && ordinaryItems.length === 0 && !hasOverflow;
    // Filter options: the configured agent list plus every agent actually
    // seen in the data, so the filter works even before the agent list
    // loads.
    const agentOptions = Array.from(new Set([
        ...agents.map(a => a.agent_id),
        ...items.flatMap(i => i.agent_ids),
    ]));

    return (
        <div className="modal-overlay" onClick={onClose}>
            <div className="modal-content ua-drawer" onClick={(e) => e.stopPropagation()}>
                <div className="modal-header">
                    <h3>User-Agent: {serviceName}</h3>
                    <button className="close-btn" onClick={onClose}>&times;</button>
                </div>
                <div className="ua-toolbar">
                    <button
                        className={`ua-toggle ${sort === 'last_seen' ? 'ua-toggle--active' : ''}`}
                        onClick={() => setSort('last_seen')}
                    >
                        По времени
                    </button>
                    <button
                        className={`ua-toggle ${sort === 'count' ? 'ua-toggle--active' : ''}`}
                        onClick={() => setSort('count')}
                    >
                        По количеству
                    </button>
                    <button
                        className="ua-toggle"
                        onClick={() => setOrder(prev => (prev === 'desc' ? 'asc' : 'desc'))}
                        title="Направление сортировки"
                    >
                        {order === 'desc' ? 'desc' : 'asc'}
                    </button>
                    <select
                        className="select-input ua-agent-select"
                        value={agentFilter}
                        onChange={e => setAgentFilter(e.target.value)}
                        title="Фильтр по агенту"
                    >
                        <option value="">Все агенты</option>
                        {agentOptions.map(id => (
                            <option key={id} value={id}>{id}</option>
                        ))}
                    </select>
                    <span className={`ua-refresh ${refreshing ? 'ua-refresh--busy' : ''}`}>
                        {refreshing ? 'обновление...' : ''}
                    </span>
                </div>
                <div className="modal-body ua-list">
                    {error && items.length === 0 && (
                        <div className="ua-empty">
                            Не удалось загрузить список (ошибка backend).
                            <button className="ua-toggle" onClick={() => load(null, false)}>Повторить</button>
                        </div>
                    )}
                    {error && items.length > 0 && (
                        <div className="ua-error-note">
                            Данные могут быть устаревшими: {error}
                        </div>
                    )}
                    {loading && items.length === 0 && !error && (
                        <div className="ua-empty">Загрузка...</div>
                    )}
                    {emptyNoData && (
                        <div className="ua-empty">
                            User-Agent не наблюдались
                            {aggregationStarted == null
                                ? ' (агрегация ещё не начиналась; статистика появится для новых запросов)'
                                : ' с момента включения агрегации'}
                        </div>
                    )}
                    {ordinaryItems.map(item => (
                        <div
                            key={item.user_agent}
                            className="ua-row ua-row--clickable"
                            onClick={() => onDrillDown(item.user_agent!, agentFilter || undefined)}
                            title="Открыть стримы с этим точным значением"
                        >
                            <div className="ua-value">{item.user_agent}</div>
                            <div className="ua-meta">
                                <span className="ua-count">{item.message_count}</span>
                                <span className="ua-time" title="Первое появление">{formatTime(item.first_seen)}</span>
                                <span className="ua-time" title="Последнее появление">{formatTime(item.last_seen)}</span>
                                <span className="ua-agents">{item.agent_ids.join(', ') || '-'}</span>
                                <button
                                    className="ua-copy"
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        copyText(item.user_agent!);
                                        setCopied(item.user_agent);
                                        setTimeout(() => setCopied(null), 1500);
                                    }}
                                    title="Скопировать точное значение"
                                >
                                    {copied === item.user_agent ? 'ok' : 'copy'}
                                </button>
                            </div>
                        </div>
                    ))}
                    {nextCursor && (
                        <button
                            className="ua-more"
                            onClick={() => load(nextCursor, true)}
                            disabled={refreshing}
                        >
                            Ещё (следующая страница)
                        </button>
                    )}
                    {showOverflowRow && overflowItem && <OverflowRow item={overflowItem} />}
                    {showOverflowRow && !overflowItem && (
                        <div className="ua-row ua-row--overflow">
                            <div className="ua-value ua-value--overflow">
                                Другие или слишком длинные User-Agent (переполнение)
                            </div>
                        </div>
                    )}
                </div>
                <div className="modal-footer ua-footer">
                    <span className="ua-note">
                        {aggregationStarted == null
                            ? 'Агрегация ещё не началась.'
                            : `Агрегация ведётся с ${formatTime(aggregationStarted)}; список охватывает только данные после этого момента.`}
                    </span>
                </div>
            </div>
        </div>
    );
}
