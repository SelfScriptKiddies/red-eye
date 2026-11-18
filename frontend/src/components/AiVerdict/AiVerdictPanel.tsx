// JEV verdict badge, history and detail modal for one stream.
// Provider-produced strings are rendered exclusively as React text nodes
// (default escaping, never dangerouslySetInnerHTML).

import { useCallback, useEffect, useState } from 'react';
import { subscribeWs } from '../../lib/wsBus';
import {
    fetchAiVerdicts,
    formatVerdictTime,
    isAiVerdictEvent,
    severityLabel,
    topNouls,
    verdictBadgeClass,
    type AiVerdict,
} from '../../lib/aiVerdict';
import './AiVerdict.css';

const HISTORY_LIMIT = 20;

function noulRow([name, p]: [string, number]): JSX.Element {
    return (
        <div key={name} className="jev-noul-row">
            <span className="jev-noul-name">{name}</span>
            <span className="jev-noul-bar-wrap">
                <span className="jev-noul-bar" style={{ width: `${Math.round(p * 100)}%` }} />
            </span>
            <span className="jev-noul-value">{p.toFixed(3)}</span>
        </div>
    );
}

export function AiVerdictPanel({ streamId }: { streamId: number }) {
    const [latest, setLatest] = useState<AiVerdict | null>(null);
    const [history, setHistory] = useState<AiVerdict[]>([]);
    const [historyOpen, setHistoryOpen] = useState(false);
    const [detail, setDetail] = useState<AiVerdict | null>(null);
    const [failed, setFailed] = useState(false);

    const load = useCallback(async () => {
        try {
            const rows = await fetchAiVerdicts(streamId, false, HISTORY_LIMIT);
            setHistory(rows);
            setLatest(rows.length > 0 ? rows[0] : null);
            setFailed(false);
        } catch {
            // The panel is informational: a failed load hides it instead
            // of spamming the operator.
            setFailed(true);
        }
    }, [streamId]);

    useEffect(() => {
        setLatest(null);
        setHistory([]);
        setHistoryOpen(false);
        setDetail(null);
        load();

        // Deep link: /packets?stream=N&verdict=V opens that revision.
        const verdictParam = new URLSearchParams(window.location.search).get('verdict');
        if (verdictParam) {
            const vid = parseInt(verdictParam, 10);
            if (Number.isFinite(vid) && history.length > 0) {
                const found = history.find(h => h.id === vid) ?? null;
                setDetail(found);
            }
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [streamId, load]);

    useEffect(() => {
        const verdictParam = new URLSearchParams(window.location.search).get('verdict');
        if (!verdictParam || history.length === 0) return;
        const vid = parseInt(verdictParam, 10);
        if (Number.isFinite(vid)) {
            setDetail(history.find(h => h.id === vid) ?? null);
        }
    }, [history]);

    useEffect(() => {
        // Live refresh: the backend broadcasts every successful verdict.
        const unsubscribe = subscribeWs({
            onMessage: (data) => {
                if (isAiVerdictEvent(data) && data.stream_id === streamId) {
                    load();
                }
            },
        });
        return unsubscribe;
    }, [streamId, load]);

    if (failed) return null;

    if (!latest) {
        return (
            <span className="jev-panel jev-panel-empty" title="JEV: вердиктов для стрима пока нет">
                JEV: -
            </span>
        );
    }

    return (
        <span className="jev-panel">
            <button
                className={`jev-badge ${verdictBadgeClass(latest.attack_class)} ${latest.is_attack ? 'jev-attack' : 'jev-benign'}`}
                onClick={() => setDetail(latest)}
                title={`Ревизия #${latest.id}: пакеты ${latest.first_packet_id}..${latest.last_packet_id}, модель ${latest.model}`}
            >
                <span className="jev-badge-class">{latest.attack_class}</span>
                <span className="jev-badge-meta">
                    sev {latest.severity.toFixed(1)} ({severityLabel(latest.severity)}) · conf {Math.round(latest.confidence * 100)}% · {formatVerdictTime(latest.created_at_unix)}
                    {latest.truncated && <span className="jev-truncated-flag"> · усечён</span>}
                </span>
            </button>
            {history.length > 1 && (
                <button
                    className="jev-history-toggle"
                    onClick={() => setHistoryOpen(v => !v)}
                    title={`История вердиктов (${history.length})`}
                >
                    {historyOpen ? 'скрыть историю' : `история (${history.length})`}
                </button>
            )}
            {historyOpen && (
                <div className="jev-history">
                    {history.map(v => (
                        <button key={v.id} className="jev-history-row" onClick={() => setDetail(v)}>
                            <span className={`jev-history-dot ${v.is_attack ? 'jev-attack' : 'jev-benign'}`} />
                            <span className="jev-history-class">{v.attack_class}</span>
                            <span className="jev-history-range">пакеты {v.first_packet_id}..{v.last_packet_id}</span>
                            <span className="jev-history-time">{formatVerdictTime(v.created_at_unix)}</span>
                        </button>
                    ))}
                </div>
            )}
            {detail && <VerdictDetailModal verdict={detail} onClose={() => setDetail(null)} />}
        </span>
    );
}

function VerdictDetailModal({ verdict, onClose }: { verdict: AiVerdict; onClose: () => void }) {
    const answers = verdict.answers;
    const nouls = Object.entries(answers?.nouls ?? {});
    const noulsSorted = topNouls(
        Object.fromEntries(nouls) as Record<string, number>,
        nouls.length
    );
    const choice = answers?.attack_class;
    const severity = answers?.severity;

    return (
        <div className="jev-modal-backdrop" onClick={onClose}>
            <div className="jev-modal" onClick={e => e.stopPropagation()}>
                <div className="jev-modal-header">
                    <span className={`jev-badge ${verdictBadgeClass(verdict.attack_class)} ${verdict.is_attack ? 'jev-attack' : 'jev-benign'}`}>
                        {verdict.attack_class}
                    </span>
                    <span className="jev-modal-title">
                        Стрим #{verdict.stream_id} · вердикт #{verdict.id} · {formatVerdictTime(verdict.created_at_unix)}
                    </span>
                    <button className="jev-modal-close" onClick={onClose}>✕</button>
                </div>

                <div className="jev-modal-grid">
                    <div><b>Вердикт:</b> {verdict.is_attack ? 'атака' : 'не атака'} (порог {verdict.ai_threshold})</div>
                    <div><b>Тяжесть:</b> {verdict.severity.toFixed(2)} - {severityLabel(verdict.severity)}</div>
                    <div><b>Пакеты:</b> {verdict.first_packet_id}..{verdict.last_packet_id} (проанализировано {verdict.analyzed_packet_count}, пропущено {verdict.omitted_packet_count} / {verdict.omitted_byte_count} байт)</div>
                    <div>
                        <b>Контекст:</b> {verdict.truncated ? 'усечён - не полный' : 'полный'}
                    </div>
                    <div><b>Модель:</b> {verdict.model} · prompt v{verdict.prompt_version}</div>
                    <div><b>Стоимость:</b> {verdict.cost != null ? `$${verdict.cost.toFixed(6)}` : 'нет данных'} · токены: {verdict.input_tokens ?? '?'}</div>
                    <div className="jev-sha"><b>SHA-256 транскрипта:</b> <code>{verdict.transcript_sha256.slice(0, 16)}...</code></div>
                </div>

                <div className="jev-section">
                    <h5>Атомарные вопросы (noul, P(yes))</h5>
                    {noulsSorted.map(noulRow)}
                </div>

                {choice && (
                    <div className="jev-section">
                        <h5>Класс атаки (choice)</h5>
                        <div className="jev-noul-row">
                            <span className="jev-noul-name">{choice.choice}</span>
                            <span className="jev-noul-bar-wrap">
                                <span className="jev-noul-bar" style={{ width: `${Math.round(choice.confidence * 100)}%` }} />
                            </span>
                            <span className="jev-noul-value">conf {choice.confidence.toFixed(3)}</span>
                        </div>
                    </div>
                )}

                {severity && (
                    <div className="jev-section">
                        <h5>Тяжесть (score)</h5>
                        <div className="jev-noul-row">
                            <span className="jev-noul-name">{severity.score.toFixed(2)}</span>
                            <span className="jev-noul-bar-wrap">
                                <span className="jev-noul-bar" style={{ width: `${Math.round((severity.score / 3) * 100)}%` }} />
                            </span>
                            <span className="jev-noul-value">conf {severity.confidence.toFixed(3)}</span>
                        </div>
                    </div>
                )}

                <p className="jev-disclaimer">
                    Вердикт JEV - информационный: он ничего не блокирует и не влияет на агентов.
                </p>
            </div>
        </div>
    );
}
