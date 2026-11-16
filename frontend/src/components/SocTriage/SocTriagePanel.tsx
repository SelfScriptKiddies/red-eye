// SOC triage panel for one stream: manual trigger, queue/result state and
// revision history. All model and operator strings render as React text
// nodes (default escaping, never dangerouslySetInnerHTML). Recommended
// action is TEXT ONLY: nothing in this panel executes it.

import { useCallback, useEffect, useState } from 'react';
import { subscribeWs } from '../../lib/wsBus';
import {
    fetchSocResult,
    fetchSocResults,
    formatSocTime,
    isSocResultEvent,
    isSocTriageEvent,
    socVerdictClass,
    submitSocTriage,
    type SocResult,
} from '../../lib/socTriage';
import './SocTriage.css';

const HISTORY_LIMIT = 20;

const VERDICT_LABELS: Record<string, string> = {
    benign: 'benign',
    suspicious: 'suspicious',
    likely_attack: 'likely attack',
    unknown: 'unknown / недостаточно данных',
};

// The stream deep link is the only cross-page contract PacketsPage
// supports today; the packet/message id stays in the label text.
function evidenceHref(result: SocResult): string {
    return `/packets?stream=${result.stream_id}`;
}

export function SocTriagePanel({ streamId }: { streamId: number }) {
    const [status, setStatus] = useState<{ enabled: boolean; manualReason: string } | null>(null);
    const [results, setResults] = useState<SocResult[]>([]);
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [notice, setNotice] = useState<string | null>(null);
    const [detail, setDetail] = useState<SocResult | null>(null);
    const [historyOpen, setHistoryOpen] = useState(false);

    const load = useCallback(async () => {
        try {
            const rows = await fetchSocResults(streamId, HISTORY_LIMIT);
            setResults(rows);
        } catch {
            // Informational panel: hide history on failure, never spam.
        }
    }, [streamId]);

    const loadStatus = useCallback(async () => {
        try {
            const st = await fetch('/api/soc/status').then(r => r.json());
            setStatus({
                enabled: Boolean(st.enabled),
                manualReason: st?.matrix?.manual?.reason ?? '',
            });
        } catch {
            setStatus({ enabled: false, manualReason: 'SOC status unavailable' });
        }
    }, []);

    useEffect(() => {
        setResults([]);
        setDetail(null);
        setError(null);
        setNotice(null);
        load();
        loadStatus();
    }, [streamId, load, loadStatus]);

    useEffect(() => {
        const unsubscribe = subscribeWs({
            onMessage: (data) => {
                if (isSocResultEvent(data) && data.stream_id === streamId) {
                    load();
                } else if (isSocTriageEvent(data) && data.stream_id === streamId) {
                    load();
                }
            },
        });
        return unsubscribe;
    }, [streamId, load]);

    const submit = async (fresh: boolean) => {
        setError(null);
        setNotice(null);
        setBusy(true);
        try {
            const res = await submitSocTriage(streamId, fresh);
            setNotice(
                res.deduplicated
                    ? `Уже в очереди (задача #${res.job_id})`
                    : `Задача #${res.job_id} поставлена в очередь`
            );
            await load();
        } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            setError(msg.includes('SOC disabled') ? msg : `Не удалось поставить задачу: ${msg}`);
        } finally {
            setBusy(false);
        }
    };

    const openDetail = async (id: number) => {
        try {
            setDetail(await fetchSocResult(id));
        } catch {
            // Row may have been trimmed between list and detail: keep the
            // panel usable.
            setError('Результат недоступен (возможно, удалён)');
        }
    };

    const latest = results[0] ?? null;

    if (status && !status.enabled) {
        return (
            <span className="soc-panel soc-panel-disabled" title={status.manualReason}>
                SOC: выключен
            </span>
        );
    }

    return (
        <span className="soc-panel">
            <button
                className={`soc-trigger ${busy ? 'soc-busy' : ''}`}
                onClick={() => submit(false)}
                disabled={busy}
                title="Поставить стрим в очередь LLM-триажа (без JEV; результат информационный)"
            >
                SOC: {busy ? '...' : 'триаж'}
            </button>
            {latest && (
                <button
                    className={`soc-verdict-badge ${socVerdictClass(latest.verdict)}`}
                    onClick={() => openDetail(latest.id)}
                    title={`Ревизия до пакета #${latest.stream_revision}, модель ${latest.model}`}
                >
                    <span className="soc-verdict-name">{VERDICT_LABELS[latest.verdict] ?? latest.verdict}</span>
                    <span className="soc-verdict-meta">
                        conf {Math.round(latest.confidence * 100)}% · sev {latest.severity} · {formatSocTime(latest.created_at_unix)}
                    </span>
                </button>
            )}
            {results.length > 1 && (
                <button
                    className="soc-history-toggle"
                    onClick={() => setHistoryOpen(v => !v)}
                    title={`История результатов (${results.length})`}
                >
                    {historyOpen ? 'скрыть историю' : `история (${results.length})`}
                </button>
            )}
            {historyOpen && (
                <div className="soc-history">
                    {results.map(r => (
                        <button key={r.id} className="soc-history-row" onClick={() => openDetail(r.id)}>
                            <span className={`soc-history-dot ${socVerdictClass(r.verdict)}`} />
                            <span className="soc-history-verdict">{VERDICT_LABELS[r.verdict] ?? r.verdict}</span>
                            <span className="soc-history-rev">ревизия #{r.stream_revision}</span>
                            <span className="soc-history-time">{formatSocTime(r.created_at_unix)}</span>
                        </button>
                    ))}
                </div>
            )}
            {notice && <span className="soc-note soc-note-ok">{notice}</span>}
            {error && <span className="soc-note soc-note-error">{error}</span>}
            {detail && <ResultDetailModal result={detail} onClose={() => setDetail(null)} />}
        </span>
    );
}

function ResultDetailModal({ result, onClose }: { result: SocResult; onClose: () => void }) {
    return (
        <div className="soc-modal-backdrop" onClick={onClose}>
            <div className="soc-modal" onClick={e => e.stopPropagation()}>
                <div className="soc-modal-header">
                    <span className={`soc-verdict-badge ${socVerdictClass(result.verdict)}`}>
                        <span className="soc-verdict-name">{VERDICT_LABELS[result.verdict] ?? result.verdict}</span>
                    </span>
                    <span className="soc-modal-title">
                        Стрим #{result.stream_id} · результат #{result.id} · ревизия #{result.stream_revision} · {formatSocTime(result.created_at_unix)}
                    </span>
                    <button className="soc-modal-close" onClick={onClose}>✕</button>
                </div>

                <div className="soc-modal-grid">
                    <div><b>Уверенность:</b> {Math.round(result.confidence * 100)}%</div>
                    <div><b>Тяжесть:</b> {result.severity} (0..3)</div>
                    <div><b>Модель:</b> {result.model} · prompt v{result.prompt_version}</div>
                    <div>
                        <b>Токены:</b> {result.input_tokens ?? '?'} вход / {result.output_tokens ?? '?'} выход ·{' '}
                        <b>Время:</b> {result.duration_ms != null ? `${result.duration_ms} мс` : '?'}
                    </div>
                </div>

                <div className="soc-section">
                    <h5>Вывод</h5>
                    <p className="soc-summary">{result.summary}</p>
                </div>

                {result.evidence?.length > 0 && (
                    <div className="soc-section">
                        <h5>Доказательства ({result.evidence.length})</h5>
                        {result.evidence.map((item, i) => {
                            const href = evidenceHref(result);
                            const idLabel = item.packet_id != null
                                ? `пакет #${item.packet_id}`
                                : item.message_id != null
                                    ? `HTTP-сообщение #${item.message_id}`
                                    : '?';
                            return (
                                <div key={i} className="soc-evidence-row">
                                    {href ? <a href={href}>{idLabel}</a> : <span>{idLabel}</span>}
                                    <span className="soc-evidence-explanation">{item.explanation}</span>
                                </div>
                            );
                        })}
                    </div>
                )}

                {result.indicators?.length > 0 && (
                    <div className="soc-section">
                        <h5>Индикаторы</h5>
                        <div className="soc-indicators">
                            {result.indicators.map((ind, i) => (
                                <span key={i} className="soc-indicator">{ind}</span>
                            ))}
                        </div>
                    </div>
                )}

                <div className="soc-section">
                    <h5>Рекомендация</h5>
                    <p className="soc-action-note">
                        Рекомендуемое действие (текст): <b>{result.recommended_action}</b>.
                        Ничего не выполняется автоматически: блокировку или прокси
                        включает только оператор вручную через UI агентов.
                    </p>
                </div>

                <p className="soc-disclaimer">
                    Вердикт SOC - информационный, генерируется локально задеплоенной
                    моделью и не является гарантией. Unknown означает недостаточно
                    данных, а не безопасность.
                </p>
            </div>
        </div>
    );
}
