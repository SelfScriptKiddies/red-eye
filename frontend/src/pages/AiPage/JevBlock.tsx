// JEV (AI stream analysis) settings block, moved here from SettingsPage:
// one status line, privacy gate, primary fields only, the rest under the
// Advanced collapsible.

import { useCallback, useEffect, useState } from 'react';
import { apiFetch } from '../../lib/api';
import {
    AI_SETTING_ROWS,
    fetchAiModels,
    fetchAiStatus,
    filterAiModels,
    putAiSetting,
    type AiModelEntry,
    type AiStatus,
} from '../../lib/aiVerdict';

function JevStateLine({ status }: { status: AiStatus | null }) {
    if (!status) return null;
    const state = status.enabled
        ? status.run_state === 'active' ? 'активен' : status.run_state === 'backoff'
            ? 'пауза'
            : status.run_state === 'cost_limited' ? 'лимит' : 'ждёт'
        : 'выкл';
    return (
        <div className="favorite-usage">
            <div className="favorite-usage-row">
                <span
                    title={
                        status.enabled && status.run_state !== 'active'
                            ? `run_state: ${status.run_state}`
                            : undefined
                    }
                >
                    JEV: {state}
                </span>
                <span>Модель: {status.model}</span>
            </div>
            <div className="favorite-usage-row">
                <span>
                    Очередь: {status.backlog} · В полёте: {status.in_flight} · Расход:
                    ${status.cost_spent_today.toFixed(2)}/${status.cost_limit}
                </span>
                <span>Вердикты: {status.verdicts_total} (атак: {status.attack_verdicts_total})</span>
            </div>
        </div>
    );
}

export function JevBlock() {
    const [status, setStatus] = useState<AiStatus | null>(null);
    const [values, setValues] = useState<Record<string, string>>({});
    const [apiKey, setApiKey] = useState('');
    const [error, setError] = useState<string | null>(null);
    const [notice, setNotice] = useState<string | null>(null);
    const [models, setModels] = useState<AiModelEntry[]>([]);

    const reload = useCallback(async () => {
        try {
            const [st, settings] = await Promise.all([
                fetchAiStatus(),
                apiFetch<Record<string, string>>('/api/settings'),
            ]);
            setStatus(st);
            setValues({
                ai_model: String(settings.ai_model ?? ''),
                ai_threshold: String(settings.ai_threshold ?? ''),
                ai_max_per_tick: String(settings.ai_max_per_tick ?? ''),
                ai_stream_debounce_ms: String(settings.ai_stream_debounce_ms ?? ''),
                ai_stream_min_interval_secs: String(settings.ai_stream_min_interval_secs ?? ''),
                ai_stream_max_bytes: String(settings.ai_stream_max_bytes ?? ''),
                ai_stream_max_entries: String(settings.ai_stream_max_entries ?? ''),
                ai_entry_max_bytes: String(settings.ai_entry_max_bytes ?? ''),
                ai_daily_cost_limit: String(settings.ai_daily_cost_limit ?? ''),
                ai_verdict_retention_days: String(settings.ai_verdict_retention_days ?? ''),
            });
        } catch (e) {
            setError(`JEV: загрузка не удалась (${e})`);
        }
    }, []);

    const reloadModels = useCallback(async () => {
        try {
            const resp = await fetchAiModels();
            setModels(filterAiModels(resp.models ?? []));
        } catch {
            setModels([]);
        }
    }, []);

    useEffect(() => {
        reload();
        reloadModels();
    }, [reload, reloadModels]);

    const save = async (key: string, value: string) => {
        setError(null);
        setNotice(null);
        try {
            await putAiSetting(key, value);
            setNotice(`${key} сохранено`);
            if (key === 'ai_api_key') await reloadModels();
            await reload();
        } catch (e) {
            setError(`${key}: отклонено (${e})`);
        }
    };

    const validateAndSave = (row: { key: string; type?: string }) => {
        const raw = (values[row.key] ?? '').trim();
        if (row.type === 'float') {
            const n = Number(raw);
            if (!raw || !Number.isFinite(n)) {
                setError(`${row.key}: нужно число`);
                return;
            }
        } else if (row.type === 'int') {
            if (!raw || !Number.isInteger(Number(raw))) {
                setError(`${row.key}: нужно целое число`);
                return;
            }
        } else if (row.key === 'ai_model' && !raw) {
            setError('ai_model не может быть пустым');
            return;
        }
        save(row.key, raw);
    };

    const toggleEnabled = () => {
        if (!status) return;
        if (status.enabled) {
            save('ai_enabled', 'false');
            return;
        }
        if (!status.configured) {
            setError('Сначала сохраните API-ключ OpenRouter');
            return;
        }
        save('ai_enabled', 'true');
    };

    const renderRow = (row: (typeof AI_SETTING_ROWS)[number], isModel = false) => (
        <div key={row.key} className="setting-row" style={{ marginTop: '8px', flexDirection: 'column', alignItems: 'stretch' }}>
            <label style={{ fontSize: '12px', color: '#ccc' }}>{row.label}</label>
            <div style={{ display: 'flex', gap: '6px', marginTop: '4px' }}>
                <input
                    type="text"
                    value={values[row.key] ?? ''}
                    onChange={(e) => setValues(v => ({ ...v, [row.key]: e.target.value }))}
                    list={isModel ? 'jev-model-list' : undefined}
                    style={{ flex: 1, padding: '8px 12px', background: '#1a1a1a', border: '1px solid transparent', borderRadius: '8px', color: 'white', fontSize: '13px' }}
                />
                <button className="ai-btn" onClick={() => validateAndSave(row)}>
                    Сохранить
                </button>
            </div>
            <p style={{ color: '#888', fontSize: '11px', margin: '4px 0 0' }}>{row.hint}</p>
        </div>
    );

    const primary = AI_SETTING_ROWS.filter(row => !row.advanced);
    const advanced = AI_SETTING_ROWS.filter(row => row.advanced);

    return (
        <div>
            <JevStateLine status={status} />

            <div className="setting-row">
                <button
                    className="ai-btn"
                    onClick={toggleEnabled}
                   
                >
                    {status?.enabled ? 'Выключить AI-анализ' : 'Включить AI-анализ'}
                </button>
                <span style={{ fontSize: '11px', color: '#888' }}>
                    {status?.configured ? 'ключ сохранён' : 'ключ не задан'}
                </span>
            </div>


            <div className="setting-row" style={{ marginTop: '10px', flexDirection: 'column', alignItems: 'stretch' }}>
                <label style={{ fontSize: '12px', color: '#ccc' }}>API-ключ OpenRouter</label>
                <div style={{ display: 'flex', gap: '6px', marginTop: '4px' }}>
                    <input
                        type="password"
                        autoComplete="off"
                        value={apiKey}
                        onChange={(e) => setApiKey(e.target.value)}
                        placeholder={status?.configured ? 'ключ скрыт; введите новый для замены' : 'sk-or-...'}
                        style={{ flex: 1, padding: '8px 12px', background: '#1a1a1a', border: '1px solid transparent', borderRadius: '8px', color: 'white', fontSize: '13px' }}
                    />
                    <button className="ai-btn" onClick={() => { setApiKey(''); save('ai_api_key', apiKey); }}>
                        Сохранить
                    </button>
                    <button className="ai-btn" onClick={() => { setApiKey(''); save('ai_api_key', ''); }}>
                        Удалить
                    </button>
                </div>
            </div>

            {primary.map(row => renderRow(row, row.key === 'ai_model'))}
            <datalist id="jev-model-list">
                {models.map(m => (
                    <option key={m.id} value={m.id}>
                        {m.context_length ? `ctx ${m.context_length}` : ''}
                    </option>
                ))}
            </datalist>

            <details style={{ margin: '10px 0' }}>
                <summary style={{ cursor: 'pointer', fontSize: '12px', color: '#888' }}>Advanced</summary>
                {advanced.map(row => renderRow(row))}
            </details>

            {error && <p style={{ color: '#e0606a', fontSize: '12px', margin: '8px 0 0' }}>{error}</p>}
            {notice && !error && <p style={{ color: '#87d37c', fontSize: '12px', margin: '8px 0 0' }}>{notice}</p>}
        </div>
    );
}
