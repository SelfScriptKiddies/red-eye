// SOC (LLM triage) block: terse effective availability, compact queue
// stats, primary settings in the open, compute knobs under Advanced.
// Model field is fed from GET /api/soc/models; a model with a declared
// context auto-fills soc_max_context_tokens (field value, not auto-save).

import { useCallback, useEffect, useState } from 'react';
import {
    SOC_SETTING_ROWS,
    fetchSocModels,
    fetchSocStatus,
    generateSocCredential,
    putSocSetting,
    type SocModelEntry,
    type SocStatus,
} from '../../lib/socTriage';

const seenTime = (unix?: number | null): string =>
    unix ? new Date(unix * 1000).toLocaleTimeString() : 'не подключался';

function SocStatusLine({ status }: { status: SocStatus | null }) {
    if (!status) return null;
    const { manual, auto } = status.matrix;
    return (
        <div className="favorite-usage">
            <div className="favorite-usage-row">
                <span title={manual.reason}>Ручной триаж: {manual.available ? 'доступен' : 'выкл'}</span>
                <span title={auto.reason}>Авто-триаж: {status.auto_enabled && auto.available ? 'вкл' : 'выкл'}</span>
            </div>
            <div className="favorite-usage-row">
                <span>
                    Очередь: {status.queue.depth} · Задач сегодня: {status.queue.jobs_used_today}/{status.limits.daily_job_limit} ·
                    Воркер: {seenTime(status.worker_last_seen_unix)}
                </span>
                <span title={seenTime(status.mcp_last_seen_unix)}>
                    MCP: {status.mcp_service_token_configured ? seenTime(status.mcp_last_seen_unix) : 'без токена'}
                </span>
            </div>
            {status.claims_paused_reason && (
                <div className="favorite-usage-row">
                    <span style={{ color: '#e8cd7e', fontSize: '11px' }} title={status.claims_paused_reason}>
                        Claims: пауза
                    </span>
                </div>
            )}
            {(status.recent_errors ?? []).slice(0, 2).map(e => (
                <div key={`${e.job_id}-${e.at_unix}`} className="favorite-usage-row">
                    <span style={{ color: '#e0606a', fontSize: '11px' }} title={e.error}>
                        Задача #{e.job_id}: {e.error.slice(0, 80)}
                    </span>
                </div>
            ))}
        </div>
    );
}

export function SocBlock() {
    const [status, setStatus] = useState<SocStatus | null>(null);
    const [values, setValues] = useState<Record<string, string>>({});
    const [apiKey, setApiKey] = useState('');
    const [error, setError] = useState<string | null>(null);
    const [notice, setNotice] = useState<string | null>(null);
    const [models, setModels] = useState<SocModelEntry[]>([]);

    const reload = useCallback(async () => {
        try {
            const [st, settings] = await Promise.all([
                fetchSocStatus(),
                fetch('/api/settings').then(r => r.json()),
            ]);
            setStatus(st);
            setValues(Object.fromEntries(SOC_SETTING_ROWS.map(row => [row.key, String(settings[row.key] ?? '')])));
        } catch (e) {
            setError(`SOC: загрузка не удалась (${e})`);
        }
    }, []);

    const reloadModels = useCallback(async () => {
        try {
            const resp = await fetchSocModels();
            setModels(resp.models ?? []);
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
            await putSocSetting(key, value);
            setNotice(`${key} сохранено`);
            if (key === 'soc_api_key' || key === 'soc_base_url') await reloadModels();
            await reload();
        } catch (e) {
            setError(`${key}: отклонено (${e})`);
        }
    };

    const saveValidated = (row: { key: string; type?: string }) => {
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
        } else if ((row.key === 'soc_model' || row.key === 'soc_base_url') && !raw) {
            setError(`${row.key} не может быть пустым`);
            return;
        }
        save(row.key, raw);
    };

    const toggle = (key: 'soc_enabled' | 'soc_auto_enabled', target: boolean) => {
        if (key === 'soc_auto_enabled' && target && !status?.enabled) {
            setError('Сначала включите LLM-SOC');
            return;
        }
        save(key, target ? 'true' : 'false');
    };

    const generate = async (kind: 'worker-token' | 'mcp-service-token', rotate: boolean) => {
        setError(null);
        setNotice(null);
        try {
            const token = await generateSocCredential(kind, rotate);
            setNotice(`Токен сгенерирован (показан один раз): ${token}`);
            await reload();
        } catch (e) {
            setError(`Токен: не сгенерирован (${e})`);
        }
    };

    // Model picked (typed or from the datalist): a declared context length
    // pre-fills the advanced context field; free text stays untouched.
    const onModelChange = (raw: string) => {
        setValues(v => ({ ...v, soc_model: raw }));
        const hit = models.find(m => m.id === raw.trim());
        if (hit?.context_length && Number.isFinite(hit.context_length)) {
            setValues(v => ({ ...v, soc_max_context_tokens: String(hit.context_length) }));
        }
    };

    const renderRow = (row: (typeof SOC_SETTING_ROWS)[number], isModel = false) => (
        <div key={row.key} className="setting-row" style={{ marginTop: '8px', flexDirection: 'column', alignItems: 'stretch' }}>
            <label style={{ fontSize: '12px', color: '#ccc' }}>{row.label}</label>
            <div style={{ display: 'flex', gap: '6px', marginTop: '4px' }}>
                <input
                    type="text"
                    value={values[row.key] ?? ''}
                    onChange={(e) => isModel ? onModelChange(e.target.value) : setValues(v => ({ ...v, [row.key]: e.target.value }))}
                    list={isModel ? 'soc-model-list' : undefined}
                    style={{ flex: 1, padding: '8px 12px', background: '#1a1a1a', border: '1px solid transparent', borderRadius: '8px', color: 'white', fontSize: '13px' }}
                />
                <button className="ai-btn" onClick={() => saveValidated(row)}>Сохранить</button>
            </div>
            <p style={{ color: '#888', fontSize: '11px', margin: '4px 0 0' }}>{row.hint}</p>
        </div>
    );

    const primary = SOC_SETTING_ROWS.filter(row => !row.advanced);
    const advanced = SOC_SETTING_ROWS.filter(row => row.advanced);

    return (
        <div>
            <SocStatusLine status={status} />

            <div className="setting-row">
                <button
                    className="ai-btn"
                    onClick={() => toggle('soc_enabled', !status?.enabled)}
                   
                >
                    {status?.enabled ? 'Выключить LLM-SOC' : 'Включить LLM-SOC'}
                </button>
                <button
                    className="ai-btn"
                    onClick={() => toggle('soc_auto_enabled', !status?.auto_enabled)}
                   
                    title="Автоматически ставить новые qualifying JEV-вердикты в очередь"
                >
                    {status?.auto_enabled ? 'Авто: вкл' : 'Авто: выкл'}
                </button>
            </div>

            <div className="setting-row" style={{ marginTop: '10px', flexDirection: 'column', alignItems: 'stretch' }}>
                <label style={{ fontSize: '12px', color: '#ccc' }}>API-ключ</label>
                <div style={{ display: 'flex', gap: '6px', marginTop: '4px' }}>
                    <input
                        type="password"
                        autoComplete="off"
                        value={apiKey}
                        onChange={(e) => setApiKey(e.target.value)}
                        placeholder={status?.api_key_configured ? 'ключ скрыт; введите новый для замены' : 'sk-... (опционально для локального сервера)'}
                        style={{ flex: 1, padding: '8px 12px', background: '#1a1a1a', border: '1px solid transparent', borderRadius: '8px', color: 'white', fontSize: '13px' }}
                    />
                    <button className="ai-btn" onClick={() => { setApiKey(''); save('soc_api_key', apiKey); }}>Сохранить</button>
                    <button className="ai-btn" onClick={() => { setApiKey(''); save('soc_api_key', ''); }}>Удалить</button>
                </div>
            </div>

            <div className="setting-row" style={{ marginTop: '10px', flexDirection: 'column', alignItems: 'stretch' }}>
                <label style={{ fontSize: '12px', color: '#ccc' }}>Служебные токены</label>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '6px', marginTop: '4px' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span style={{ fontSize: '12px', width: '110px' }}>Воркер</span>
                        <span className="info-badge">{status?.worker_token_configured ? 'задан' : 'нет'}</span>
                        <button className="ai-btn" onClick={() => generate('worker-token', true)}>Перевыпустить</button>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span style={{ fontSize: '12px', width: '110px' }}>MCP-сервис</span>
                        <span className="info-badge">{status?.mcp_service_token_configured ? 'задан' : 'нет'}</span>
                        <button className="ai-btn" onClick={() => generate('mcp-service-token', true)}>Перевыпустить</button>
                    </div>
                </div>
            </div>

            {primary.map(row => renderRow(row, row.key === 'soc_model'))}
            <datalist id="soc-model-list">
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
            {notice && !error && <p style={{ color: '#87d37c', fontSize: '12px', margin: '8px 0 0', wordBreak: 'break-all' }}>{notice}</p>}
        </div>
    );
}
