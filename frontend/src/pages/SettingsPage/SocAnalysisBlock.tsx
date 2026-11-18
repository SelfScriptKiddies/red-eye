// SOC (LLM triage) settings card: independent from JEV, shows the
// combined effective behavior from the configuration matrix, all
// thresholds and compute limits, queue state, worker/MCP status and
// one-time credential generation. Rendered strings are React text.

import { useCallback, useEffect, useState } from 'react';
import {
    SOC_SETTING_ROWS,
    fetchSocStatus,
    generateSocCredential,
    isExternalLlmEndpoint,
    putSocSetting,
    type SocStatus,
} from '../../lib/socTriage';

function WorkerBadge({ status }: { status: SocStatus | null }) {
    if (!status) return null;
    const seen = status.worker_last_seen_unix
        ? new Date(status.worker_last_seen_unix * 1000).toLocaleTimeString()
        : 'никогда';
    const mcpSeen = status.mcp_last_seen_unix
        ? new Date(status.mcp_last_seen_unix * 1000).toLocaleTimeString()
        : 'никогда';
    return (
        <div className="favorite-usage">
            <div className="favorite-usage-row">
                <span>Очередь: {status.queue.depth} · lease: {status.queue.active_leases} · старейшая: {status.queue.oldest_wait_secs}с</span>
                <span>Задач сегодня: {status.queue.jobs_used_today} из {status.limits.daily_job_limit}</span>
            </div>
            <div className="favorite-usage-row">
                <span>Воркер: последний отклик {seen}</span>
                <span>MCP: {status.mcp_service_token_configured ? 'токен задан' : 'токена нет'} (отклик {mcpSeen})</span>
            </div>
            {(status.recent_errors ?? []).slice(0, 3).map(e => (
                <div key={`${e.job_id}-${e.at_unix}`} className="favorite-usage-row">
                    <span style={{ color: '#e0606a', fontSize: '11px' }}>
                        задача #{e.job_id}: {e.error}
                    </span>
                </div>
            ))}
        </div>
    );
}

export function SocAnalysisBlock() {
    const [status, setStatus] = useState<SocStatus | null>(null);
    const [values, setValues] = useState<Record<string, string>>({});
    const [apiKey, setApiKey] = useState('');
    const [acknowledged, setAcknowledged] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [notice, setNotice] = useState<string | null>(null);

    const reload = useCallback(async () => {
        try {
            const [st, settings] = await Promise.all([
                fetchSocStatus(),
                fetch('/api/settings').then(r => r.json()),
            ]);
            setStatus(st);
            setValues(Object.fromEntries(SOC_SETTING_ROWS.map(row => [row.key, String(settings[row.key] ?? '')])));
        } catch (e) {
            setError(`Не удалось загрузить состояние SOC: ${e}`);
        }
    }, []);

    useEffect(() => { reload(); }, [reload]);

    const save = async (key: string, value: string) => {
        setError(null);
        setNotice(null);
        try {
            await putSocSetting(key, value);
            setNotice(`${key} сохранено, применяется без перезапуска`);
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
            setError('Сначала включите LLM-SOC (soc_enabled)');
            return;
        }
        if (key === 'soc_enabled' && target) {
            const external = isExternalLlmEndpoint(values.soc_base_url ?? '');
            if (external && !acknowledged) {
                setError('Подтвердите предупреждение о приватности: endpoint вне локальной сети');
                return;
            }
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
            setError(`Не удалось сгенерировать токен: ${e}`);
        }
    };

    const matrix = status?.matrix;
    const external = isExternalLlmEndpoint(values.soc_base_url ?? '');

    return (
        <div>
            {matrix && (
                <div className="favorite-usage">
                    <div className="favorite-usage-row">
                        <span>Ручной триаж: {matrix.manual.available ? 'доступен' : 'недоступен'} - {matrix.manual.reason}</span>
                    </div>
                    <div className="favorite-usage-row">
                        <span>Авто-триаж: {matrix.auto.available ? 'активен' : 'выключен'} - {matrix.auto.reason}</span>
                    </div>
                    <div className="favorite-usage-row">
                        <span>JEV: {matrix.jev_state} · SOC: {matrix.soc_state} (строка {matrix.matrix_row})</span>
                    </div>
                    {status?.claims_paused_reason && (
                        <div className="favorite-usage-row">
                            <span style={{ color: '#e8cd7e', fontSize: '11px' }}>Claims на паузе: {status.claims_paused_reason}</span>
                        </div>
                    )}
                </div>
            )}
            <WorkerBadge status={status} />

            <div className="setting-row">
                <button
                    className="add-btn"
                    onClick={() => toggle('soc_enabled', !status?.enabled)}
                    style={{ fontSize: '12px', padding: '6px 14px' }}
                >
                    {status?.enabled ? 'Выключить LLM-SOC' : 'Включить LLM-SOC'}
                </button>
                <button
                    className={`add-btn ${status?.auto_enabled ? '' : 'disabled'}`}
                    onClick={() => toggle('soc_auto_enabled', !status?.auto_enabled)}
                    style={{ fontSize: '12px', padding: '6px 14px' }}
                    title={status?.auto_enabled
                        ? 'Выключить автоматическое занесение новых JEV-вердиктов в очередь'
                        : 'Включить автоматический триаж qualifying JEV-вердиктов'}
                >
                    {status?.auto_enabled ? 'Авто: вкл' : 'Авто: выкл'}
                </button>
            </div>

            {external && status !== null && (
                <div className="jev-warning" style={{
                    border: '1px solid #a05050', borderRadius: '8px', padding: '10px',
                    margin: '10px 0', background: 'rgba(160, 80, 80, 0.12)', fontSize: '12px',
                }}>
                    <b>Приватность.</b> SOC endpoint ({values.soc_base_url}) вне локальной сети:
                    содержимое трафика (расшифрованный TLS-MITM текст, учётные данные,
                    флаги, эксплойты) покинет игровую сеть. Отправку нельзя отозвать.
                    <label className="checkbox-label" style={{ display: 'block', marginTop: '8px' }}>
                        <input
                            type="checkbox"
                            checked={acknowledged}
                            onChange={(e) => setAcknowledged(e.target.checked)}
                        />
                        Я понимаю, что данные покидают сеть
                    </label>
                </div>
            )}
            {!external && (
                <p style={{ color: '#888', fontSize: '11px', margin: '8px 0' }}>
                    Endpoint в локальной сети (localhost/приватный диапазон/имя compose-сервиса):
                    отдельное подтверждение приватности не требуется.
                </p>
            )}

            <div className="setting-row" style={{ marginTop: '10px', flexDirection: 'column', alignItems: 'stretch' }}>
                <label style={{ fontSize: '12px', color: '#ccc' }}>API-ключ LLM (запись-только)</label>
                <div style={{ display: 'flex', gap: '6px', marginTop: '4px' }}>
                    <input
                        type="password"
                        autoComplete="off"
                        value={apiKey}
                        onChange={(e) => setApiKey(e.target.value)}
                        placeholder={status?.api_key_configured ? 'ключ скрыт; введите новый для замены' : 'sk-... (опционально для локального сервера)'}
                        style={{ flex: 1, padding: '8px 12px', background: '#1a1a1a', border: '1px solid transparent', borderRadius: '8px', color: 'white', fontSize: '13px' }}
                    />
                    <button className="add-btn" onClick={() => { setApiKey(''); save('soc_api_key', apiKey); }} style={{ fontSize: '12px', padding: '6px 14px' }}>Сохранить</button>
                    <button className="add-btn" onClick={() => { setApiKey(''); save('soc_api_key', ''); }} style={{ fontSize: '12px', padding: '6px 14px' }}>Удалить</button>
                </div>
                <p style={{ color: '#888', fontSize: '11px', margin: '4px 0 0' }}>
                    Ключ никогда не возвращается через API и не попадает в логи.
                </p>
            </div>

            <div className="setting-row" style={{ marginTop: '10px', flexDirection: 'column', alignItems: 'stretch' }}>
                <label style={{ fontSize: '12px', color: '#ccc' }}>Служебные токены (показываются один раз)</label>
                <div style={{ display: 'flex', gap: '6px', marginTop: '4px', flexWrap: 'wrap' }}>
                    <button className="add-btn" onClick={() => generate('worker-token', false)} style={{ fontSize: '11px', padding: '5px 10px' }}>
                        Токен воркера {status?.worker_token_configured ? '(задан; rotate=false отклонит)' : '(сгенерировать)'}
                    </button>
                    <button className="add-btn" onClick={() => generate('worker-token', true)} style={{ fontSize: '11px', padding: '5px 10px' }}>
                        Перевыпустить воркера
                    </button>
                    <button className="add-btn" onClick={() => generate('mcp-service-token', false)} style={{ fontSize: '11px', padding: '5px 10px' }}>
                        Токен MCP-сервиса {status?.mcp_service_token_configured ? '(задан)' : '(сгенерировать)'}
                    </button>
                    <button className="add-btn" onClick={() => generate('mcp-service-token', true)} style={{ fontSize: '11px', padding: '5px 10px' }}>
                        Перевыпустить MCP
                    </button>
                </div>
                <p style={{ color: '#888', fontSize: '11px', margin: '4px 0 0' }}>
                    Воркер: в .env как SOC_WORKER_TOKEN. MCP-сервис: в .env как MCP_SERVICE_TOKEN.
                    Хранятся только как SHA-256; старое значение действует до перевыпуска.
                </p>
            </div>

            {SOC_SETTING_ROWS.map(row => (
                <div key={row.key} className="setting-row" style={{ marginTop: '8px', flexDirection: 'column', alignItems: 'stretch' }}>
                    <label style={{ fontSize: '12px', color: '#ccc' }}>{row.label}</label>
                    <div style={{ display: 'flex', gap: '6px', marginTop: '4px' }}>
                        <input
                            type="text"
                            value={values[row.key] ?? ''}
                            onChange={(e) => setValues(v => ({ ...v, [row.key]: e.target.value }))}
                            style={{ flex: 1, padding: '8px 12px', background: '#1a1a1a', border: '1px solid transparent', borderRadius: '8px', color: 'white', fontSize: '13px' }}
                        />
                        <button className="add-btn" onClick={() => saveValidated(row)} style={{ fontSize: '12px', padding: '6px 14px' }}>Сохранить</button>
                    </div>
                    <p style={{ color: '#888', fontSize: '11px', margin: '4px 0 0' }}>{row.hint}</p>
                </div>
            ))}
            {error && <p style={{ color: '#e0606a', fontSize: '12px', margin: '8px 0 0' }}>{error}</p>}
            {notice && !error && <p style={{ color: '#87d37c', fontSize: '12px', margin: '8px 0 0', wordBreak: 'break-all' }}>{notice}</p>}
        </div>
    );
}
