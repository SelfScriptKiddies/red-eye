import { useCallback, useEffect, useId, useRef, useState, type ReactNode } from 'react';
import { apiFetch, errorMessage } from '../../lib/api';
import { formatSeen, socState } from '../../lib/aiStatus';
import {
    SOC_SETTING_ROWS,
    fetchSocModels,
    fetchSocStatus,
    generateSocCredential,
    putSocSetting,
    type SocModelEntry,
    type SocStatus,
} from '../../lib/socTriage';
import { AiColumn } from './AiColumn';
import { Disclosure, Facts, FeedbackLine, SecretField, SecretReveal, SettingRows, StateLine, type Fact } from './parts';
import { useSettingForm } from './useSettingForm';

const POLL_MS = 15_000;

const CONNECTION_ROWS = SOC_SETTING_ROWS.filter(row => !row.advanced && (row.key === 'soc_base_url' || row.key === 'soc_model'));
const BEHAVIOUR_ROWS = SOC_SETTING_ROWS.filter(row => !row.advanced && row.key !== 'soc_base_url' && row.key !== 'soc_model');
const ADVANCED_ROWS = SOC_SETTING_ROWS.filter(row => row.advanced);
const CONTEXT_ROW = SOC_SETTING_ROWS.find(row => row.key === 'soc_max_context_tokens');

function CredentialRow({ label, configured, busy, onGenerate }: {
    label: string;
    configured: boolean;
    busy: boolean;
    onGenerate: () => void;
}) {
    return (
        <div className="ai-field">
            <span className="ai-field-label">{label}</span>
            <div className="ai-field-control ai-field-control--row">
                <span className={configured ? 'ai-value' : 'ai-value ai-value--empty'}>{configured ? 'задан' : 'нет'}</span>
                <button type="button" className="ai-btn" onClick={onGenerate} disabled={busy}>
                    {configured ? 'Сменить' : 'Создать'}
                </button>
            </div>
        </div>
    );
}

export function SocColumn() {
    const [status, setStatus] = useState<SocStatus | null>(null);
    const [unreachable, setUnreachable] = useState(false);
    const [models, setModels] = useState<SocModelEntry[]>([]);
    const [apiKey, setApiKey] = useState('');
    const [settingsOpen, setSettingsOpen] = useState(false);
    const [generated, setGenerated] = useState<{ title: string; token: string } | null>(null);
    const [busy, setBusy] = useState(false);
    const [contextFrom, setContextFrom] = useState('');
    const panelId = useId();
    const modelListId = useId();
    const modelsRequest = useRef(0);
    const form = useSettingForm(SOC_SETTING_ROWS, putSocSetting);
    const { load, fail } = form;

    const refreshStatus = useCallback(async () => {
        try {
            setStatus(await fetchSocStatus());
            setUnreachable(false);
        } catch {
            setUnreachable(true);
        }
    }, []);

    // A slow failed catalog fetch must not overwrite a newer successful one.
    const refreshModels = useCallback(async () => {
        const request = ++modelsRequest.current;
        try {
            const list = (await fetchSocModels()).models ?? [];
            if (request === modelsRequest.current) setModels(list);
        } catch {
            if (request === modelsRequest.current) setModels([]);
        }
    }, []);

    const loadSettings = useCallback(async () => {
        try {
            load(await apiFetch<Record<string, unknown>>('/api/settings'));
        } catch (e) {
            fail(`Настройки не загрузились (${errorMessage(e)})`);
        }
    }, [load, fail]);

    useEffect(() => {
        refreshStatus();
        refreshModels();
        loadSettings();
        const id = window.setInterval(refreshStatus, POLL_MS);
        return () => window.clearInterval(id);
    }, [refreshStatus, refreshModels, loadSettings]);

    const setEnabled = async (enabled: boolean) => {
        if (await form.saveRaw('soc_enabled', String(enabled))) await refreshStatus();
    };

    const setAuto = async (enabled: boolean) => {
        if (await form.saveRaw('soc_auto_enabled', String(enabled))) await refreshStatus();
    };

    const saveKey = async () => {
        const value = apiKey.trim();
        const notice = value ? 'Ключ сохранён' : 'Ключ удалён';
        if (await form.saveRaw('soc_api_key', value, notice)) {
            setApiKey('');
            refreshModels();
            refreshStatus();
        }
    };

    // A catalog model's declared context rides along with the model save.
    const afterSave = async (key: string) => {
        const model = (form.values.soc_model ?? '').trim();
        if (key === 'soc_model' && CONTEXT_ROW && contextFrom === model && form.isDirty(CONTEXT_ROW)) {
            await form.save(CONTEXT_ROW);
        }
        if (key === 'soc_base_url') refreshModels();
        refreshStatus();
    };

    const onModelChange = (model: string) => {
        form.edit('soc_model', model);
        const hit = models.find(m => m.id === model.trim());
        if (hit?.context_length && Number.isFinite(hit.context_length)) {
            form.edit('soc_max_context_tokens', String(hit.context_length));
            setContextFrom(hit.id);
        }
    };

    const generate = async (kind: 'worker-token' | 'mcp-service-token', title: string) => {
        form.clearFeedback();
        setBusy(true);
        try {
            const token = await generateSocCredential(kind, true);
            setGenerated({ title, token });
            await refreshStatus();
        } catch (e) {
            form.fail(`Токен не создан (${errorMessage(e)})`);
        } finally {
            setBusy(false);
        }
    };

    const facts: Fact[] = [];
    if (status?.enabled) {
        const queue = status.queue;
        facts.push(
            {
                label: 'Очередь',
                value: String(queue.depth),
                title: `В работе ${queue.active_leases}, старейшая ждёт ${queue.oldest_wait_secs} с`,
            },
            { label: 'Задач сегодня', value: `${queue.jobs_used_today} из ${status.limits.daily_job_limit ?? ''}` },
            { label: 'Воркер', value: formatSeen(status.worker_last_seen_unix) },
            {
                label: 'MCP-сервис',
                value: status.mcp_service_token_configured ? formatSeen(status.mcp_last_seen_unix) : 'нет токена',
            },
        );
    }

    let action: ReactNode = null;
    if (status) {
        action = (
            <button
                type="button"
                className="ai-btn ai-btn--pill"
                onClick={() => setEnabled(!status.enabled)}
                disabled={form.busy}
            >
                {status.enabled ? 'Выключить' : 'Включить'}
            </button>
        );
    }

    const socOn = !!status?.enabled;

    return (
        <AiColumn title="Авто-триаж" action={action}>
            <StateLine view={status ? socState(status) : null} unreachable={unreachable} />

            <label className={socOn ? 'ai-check' : 'ai-check ai-check--locked'} title={socOn ? undefined : 'Сначала включите триаж'}>
                <input
                    type="checkbox"
                    checked={!!status?.auto_enabled}
                    disabled={!socOn || form.busy}
                    onChange={e => setAuto(e.target.checked)}
                />
                Запускать триаж по новым вердиктам JEV
            </label>

            {facts.length > 0 && <Facts items={facts} />}

            {status && status.recent_errors.length > 0 && (
                <ul className="ai-alerts">
                    {status.recent_errors.slice(0, 2).map(e => (
                        <li key={`${e.job_id}-${e.at_unix}`} title={e.error}>
                            Задача {e.job_id}: {e.error.slice(0, 80)}
                        </li>
                    ))}
                </ul>
            )}

            <FeedbackLine feedback={form.feedback} />

            <Disclosure id={panelId} label="Настройки" open={settingsOpen} onToggle={() => setSettingsOpen(open => !open)}>
                <SettingRows
                    rows={CONNECTION_ROWS}
                    form={form}
                    onSaved={afterSave}
                    modelPicker={{ rowKey: 'soc_model', listId: modelListId, models, onChange: onModelChange }}
                />
                <SecretField
                    label="API-ключ"
                    value={apiKey}
                    onChange={setApiKey}
                    onSave={saveKey}
                    configured={!!status?.api_key_configured}
                    saving={form.busy}
                    placeholder={status?.api_key_configured ? 'ключ сохранён' : 'не обязателен'}
                />
                <SettingRows rows={BEHAVIOUR_ROWS} form={form} onSaved={afterSave} />

                <h3 className="ai-subhead">Служебные токены</h3>
                <CredentialRow
                    label="Воркер"
                    configured={!!status?.worker_token_configured}
                    busy={busy}
                    onGenerate={() => generate('worker-token', 'Токен воркера показан один раз')}
                />
                <CredentialRow
                    label="MCP-сервис"
                    configured={!!status?.mcp_service_token_configured}
                    busy={busy}
                    onGenerate={() => generate('mcp-service-token', 'Токен MCP-сервиса показан один раз')}
                />
                {generated && (
                    <SecretReveal title={generated.title} secret={generated.token} onHide={() => setGenerated(null)} />
                )}

                <details className="ai-more">
                    <summary className="ai-toggle">Дополнительно</summary>
                    <div className="ai-panel">
                        <SettingRows rows={ADVANCED_ROWS} form={form} onSaved={afterSave} />
                    </div>
                </details>
            </Disclosure>
        </AiColumn>
    );
}
