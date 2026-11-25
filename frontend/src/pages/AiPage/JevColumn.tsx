import { useCallback, useEffect, useId, useRef, useState, type ReactNode } from 'react';
import { apiFetch, errorMessage } from '../../lib/api';
import { formatMoney, jevState } from '../../lib/aiStatus';
import {
    AI_SETTING_ROWS,
    fetchAiModels,
    fetchAiStatus,
    filterAiModels,
    putAiSetting,
    type AiStatus,
} from '../../lib/aiVerdict';
import { AiColumn } from './AiColumn';
import { Disclosure, Facts, FeedbackLine, SecretField, SettingRows, StateLine, type Fact } from './parts';
import { useSettingForm } from './useSettingForm';

const POLL_MS = 15_000;

const PRIMARY_ROWS = AI_SETTING_ROWS.filter(row => !row.advanced);
const ADVANCED_ROWS = AI_SETTING_ROWS.filter(row => row.advanced);

export function JevColumn() {
    const [status, setStatus] = useState<AiStatus | null>(null);
    const [unreachable, setUnreachable] = useState(false);
    const [models, setModels] = useState<{ id: string }[]>([]);
    const [apiKey, setApiKey] = useState('');
    const [settingsOpen, setSettingsOpen] = useState(false);
    const [focusKey, setFocusKey] = useState(false);
    const keyInput = useRef<HTMLInputElement>(null);
    const panelId = useId();
    const modelListId = useId();
    const modelsRequest = useRef(0);
    const form = useSettingForm(AI_SETTING_ROWS, putAiSetting);
    const { load, fail } = form;

    const refreshStatus = useCallback(async () => {
        try {
            setStatus(await fetchAiStatus());
            setUnreachable(false);
        } catch {
            setUnreachable(true);
        }
    }, []);

    // A slow failed catalog fetch must not overwrite a newer successful one.
    const refreshModels = useCallback(async () => {
        const request = ++modelsRequest.current;
        try {
            const list = filterAiModels((await fetchAiModels()).models ?? []);
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

    useEffect(() => {
        if (settingsOpen && focusKey) {
            keyInput.current?.focus();
            setFocusKey(false);
        }
    }, [settingsOpen, focusKey]);

    const setEnabled = async (enabled: boolean) => {
        if (await form.saveRaw('ai_enabled', String(enabled))) await refreshStatus();
    };

    const saveKey = async () => {
        const value = apiKey.trim();
        const notice = value ? 'Ключ сохранён' : 'Ключ удалён';
        if (await form.saveRaw('ai_api_key', value, notice)) {
            setApiKey('');
            refreshModels();
            refreshStatus();
        }
    };

    const afterSave = () => {
        refreshStatus();
    };

    const facts: Fact[] = [];
    if (status?.enabled) {
        facts.push(
            { label: 'Модель', value: status.model },
            { label: 'Очередь', value: String(status.backlog), title: `В работе ${status.in_flight}` },
            { label: 'Расход сегодня', value: `${formatMoney(status.cost_spent_today)} из ${formatMoney(status.cost_limit)}` },
        );
    }
    if (status && status.verdicts_total > 0) {
        facts.push({ label: 'Вердикты', value: `${status.verdicts_total}, атак ${status.attack_verdicts_total}` });
    }

    let action: ReactNode = null;
    if (status?.enabled) {
        action = (
            <button type="button" className="ai-btn ai-btn--pill" onClick={() => setEnabled(false)} disabled={form.busy}>
                Выключить
            </button>
        );
    } else if (status) {
        action = status.configured ? (
            <button type="button" className="ai-btn ai-btn--pill" onClick={() => setEnabled(true)} disabled={form.busy}>
                Включить
            </button>
        ) : (
            <button
                type="button"
                className="ai-btn ai-btn--pill"
                onClick={() => { setSettingsOpen(true); setFocusKey(true); }}
            >
                Добавить ключ
            </button>
        );
    }

    return (
        <AiColumn title="JEV" action={action}>
            <StateLine view={status ? jevState(status) : null} unreachable={unreachable} />

            {facts.length > 0 && <Facts items={facts} />}

            <FeedbackLine feedback={form.feedback} />

            <Disclosure id={panelId} label="Настройки" open={settingsOpen} onToggle={() => setSettingsOpen(open => !open)}>
                <SecretField
                    label="Ключ OpenRouter"
                    value={apiKey}
                    onChange={setApiKey}
                    onSave={saveKey}
                    configured={!!status?.configured}
                    saving={form.busy}
                    placeholder={status?.configured ? 'ключ сохранён' : 'sk-or-...'}
                    inputRef={keyInput}
                />
                <SettingRows
                    rows={PRIMARY_ROWS}
                    form={form}
                    onSaved={afterSave}
                    modelPicker={{ rowKey: 'ai_model', listId: modelListId, models, onChange: value => form.edit('ai_model', value) }}
                />
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
