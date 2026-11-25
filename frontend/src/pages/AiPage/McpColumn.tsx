import { useCallback, useEffect, useState, type FormEvent } from 'react';
import { errorMessage } from '../../lib/api';
import { createMcpToken, fetchMcpTokens, fetchSocStatus, revokeMcpToken, type McpToken } from '../../lib/socTriage';
import { AiColumn } from './AiColumn';
import { FeedbackLine, SecretReveal } from './parts';
import type { Feedback } from './useSettingForm';

const POLL_MS = 15_000;

function tokenExpiry(token: McpToken): string {
    return token.expires_at_unix ? `до ${new Date(token.expires_at_unix * 1000).toLocaleDateString('ru-RU')}` : 'бессрочно';
}

export function McpColumn() {
    const [tokens, setTokens] = useState<McpToken[]>([]);
    const [fullControlAllowed, setFullControlAllowed] = useState(false);
    const [loadError, setLoadError] = useState<string | null>(null);
    const [formOpen, setFormOpen] = useState(false);
    const [name, setName] = useState('');
    const [capability, setCapability] = useState('read_only');
    const [expiryDays, setExpiryDays] = useState('');
    const [created, setCreated] = useState<{ name: string; token: string } | null>(null);
    const [feedback, setFeedback] = useState<Feedback | null>(null);
    const [busy, setBusy] = useState(false);

    const reload = useCallback(async () => {
        try {
            const [list, status] = await Promise.all([fetchMcpTokens(), fetchSocStatus().catch(() => null)]);
            setTokens(list);
            setFullControlAllowed(status?.mcp_service_capability === 'full_control');
            setLoadError(null);
        } catch (e) {
            setLoadError(`Токены не загрузились (${errorMessage(e)})`);
        }
    }, []);

    useEffect(() => {
        reload();
        const id = window.setInterval(reload, POLL_MS);
        return () => window.clearInterval(id);
    }, [reload]);

    const closeForm = () => {
        setFormOpen(false);
        setFeedback(null);
    };

    const openForm = () => {
        setFeedback(null);
        setName('');
        setCapability('read_only');
        setExpiryDays('');
        setFormOpen(true);
    };

    const submit = async (e: FormEvent) => {
        e.preventDefault();
        const label = name.trim();
        if (!label) {
            setFeedback({ kind: 'error', text: 'Укажите название' });
            return;
        }
        const days = Number(expiryDays);
        const limited = expiryDays.trim() !== '';
        if (limited && (!Number.isInteger(days) || days <= 0)) {
            setFeedback({ kind: 'error', text: 'Срок: целое число дней' });
            return;
        }
        setFeedback(null);
        setBusy(true);
        try {
            const res = await createMcpToken(label, capability, limited ? days * 86400 : undefined);
            setCreated({ name: label, token: res.token });
            setFormOpen(false);
            await reload();
        } catch (err) {
            setFeedback({ kind: 'error', text: `Токен не создан (${errorMessage(err)})` });
        } finally {
            setBusy(false);
        }
    };

    const revoke = async (id: number) => {
        setFeedback(null);
        setBusy(true);
        try {
            await revokeMcpToken(id);
            await reload();
        } catch (err) {
            setFeedback({ kind: 'error', text: `Токен не отозван (${errorMessage(err)})` });
        } finally {
            setBusy(false);
        }
    };

    return (
        <AiColumn
            title="MCP"
            action={!formOpen && <button type="button" className="ai-btn ai-btn--pill" onClick={openForm}>+ Токен</button>}
        >
            {formOpen && (
                <form className="ai-panel" onSubmit={submit}>
                    <div className="ai-field">
                        <label className="ai-field-label" htmlFor="mcp-token-name">Название</label>
                        <div className="ai-field-control">
                            <input
                                id="mcp-token-name"
                                className="ai-input"
                                value={name}
                                onChange={e => setName(e.target.value)}
                                autoFocus
                            />
                        </div>
                    </div>
                    <div className="ai-field">
                        <label className="ai-field-label" htmlFor="mcp-token-capability">Права</label>
                        <div className="ai-field-control">
                            <select
                                id="mcp-token-capability"
                                className="ai-select"
                                value={capability}
                                onChange={e => setCapability(e.target.value)}
                            >
                                <option value="read_only">read_only</option>
                                <option value="full_control" disabled={!fullControlAllowed}>full_control</option>
                            </select>
                        </div>
                    </div>
                    <div className="ai-field">
                        <label className="ai-field-label" htmlFor="mcp-token-expiry">Срок, дней</label>
                        <div className="ai-field-control">
                            <input
                                id="mcp-token-expiry"
                                className="ai-input"
                                inputMode="numeric"
                                placeholder="бессрочно"
                                value={expiryDays}
                                onChange={e => setExpiryDays(e.target.value)}
                            />
                        </div>
                    </div>
                    <div className="ai-form-actions">
                        <button type="submit" className="ai-btn ai-btn--primary" disabled={busy}>Создать</button>
                        <button type="button" className="ai-btn" onClick={closeForm}>Отмена</button>
                    </div>
                    {!fullControlAllowed && <p className="ai-note">full_control выключен в деплое</p>}
                </form>
            )}

            <FeedbackLine feedback={feedback} />

            {created && (
                <SecretReveal
                    title={`Токен ${created.name} показан один раз`}
                    secret={created.token}
                    onHide={() => setCreated(null)}
                />
            )}

            {tokens.length > 0 ? (
                <ul className="ai-tokens">
                    {tokens.map(t => (
                        <li key={t.id} className={t.revoked ? 'ai-token ai-token--revoked' : 'ai-token'}>
                            <span className="ai-token-name">{t.name}</span>
                            <span className="ai-badge">{t.capability}</span>
                            {t.revoked ? (
                                <span className="ai-badge ai-badge--danger">отозван</span>
                            ) : (
                                <>
                                    <span className="ai-token-meta">{tokenExpiry(t)}</span>
                                    <button type="button" className="ai-btn ai-btn--ghost" onClick={() => revoke(t.id)} disabled={busy}>
                                        Отозвать
                                    </button>
                                </>
                            )}
                        </li>
                    ))}
                </ul>
            ) : !formOpen && <p className="ai-empty">Токенов нет</p>}

            {loadError && <FeedbackLine feedback={{ kind: 'error', text: loadError }} />}
        </AiColumn>
    );
}
