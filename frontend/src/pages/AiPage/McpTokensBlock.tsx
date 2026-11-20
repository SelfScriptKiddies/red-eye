// MCP token management: create (one-time plaintext display), revoke,
// capability change. Tokens are NEVER editable through MCP itself; this
// block is the only management surface.

import { useCallback, useEffect, useState } from 'react';
import { copyText } from '../../lib/api';
import {
    createMcpToken,
    fetchMcpTokens,
    revokeMcpToken,
    type McpToken,
} from '../../lib/socTriage';

const MCP_MODE_TOOLTIP =
    'По умолчанию read_only: мутаций нет. full_control требует перевёртки '
    + 'процесса (MCP_MODE + MCP_ALLOW_FULL_CONTROL), токен full_control и '
    + 'service-credential той же scope. Режим меняется деплоем, не через MCP.';

const CAPABILITY_LABELS: Record<string, string> = {
    read_only: 'read_only',
    full_control: 'full_control',
};

export function McpTokensBlock() {
    const [tokens, setTokens] = useState<McpToken[]>([]);
    const [name, setName] = useState('');
    const [capability, setCapability] = useState('read_only');
    const [expiryDays, setExpiryDays] = useState('');
    const [error, setError] = useState<string | null>(null);
    const [created, setCreated] = useState<{ name: string; token: string } | null>(null);

    const reload = useCallback(async () => {
        try {
            setTokens(await fetchMcpTokens());
        } catch (e) {
            setError(`Не удалось загрузить токены: ${e}`);
        }
    }, []);

    useEffect(() => { reload(); }, [reload]);

    const create = async () => {
        setError(null);
        if (!name.trim()) {
            setError('Укажите имя токена');
            return;
        }
        try {
            const days = Number(expiryDays);
            const res = await createMcpToken(name.trim(), capability, Number.isFinite(days) && days > 0 ? days * 86400 : undefined);
            setCreated({ name: name.trim(), token: res.token });
            setName('');
            await reload();
        } catch (e) {
            setError(`Не удалось создать токен: ${e}`);
        }
    };

    const revoke = async (id: number) => {
        setError(null);
        try {
            await revokeMcpToken(id);
            await reload();
        } catch (e) {
            setError(`Не удалось отозвать токен: ${e}`);
        }
    };

    return (
        <div>
            <div className="jev-warning" style={{
                border: '1px solid #a05050', borderRadius: '8px', padding: '10px',
                margin: '10px 0', background: 'rgba(160, 80, 80, 0.12)', fontSize: '12px',
            }} title={MCP_MODE_TOOLTIP}>
                <b>Режим MCP.</b> По умолчанию read_only, мутаций нет; full_control требует
                перевёртки процесса и отдельного токена. Токены нельзя менять через сам MCP.
            </div>

            <div className="setting-row" style={{ flexDirection: 'column', alignItems: 'stretch' }}>
                <div style={{ display: 'flex', gap: '6px', marginTop: '4px', flexWrap: 'wrap' }}>
                    <input
                        type="text"
                        value={name}
                        onChange={(e) => setName(e.target.value)}
                        placeholder="имя токена (кто им пользуется)"
                        style={{ flex: 1, minWidth: '160px', padding: '8px 12px', background: '#1a1a1a', border: '1px solid transparent', borderRadius: '8px', color: 'white', fontSize: '13px' }}
                    />
                    <select
                        value={capability}
                        onChange={(e) => setCapability(e.target.value)}
                        style={{ padding: '8px 12px', background: '#1a1a1a', border: '1px solid #333', borderRadius: '8px', color: 'white', fontSize: '13px' }}
                    >
                        <option value="read_only">read_only</option>
                        <option value="full_control">full_control</option>
                    </select>
                    <input
                        type="text"
                        inputMode="numeric"
                        value={expiryDays}
                        onChange={(e) => setExpiryDays(e.target.value)}
                        placeholder="срок, дней (пусто = бессрочно)"
                        style={{ width: '170px', padding: '8px 12px', background: '#1a1a1a', border: '1px solid transparent', borderRadius: '8px', color: 'white', fontSize: '13px' }}
                    />
                    <button className="add-btn" onClick={create} style={{ fontSize: '12px', padding: '6px 14px' }}>Создать</button>
                </div>
            </div>

            {created && (
                <div className="jev-warning" style={{
                    border: '1px solid #87d37c', borderRadius: '8px', padding: '10px',
                    margin: '10px 0', background: 'rgba(135, 211, 124, 0.08)', fontSize: '12px',
                }}>
                    Токен <b>{created.name}</b> создан, plaintext показан один раз:
                    <div style={{ display: 'flex', gap: '6px', marginTop: '6px', alignItems: 'center' }}>
                        <code style={{ wordBreak: 'break-all', flex: 1 }}>{created.token}</code>
                        <button className="add-btn" onClick={() => copyText(created.token)} style={{ fontSize: '11px', padding: '4px 10px' }}>Копировать</button>
                        <button className="add-btn" onClick={() => setCreated(null)} style={{ fontSize: '11px', padding: '4px 10px' }}>Скрыть</button>
                    </div>
                </div>
            )}

            <div className="items-list" style={{ marginTop: '8px' }}>
                {tokens.map(t => (
                    <div key={t.id} className="settings-card" style={{ padding: '8px 12px', marginBottom: '6px' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                            <span style={{ fontSize: '13px' }}>{t.name}</span>
                            <span className="info-badge">{CAPABILITY_LABELS[t.capability] ?? t.capability}</span>
                            {t.revoked && <span className="info-badge" style={{ color: '#e0606a' }}>отозван</span>}
                            {t.expires_at_unix && !t.revoked && (
                                <span className="info-badge">до {new Date(t.expires_at_unix * 1000).toLocaleDateString()}</span>
                            )}
                            <span style={{ marginLeft: 'auto' }}>
                                {!t.revoked && (
                                    <button className="add-btn" onClick={() => revoke(t.id)} style={{ fontSize: '11px', padding: '4px 10px' }}>
                                        Отозвать
                                    </button>
                                )}
                            </span>
                        </div>
                    </div>
                ))}
                {tokens.length === 0 && (
                    <p style={{ color: '#666', fontSize: '12px' }}>Токенов пока нет</p>
                )}
            </div>

            {error && <p style={{ color: '#e0606a', fontSize: '12px', margin: '8px 0 0' }}>{error}</p>}
        </div>
    );
}
