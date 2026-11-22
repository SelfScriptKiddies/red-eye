import { useCallback, useEffect, useState } from 'react';
import { copyText } from '../../lib/api';
import {
    createMcpToken,
    fetchMcpTokens,
    fetchSocStatus,
    revokeMcpToken,
    type McpToken,
} from '../../lib/socTriage';

export function McpTokensBlock() {
    const [tokens, setTokens] = useState<McpToken[]>([]);
    const [name, setName] = useState('');
    const [capability, setCapability] = useState('read_only');
    const [expiryDays, setExpiryDays] = useState('');
    const [fullControlAllowed, setFullControlAllowed] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [created, setCreated] = useState<{ name: string; token: string } | null>(null);

    const reload = useCallback(async () => {
        try {
            const [list, status] = await Promise.all([fetchMcpTokens(), fetchSocStatus().catch(() => null)]);
            setTokens(list);
            setFullControlAllowed(status?.mcp_service_capability === 'full_control');
        } catch (e) {
            setError(`Не удалось загрузить токены: ${e}`);
        }
    }, []);

    useEffect(() => { reload(); }, [reload]);

    const create = async () => {
        setError(null);
        if (!name.trim()) {
            setError('Укажите название');
            return;
        }
        try {
            const days = Number(expiryDays);
            const res = await createMcpToken(name.trim(), capability, Number.isFinite(days) && days > 0 ? days * 86400 : undefined);
            setCreated({ name: name.trim(), token: res.token });
            setName('');
            await reload();
        } catch (e) {
            setError(`Не удалось создать: ${e}`);
        }
    };

    const revoke = async (id: number) => {
        setError(null);
        try {
            await revokeMcpToken(id);
            await reload();
        } catch (e) {
            setError(`Не удалось отозвать: ${e}`);
        }
    };

    const inputStyle = { padding: '6px 10px', background: '#1a1a1a', border: '1px solid transparent', borderRadius: '8px', color: 'white', fontSize: '12px' } as const;

    return (
        <div>
            <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                <input
                    type="text"
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                    placeholder="Название"
                    style={{ ...inputStyle, flex: 1, minWidth: '120px' }}
                />
                <select
                    value={capability}
                    onChange={(e) => setCapability(e.target.value)}
                    style={inputStyle}
                >
                    <option value="read_only">read_only</option>
                    <option value="full_control" disabled={!fullControlAllowed}>
                        full_control
                    </option>
                </select>
                <input
                    type="text"
                    inputMode="numeric"
                    value={expiryDays}
                    onChange={(e) => setExpiryDays(e.target.value)}
                    placeholder="Дней"
                    title="Срок в днях; пусто - бессрочно"
                    style={{ ...inputStyle, width: '64px' }}
                />
                <button className="ai-btn" onClick={create}>Создать</button>
            </div>
            {!fullControlAllowed && (
                <p style={{ color: '#666', fontSize: '11px', margin: '6px 0 0' }}>
                    full_control требует MCP_MODE=full_control в деплое
                </p>
            )}

            {created && (
                <div style={{
                    border: '1px solid #87d37c', borderRadius: '8px', padding: '10px',
                    margin: '10px 0', background: 'rgba(135, 211, 124, 0.08)', fontSize: '12px',
                }}>
                    <b>{created.name}</b> - токен показан один раз:
                    <div style={{ display: 'flex', gap: '6px', marginTop: '6px', alignItems: 'center' }}>
                        <code style={{ wordBreak: 'break-all', flex: 1 }}>{created.token}</code>
                        <button className="ai-btn" onClick={() => copyText(created.token)}>Копировать</button>
                        <button className="ai-btn" onClick={() => setCreated(null)}>Скрыть</button>
                    </div>
                </div>
            )}

            <div className="items-list" style={{ marginTop: '8px' }}>
                {tokens.map(t => (
                    <div key={t.id} className="settings-card" style={{ padding: '6px 10px', marginBottom: '6px' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                            <span style={{ fontSize: '12px' }}>{t.name}</span>
                            <span className="info-badge">{t.capability}</span>
                            {t.revoked && <span className="info-badge" style={{ color: '#e0606a' }}>отозван</span>}
                            {t.expires_at_unix && !t.revoked && (
                                <span className="info-badge">до {new Date(t.expires_at_unix * 1000).toLocaleDateString()}</span>
                            )}
                            {!t.revoked && (
                                <button className="ai-btn" style={{ marginLeft: 'auto' }} onClick={() => revoke(t.id)}>
                                    Отозвать
                                </button>
                            )}
                        </div>
                    </div>
                ))}
                {tokens.length === 0 && (
                    <p style={{ color: '#666', fontSize: '12px' }}>Токенов нет</p>
                )}
            </div>

            {error && <p style={{ color: '#e0606a', fontSize: '12px', margin: '8px 0 0' }}>{error}</p>}
        </div>
    );
}
