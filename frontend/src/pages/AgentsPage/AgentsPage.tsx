import { useState, useEffect } from 'react';
import './AgentsPage.css';

interface NftRedirect {
  protocol: string;
  from_port: number;
  to_port: number;
}

interface Service {
  id: number;
  name: string;
  port: number;
  http_parse: boolean;
  decrypt_tls: boolean;
}

interface Agent {
  agent_id: string;
  uptime_secs: number;
  sniffer_active: boolean;
  sniffer_ports: number[];
  harpoon_rules: string[];
  active_filters: number;
  nft_redirects: NftRedirect[];
  tls_available: boolean;
  timestamp_ms: number;
  online: boolean;
}

interface ProxyForm {
  name: string;
  listen: string;
  target: string;
  protocol: string;
  tls_mode: string;
  nft_redirect_port: string;
  duplicate_to: string;
}

interface FilterForm {
  name: string;
  kind: string;
  pattern: string;
  direction: string;
  action: string;
}

interface ReplayForm {
  name: string;
  listen_port: string;
  target_addr: string;
  protocol: string;
}

const defaultProxy: ProxyForm = {
  name: '', listen: '', target: '', protocol: 'tcp',
  tls_mode: '', nft_redirect_port: '', duplicate_to: ''
};

const defaultFilter: FilterForm = {
  name: '', kind: 'substr', pattern: '', direction: 'c2s', action: 'drop'
};

const defaultReplay: ReplayForm = {
  name: '', listen_port: '', target_addr: '', protocol: 'tcp'
};

function formatUptime(secs: number): string {
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = secs % 60;
  return `${h}h ${m}m ${s}s`;
}

export default function AgentsPage() {
  const [agents, setAgents] = useState<Agent[]>([]);
  const [selected, setSelected] = useState<string | null>(null);

  const [proxyModal, setProxyModal] = useState(false);
  const [proxyForm, setProxyForm] = useState<ProxyForm>(defaultProxy);

  const [filterModal, setFilterModal] = useState(false);
  const [filters, setFilters] = useState<FilterForm[]>([]);

  const [replayModal, setReplayModal] = useState(false);
  const [replayForm, setReplayForm] = useState<ReplayForm>(defaultReplay);

  // Sniffer config state — previously missing entirely, which made the
  // "Настроить" button throw a ReferenceError at runtime. The modal UI that
  // will read these values is not implemented yet; only the setters are used.
  const [, setSnifferModal] = useState(false);
  const [, setSnifferPorts] = useState('');

  const [services, setServices] = useState<Service[]>([]);
  const [boundServices, setBoundServices] = useState<number[]>([]);

  const fetchAgents = () => {
    fetch('/api/agent')
      .then(r => r.json())
      .then(data => setAgents(Array.isArray(data) ? data : Object.values(data)))
      .catch(() => {});
  };

  const fetchServices = () => {
    fetch('/api/service')
      .then(r => r.json())
      .then(data => setServices(Array.isArray(data) ? data : Object.values(data)))
      .catch(() => {});
  };

  const fetchBoundServices = (agentId: string) => {
    fetch(`/api/agent/${agentId}/services`)
      .then(r => r.json())
      .then(data => setBoundServices(Array.isArray(data) ? data : []))
      .catch(() => setBoundServices([]));
  };

  const bindService = (serviceId: number) => {
    if (!selected) return;
    fetch(`/api/agent/${selected}/services/bind`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ service_id: serviceId }),
    }).then(() => {
      fetchBoundServices(selected);
      setTimeout(fetchAgents, 1000);
    });
  };

  const unbindService = (serviceId: number) => {
    if (!selected) return;
    fetch(`/api/agent/${selected}/services/unbind`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ service_id: serviceId }),
    }).then(() => {
      fetchBoundServices(selected);
      setTimeout(fetchAgents, 1000);
    });
  };

  // Check protection status for a service on selected agent
  const getProtectionStatus = (service: Service): 'off' | 'sniffing' | 'proxy' | 'proxy-nft' => {
    if (!selectedAgent) return 'off';
    const ruleName = `protect-${service.name.replace(/ /g, '-').toLowerCase()}`;
    const hasProxy = selectedAgent.harpoon_rules.includes(ruleName);
    const hasNft = selectedAgent.nft_redirects.some(r => r.from_port === service.port);
    const isSniffed = selectedAgent.sniffer_ports.includes(service.port);

    if (hasProxy && hasNft) return 'proxy-nft';
    if (hasProxy) return 'proxy';
    if (isSniffed) return 'sniffing';
    return 'off';
  };

  const statusLabel: Record<string, string> = {
    'off': 'Off',
    'sniffing': 'Sniffing',
    'proxy': 'Proxy',
    'proxy-nft': 'Protected',
  };

  const statusStyle: Record<string, string> = {
    'off': '',
    'sniffing': 'info-badge',
    'proxy': 'info-badge',
    'proxy-nft': 'option-badge',
  };

  const toggleProtection = async (service: Service, enable: boolean, tlsMitm: boolean = false) => {
    if (!selected) return;
    await fetch('/api/protect', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        service_id: service.id,
        agent_id: selected,
        enable,
        tls_mitm: tlsMitm,
      }),
    });
    setTimeout(fetchAgents, 2000);
  };

  useEffect(() => {
    fetchAgents();
    fetchServices();
    const interval = setInterval(fetchAgents, 5000);
    return () => clearInterval(interval);
  }, []);

  const selectedAgent = agents.find(a => a.agent_id === selected);

  const sendCommand = (agentId: string, command: object) => {
    fetch(`/api/agent/${agentId}/command`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(command)
    }).then(() => setTimeout(fetchAgents, 1000));
  };

  const broadcastCommand = (command: object, agentIds: string[] = []) => {
    fetch('/api/agent/broadcast', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ agent_ids: agentIds, ...command })
    }).then(() => setTimeout(fetchAgents, 1000));
  };

  // Proxy
  const saveProxy = () => {
    if (!selected || !proxyForm.name.trim() || !proxyForm.listen.trim() || !proxyForm.target.trim()) return;
    const cmd: any = {
      type: 'ProxyAdd',
      name: proxyForm.name,
      listen: proxyForm.listen,
      target: proxyForm.target,
      protocol: proxyForm.protocol
    };
    if (proxyForm.tls_mode) cmd.tls_mode = proxyForm.tls_mode;
    if (proxyForm.nft_redirect_port) cmd.nft_redirect_port = parseInt(proxyForm.nft_redirect_port);
    if (proxyForm.duplicate_to) cmd.duplicate_to = proxyForm.duplicate_to;
    sendCommand(selected, cmd);
    setProxyModal(false);
    setProxyForm(defaultProxy);
  };

  const removeProxy = (ruleName: string) => {
    if (!selected || !confirm(`Удалить прокси "${ruleName}"?`)) return;
    sendCommand(selected, { type: 'ProxyRemove', name: ruleName });
  };

  // Filters
  const addFilterRow = () => setFilters([...filters, { ...defaultFilter }]);
  const removeFilterRow = (i: number) => setFilters(filters.filter((_, idx) => idx !== i));
  const updateFilterRow = (i: number, field: string, value: string) => {
    const updated = [...filters];
    (updated[i] as any)[field] = value;
    setFilters(updated);
  };

  const syncFilters = () => {
    if (!selected) return;
    sendCommand(selected, { type: 'FilterSync', filters });
    setFilterModal(false);
  };

  const syncFiltersAll = () => {
    broadcastCommand({ type: 'FilterSync', filters });
    setFilterModal(false);
  };

  // Replay
  const saveReplay = () => {
    if (!selected || !replayForm.name.trim() || !replayForm.listen_port || !replayForm.target_addr.trim()) return;
    sendCommand(selected, {
      type: 'ReplayForward',
      name: replayForm.name,
      listen_port: parseInt(replayForm.listen_port),
      target_addr: replayForm.target_addr,
      protocol: replayForm.protocol,
    });
    setReplayModal(false);
    setReplayForm(defaultReplay);
  };

  // Sniffer config is applied via the (not yet implemented) sniffer modal;
  // when it lands it should build on setSnifferPorts above.

  return (
    <div className="AgentsPage">
      <div className="agents-columns">
        {/* Column 1: Agent List */}
        <div className="agents-column">
          <div className="column-header">
            <h2>Агенты</h2>
            <span className="agent-count">{agents.filter(a => a.online).length}/{agents.length}</span>
          </div>
          <div className="items-list">
            {agents.map(agent => (
              <div
                key={agent.agent_id}
                className={`agent-card ${selected === agent.agent_id ? 'selected' : ''} ${agent.online ? 'online' : 'offline'}`}
                onClick={() => { setSelected(agent.agent_id); fetchBoundServices(agent.agent_id); }}
              >
                <div className="agent-card-header">
                  <span className={`status-dot ${agent.online ? 'online' : 'offline'}`} />
                  <span className="agent-name">{agent.agent_id}</span>
                </div>
                <div className="agent-card-meta">
                  <span className="badge info-badge">{formatUptime(agent.uptime_secs)}</span>
                  {agent.harpoon_rules.length > 0 && (
                    <span className="badge option-badge">{agent.harpoon_rules.length} proxy</span>
                  )}
                  {agent.active_filters > 0 && (
                    <span className="badge option-badge">{agent.active_filters} filters</span>
                  )}
                  {agent.tls_available && <span className="badge info-badge">TLS</span>}
                </div>
              </div>
            ))}
            {agents.length === 0 && (
              <div className="placeholder-content"><p>Нет подключенных агентов</p></div>
            )}
          </div>
        </div>

        {/* Column 2: Agent Details */}
        <div className="agents-column">
          {selectedAgent ? (
            <>
              <div className="column-header">
                <h2>{selectedAgent.agent_id}</h2>
                <span className={`status-text ${selectedAgent.online ? 'online' : 'offline'}`}>
                  {selectedAgent.online ? 'Online' : 'Offline'}
                </span>
              </div>
              <div className="items-list">
                {/* Sniffer */}
                <div className="detail-section">
                  <div className="section-header">
                    <h3>Sniffer</h3>
                    <button className="small-btn" onClick={() => {
                      setSnifferPorts(selectedAgent.sniffer_ports.join(', '));
                      setSnifferModal(true);
                    }}>Настроить</button>
                  </div>
                  <div className="badges-row">
                    {selectedAgent.sniffer_ports.map(p => (
                      <span key={p} className="badge info-badge">:{p}</span>
                    ))}
                    {selectedAgent.sniffer_ports.length === 0 && <span className="text-muted">Нет портов</span>}
                  </div>
                </div>

                {/* Proxy Rules */}
                <div className="detail-section">
                  <div className="section-header">
                    <h3>Proxy Rules</h3>
                    <button className="small-btn" onClick={() => setProxyModal(true)}>+ Добавить</button>
                  </div>
                  {selectedAgent.harpoon_rules.map(rule => (
                    <div key={rule} className="settings-card">
                      <div className="card-row">
                        <span className="rule-name">{rule}</span>
                        <div className="card-actions">
                          <button onClick={() => removeProxy(rule)}>✕</button>
                        </div>
                      </div>
                    </div>
                  ))}
                  {selectedAgent.harpoon_rules.length === 0 && <span className="text-muted">Нет прокси-правил</span>}
                </div>

                {/* NFTables */}
                {selectedAgent.nft_redirects.length > 0 && (
                  <div className="detail-section">
                    <h3>NFTables Redirects</h3>
                    {selectedAgent.nft_redirects.map((r, i) => (
                      <div key={i} className="badges-row">
                        <span className="badge info-badge">{r.protocol}</span>
                        <span className="badge option-badge">:{r.from_port} → :{r.to_port}</span>
                      </div>
                    ))}
                  </div>
                )}

                {/* Filters */}
                <div className="detail-section">
                  <div className="section-header">
                    <h3>Firewall Filters</h3>
                    <span className="badge info-badge">{selectedAgent.active_filters} active</span>
                  </div>
                </div>

                {/* Services bound to this agent */}
                <div className="detail-section">
                  <div className="section-header">
                    <h3>Сервисы</h3>
                  </div>
                  {services.map(svc => {
                    const isBound = boundServices.includes(svc.id);
                    const status = getProtectionStatus(svc);
                    const hasProxy = status === 'proxy' || status === 'proxy-nft';
                    return (
                      <div key={svc.id} className={`settings-card ${!isBound ? 'card-dimmed' : ''}`}>
                        <div className="card-row">
                          <span className="rule-name">{svc.name}</span>
                          <div className="card-right">
                            <span className="port-label">:{svc.port}</span>
                            {isBound && (
                              <span className={`badge ${statusStyle[status]}`}
                                style={status === 'off' ? { color: '#666', border: '1px solid #444', fontSize: '10px', padding: '2px 6px' } : {}}>
                                {statusLabel[status]}
                              </span>
                            )}
                          </div>
                        </div>
                        <div className="card-buttons">
                          {isBound ? (
                            <>
                              {!hasProxy ? (
                                <>
                                  <button className="small-btn" onClick={() => toggleProtection(svc, true)}>Proxy</button>
                                  <button className="small-btn" onClick={() => toggleProtection(svc, true, true)}>Proxy + TLS</button>
                                </>
                              ) : (
                                <button className="small-btn btn-danger" onClick={() => toggleProtection(svc, false)}>Отключить защиту</button>
                              )}
                              <button className="small-btn btn-danger" onClick={() => unbindService(svc.id)}>Отвязать</button>
                            </>
                          ) : (
                            <button className="small-btn" onClick={() => bindService(svc.id)}>Привязать</button>
                          )}
                        </div>
                      </div>
                    );
                  })}
                  {services.length === 0 && <span className="text-muted">Нет сервисов</span>}
                </div>
              </div>
            </>
          ) : (
            <>
              <div className="column-header">
                <h2>Детали</h2>
              </div>
              <div className="placeholder-content"><p>Выберите агента</p></div>
            </>
          )}
        </div>

        {/* Column 3: Actions */}
        <div className="agents-column">
          <div className="column-header"><h2>Действия</h2></div>
          <div className="items-list">
            <div className="action-group">
              <h3>Для выбранного</h3>
              <button className="action-btn" disabled={!selected} onClick={() => setReplayModal(true)}>Port Forward</button>
              <button className="action-btn" disabled={!selected} onClick={() => { setFilters([]); setFilterModal(true); }}>Firewall</button>
            </div>
            <div className="action-group">
              <h3>Для всех агентов</h3>
              <button className="action-btn broadcast" onClick={() => { setFilters([]); setFilterModal(true); }}>Firewall (все агенты)</button>
            </div>
          </div>
        </div>
      </div>

      {/* Proxy Modal */}
      {proxyModal && (
        <div className="modal-overlay" onClick={() => setProxyModal(false)}>
          <div className="modal-content" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <h3>Добавить прокси</h3>
              <button className="close-btn" onClick={() => setProxyModal(false)}>✕</button>
            </div>
            <div className="modal-body">
              <input placeholder="Имя правила" value={proxyForm.name}
                onChange={e => setProxyForm({...proxyForm, name: e.target.value})} />
              <input placeholder="Listen (0.0.0.0:19080)" value={proxyForm.listen}
                onChange={e => setProxyForm({...proxyForm, listen: e.target.value})} />
              <input placeholder="Target (127.0.0.1:8080)" value={proxyForm.target}
                onChange={e => setProxyForm({...proxyForm, target: e.target.value})} />
              <select className="select-input" value={proxyForm.protocol}
                onChange={e => setProxyForm({...proxyForm, protocol: e.target.value})}>
                <option value="tcp">TCP</option>
                <option value="udp">UDP</option>
              </select>
              <select className="select-input" value={proxyForm.tls_mode}
                onChange={e => setProxyForm({...proxyForm, tls_mode: e.target.value})}>
                <option value="">Без TLS</option>
                <option value="mitm">MITM</option>
                <option value="terminate">Terminate</option>
                <option value="passthrough">Passthrough</option>
              </select>
              <input placeholder="NFT Redirect Port (необяз.)" value={proxyForm.nft_redirect_port}
                onChange={e => setProxyForm({...proxyForm, nft_redirect_port: e.target.value})} />
              <input placeholder="Duplicate To (необяз., IP:port)" value={proxyForm.duplicate_to}
                onChange={e => setProxyForm({...proxyForm, duplicate_to: e.target.value})} />
            </div>
            <div className="modal-footer">
              <button onClick={() => setProxyModal(false)}>Отмена</button>
              <button onClick={saveProxy}>Добавить</button>
            </div>
          </div>
        </div>
      )}

      {/* Filter Modal */}
      {filterModal && (
        <div className="modal-overlay" onClick={() => setFilterModal(false)}>
          <div className="modal-content wide" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <h3>Фильтры файервола</h3>
              <button className="close-btn" onClick={() => setFilterModal(false)}>✕</button>
            </div>
            <div className="modal-body">
              {filters.map((f, i) => (
                <div key={i} className="filter-row">
                  <input placeholder="Имя" value={f.name}
                    onChange={e => updateFilterRow(i, 'name', e.target.value)} />
                  <select className="select-input" value={f.kind}
                    onChange={e => updateFilterRow(i, 'kind', e.target.value)}>
                    <option value="substr">Substr</option>
                    <option value="regex">Regex</option>
                    <option value="bsubstr">Binary</option>
                  </select>
                  <input placeholder="Паттерн" value={f.pattern}
                    onChange={e => updateFilterRow(i, 'pattern', e.target.value)} />
                  <select className="select-input" value={f.direction}
                    onChange={e => updateFilterRow(i, 'direction', e.target.value)}>
                    <option value="c2s">C→S</option>
                    <option value="s2c">S→C</option>
                    <option value="both">Both</option>
                  </select>
                  <select className="select-input" value={f.action}
                    onChange={e => updateFilterRow(i, 'action', e.target.value)}>
                    <option value="drop">Drop Packet</option>
                    <option value="drop_connection">Drop Connection</option>
                    <option value="alert">Alert</option>
                  </select>
                  <button className="remove-btn" onClick={() => removeFilterRow(i)}>✕</button>
                </div>
              ))}
              <button className="add-row-btn" onClick={addFilterRow}>+ Добавить фильтр</button>
            </div>
            <div className="modal-footer">
              <button onClick={() => setFilterModal(false)}>Отмена</button>
              {selected && <button onClick={syncFilters}>Применить к {selected}</button>}
              <button onClick={syncFiltersAll}>Применить ко всем</button>
            </div>
          </div>
        </div>
      )}

      {/* Replay Modal */}
      {replayModal && (
        <div className="modal-overlay" onClick={() => setReplayModal(false)}>
          <div className="modal-content" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <h3>Port Forward</h3>
              <button className="close-btn" onClick={() => setReplayModal(false)}>✕</button>
            </div>
            <div className="modal-body">
              <input placeholder="Имя" value={replayForm.name}
                onChange={e => setReplayForm({...replayForm, name: e.target.value})} />
              <select value={replayForm.protocol}
                onChange={e => setReplayForm({...replayForm, protocol: e.target.value})}>
                <option value="tcp">TCP</option>
                <option value="udp">UDP</option>
              </select>
              <input placeholder="Listen Port (29000)" value={replayForm.listen_port}
                onChange={e => setReplayForm({...replayForm, listen_port: e.target.value})} />
              <input placeholder="Target (10.80.2.1:8080)" value={replayForm.target_addr}
                onChange={e => setReplayForm({...replayForm, target_addr: e.target.value})} />
            </div>
            <div className="modal-footer">
              <button onClick={() => setReplayModal(false)}>Отмена</button>
              <button onClick={saveReplay}>Запустить</button>
            </div>
          </div>
        </div>
      )}

      {/* Sniffer modal removed — ports managed via service bindings */}
    </div>
  );
}
