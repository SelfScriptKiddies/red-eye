import './SettingsPage.css'
import { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { AgentContainerInfo, AgentListItem, apiFetch, copyText, fetchAgentContainers } from '../../lib/api';
import {
    buildSanityCheck,
    buildStreamsScopeLink,
    expressionUsesPayload,
    filterInspectsPayload,
    validateFilterForm,
} from '../../lib/filterSanity';

interface ServiceVersionInfo {
    version?: string | null;
    git_sha?: string | null;
    error?: string | null;
}

interface VersionsResponse {
    backend: { version: string; git_sha: string };
    analyzer?: ServiceVersionInfo;
    frontend?: ServiceVersionInfo;
    agents?: { agent_id: string; online: boolean; version?: string | null }[];
    cache?: { ttl_secs: number; age_secs: number };
}

function VersionRow({ name, info }: { name: string; info?: ServiceVersionInfo }) {
    const version = info?.version && info?.git_sha
        ? `${info.version}+${info.git_sha}`
        : null;
    return (
        <div className="version-row">
            <span className="version-name">{name}</span>
            {version ? (
                <span className={`version-value ${info.git_sha && info.git_sha !== 'unknown' ? '' : 'version-stale'}`}>
                    {version}
                </span>
            ) : info?.error ? (
                <span className="version-error" title={info.error}>недоступен</span>
            ) : (
                <span className="version-error">нет данных</span>
            )}
        </div>
    );
}

/** Versions of every service from /api/version (60s server-side cache).
 * Different git shas across rows = the stack drifted apart — redeploy. */
function VersionsBlock() {
    const { data, isLoading, error } = useQuery({
        queryKey: ['versions'],
        queryFn: () => apiFetch<VersionsResponse>('/api/version'),
        refetchInterval: 30_000,
    });

    if (isLoading) return <p className="text-muted" style={{ fontSize: '12px' }}>Загрузка…</p>;
    if (error || !data) return <p className="text-muted" style={{ fontSize: '12px' }}>Версии недоступны</p>;

    const shas = [data.backend?.git_sha, data.analyzer?.git_sha, data.frontend?.git_sha]
        .filter((s): s is string => Boolean(s) && s !== 'unknown');
    const drifted = new Set(shas).size > 1;

    return (
        <div className="versions-block">
            <VersionRow name="backend" info={data.backend} />
            <VersionRow name="analyzer" info={data.analyzer} />
            <VersionRow name="frontend" info={data.frontend} />
            {(data.agents ?? []).length > 0 && (
                <>
                    <div className="version-sep">агенты</div>
                    {(data.agents ?? []).map(a => {
                        // a.version is already the full "0.1.0+sha" — split it
                        // back for the row renderer.
                        const [agentVer, agentSha] = (a.version ?? '').split('+');
                        return (
                            <VersionRow
                                key={a.agent_id}
                                name={a.agent_id + (a.online ? '' : ' (offline)')}
                                info={{ version: agentVer || null, git_sha: agentSha || null }}
                            />
                        );
                    })}
                </>
            )}
            {drifted && (
                <div className="version-drift">⚠ Версии разошлись — переразвёртка</div>
            )}
        </div>
    );
}

interface Service {
    id?: number;
    name: string;
    port: number;
    /** Transport protocol: "TCP" | "UDP" (required on create) */
    protocol: string;
    /** true = applies to all agents; false = explicit agent_ids set */
    is_global: boolean;
    /** Explicit agent set when not global */
    agent_ids: string[];
    chunked_transfer_encoding: boolean;
    urldecode: boolean;
    merge_adjacent_packets: boolean;
    inflate_websockets: boolean;
    decrypt_tls: boolean;
    http_parse: boolean;
    http2_parse: boolean;
    alert_flag_leak?: boolean;
}

interface AgentForAssignment {
    agent_id: string;
    online: boolean;
}

interface Threshold {
    threshold_count: number | null;
    threshold_window_secs: number | null;
    threshold_group_by: string | null;
    threshold_cooldown_secs: number | null;
    threshold_block_secs: number | null;
}

interface Filter extends Threshold {
    id?: number;
    name: string;
    color: number;
    template_type: string;
    template: string;
    direction: string;
    special_type: string | null;
    action: string;
    enabled: boolean;
    ip_network: string | null;
    ip_endpoint: string | null;
    expression_source: string | null;
    service_targets_global: boolean;
    agent_targets_global: boolean;
    service_ids: number[];
    agent_ids: string[];
    expires_at: string | null;
    revision: number;
}

interface ActiveBlock {
    id: number;
    filter_id: number;
    filter_name: string;
    agent_id: string;
    remote_ip: string;
    remaining_secs: number;
}

interface FilterEventRow {
    id: number;
    filter_id: number;
    agent_id: string;
    remote_ip: string | null;
    kind: string;
    created_at: number;
}

const emptyFilter = (): Filter => ({
    name: '',
    color: 0,
    template_type: 'regex',
    template: '',
    direction: 'both',
    special_type: null,
    action: 'tag',
    enabled: true,
    ip_network: null,
    ip_endpoint: 'src',
    expression_source: null,
    service_targets_global: true,
    agent_targets_global: true,
    service_ids: [],
    agent_ids: [],
    threshold_count: null,
    threshold_window_secs: null,
    threshold_group_by: null,
    threshold_cooldown_secs: null,
    threshold_block_secs: null,
    expires_at: null,
    revision: 0,
});

export function SettingsPage() {
    // Settings state
    const [farmUrl, setFarmUrl] = useState('');
    const [farmEnabled, setFarmEnabled] = useState(false);
    const [, setSettingsLoaded] = useState(false);

    // Services state
    const [services, setServices] = useState<Service[]>([]);
    const [serviceModalOpen, setServiceModalOpen] = useState(false);
    const [editingService, setEditingService] = useState<Service | null>(null);
    const [serviceForm, setServiceForm] = useState<Service>({
        name: '',
        port: 8080,
        protocol: 'TCP',
        is_global: true,
        agent_ids: [],
        chunked_transfer_encoding: false,
        urldecode: false,
        merge_adjacent_packets: false,
        inflate_websockets: false,
        decrypt_tls: false,
        http_parse: false,
        http2_parse: false,
        alert_flag_leak: false
    });
    // Known agents for the explicit-assignment editor (online + offline)
    const [agentsForAssignment, setAgentsForAssignment] = useState<AgentForAssignment[]>([]);
    // "pending sync" notice from the last save (publication failure)
    const [serviceSyncNotice, setServiceSyncNotice] = useState<string | null>(null);
    
    // Filters state
    const [filters, setFilters] = useState<Filter[]>([]);
    const [filterModalOpen, setFilterModalOpen] = useState(false);
    const [editingFilter, setEditingFilter] = useState<Filter | null>(null);
    const [filterForm, setFilterForm] = useState<Filter>(emptyFilter());
    const [filterError, setFilterError] = useState<string | null>(null);
    // Active blocks + recent threshold events (Settings refresh both)
    const [activeBlocks, setActiveBlocks] = useState<ActiveBlock[]>([]);
    const [filterEvents, setFilterEvents] = useState<FilterEventRow[]>([]);
    // Sanity check state (test payload + generated curl)
    const [sanitySample, setSanitySample] = useState('');
    const [sanityHost, setSanityHost] = useState('');
    const [sanityServiceId, setSanityServiceId] = useState('');
    const [sanityAgentId, setSanityAgentId] = useState('');
    const [sanityResult, setSanityResult] = useState<{ curl: string; resultLink: string; nonce: string } | null>(null);

    // "Add by container" state
    const [containerModalOpen, setContainerModalOpen] = useState(false);
    const [agentsList, setAgentsList] = useState<AgentListItem[]>([]);
    const [containerAgentId, setContainerAgentId] = useState('');
    // null = still loading/polling the agent reply
    const [containers, setContainers] = useState<AgentContainerInfo[] | null>(null);
    const [containersError, setContainersError] = useState<string | null>(null);
    // true when the error came from the agent's Docker (daemon down / socket
    // missing) — drives the "is Docker running?" hint
    const [containersDockerError, setContainersDockerError] = useState(false);
    const [containersStaleAgeSec, setContainersStaleAgeSec] = useState<number | null>(null);
    const [selectedContainerName, setSelectedContainerName] = useState('');
    const [containerPortCustom, setContainerPortCustom] = useState(false);
    const [containerPort, setContainerPort] = useState(0);
    const [containerServiceName, setContainerServiceName] = useState('');
    const [creatingFromContainer, setCreatingFromContainer] = useState(false);

    // Proto files state
    const [protoFiles, setProtoFiles] = useState<{id: number, service_id: number, filename: string}[]>([]);
    const [protoServiceId, setProtoServiceId] = useState('');
    const [protoFilename, setProtoFilename] = useState('');
    const [protoContent, setProtoContent] = useState('');

    const API_URL = '/api';

    const fetchSettings = async () => {
        try {
            const resp = await fetch(`${API_URL}/settings`);
            if (resp.ok) {
                const data = await resp.json();
                setFarmUrl(data.farm_url || '');
                setFarmEnabled(data.farm_enabled === 'true');
                setSettingsLoaded(true);
            }
        } catch (e) { console.error('Error fetching settings:', e); }
    };

    const saveSetting = async (key: string, value: string) => {
        try {
            await fetch(`${API_URL}/settings/${key}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ value })
            });
        } catch (e) { console.error('Error saving setting:', e); }
    };

    const fetchProtoFiles = async () => {
        try {
            const resp = await fetch(`${API_URL}/proto`);
            if (resp.ok) setProtoFiles(await resp.json());
        } catch { /* proto listing is non-critical */ }
    };

    const uploadProto = async () => {
        if (!protoServiceId || !protoFilename.trim() || !protoContent.trim()) return;
        await fetch(`${API_URL}/proto`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ service_id: parseInt(protoServiceId), filename: protoFilename, content: protoContent })
        });
        setProtoFilename('');
        setProtoContent('');
        fetchProtoFiles();
    };

    const deleteProto = async (id: number) => {
        if (!confirm('Удалить .proto файл?')) return;
        await fetch(`${API_URL}/proto/${id}`, { method: 'DELETE' });
        fetchProtoFiles();
    };

    // Fetch on mount
    useEffect(() => {
        fetchServices();
        fetchFilters();
        fetchSettings();
        fetchProtoFiles();
    }, []);

    // Services CRUD operations
    const fetchServices = async () => {
        try {
            const response = await fetch(`${API_URL}/service`);
            if (response.ok) {
                const data = await response.json();
                setServices(Object.values(data));
            }
        } catch (error) {
            console.error('Error fetching services:', error);
        }
    };

    const openServiceModal = (service?: Service) => {
        if (service) {
            setEditingService(service);
            setServiceForm(service);
        } else {
            setEditingService(null);
            setServiceForm({
                name: '',
                port: 8080,
                protocol: 'TCP',
                is_global: true,
                agent_ids: [],
                chunked_transfer_encoding: false,
                urldecode: false,
                merge_adjacent_packets: false,
                inflate_websockets: false,
                decrypt_tls: false,
                http_parse: false,
                http2_parse: false,
                alert_flag_leak: false
            });
        }
        // Refresh the agent list the operator assigns to (online + offline)
        fetch(`${API_URL}/agent`)
            .then(r => (r.ok ? r.json() : []))
            .then(data => setAgentsForAssignment(Array.isArray(data) ? data : Object.values(data)))
            .catch(() => {});
        setServiceSyncNotice(null);
        setServiceModalOpen(true);
    };

    const closeServiceModal = () => {
        setServiceModalOpen(false);
        setEditingService(null);
    };

    const saveService = async () => {
        if (!serviceForm.name.trim()) return;
        const port = Number(serviceForm.port);
        if (!Number.isInteger(port) || port < 1 || port > 65535) {
            setServiceSyncNotice('Порт должен быть в диапазоне 1-65535');
            return;
        }

        try {
            const url = editingService
                ? `${API_URL}/service/${editingService.id}`
                : `${API_URL}/service`;
            const method = editingService ? 'PUT' : 'POST';

            const response = await fetch(url, {
                method,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    name: serviceForm.name,
                    port,
                    protocol: serviceForm.protocol,
                    is_global: serviceForm.is_global,
                    agent_ids: serviceForm.is_global ? [] : serviceForm.agent_ids,
                    chunked_transfer_encoding: serviceForm.chunked_transfer_encoding,
                    urldecode: serviceForm.urldecode,
                    merge_adjacent_packets: serviceForm.merge_adjacent_packets,
                    inflate_websockets: serviceForm.inflate_websockets,
                    decrypt_tls: serviceForm.decrypt_tls,
                    http_parse: serviceForm.http_parse,
                    http2_parse: serviceForm.http2_parse,
                    alert_flag_leak: serviceForm.alert_flag_leak ?? false
                })
            });

            if (response.ok) {
                const body = await response.json().catch(() => null);
                // A committed write with failed NATS publication stays
                // visible as pending sync; agents converge later via
                // reconciliation, so the operator must see it.
                if (body?.sync === 'pending') {
                    setServiceSyncNotice('Сохранено, но публикация не удалась — синхронизация агентов в ожидании');
                } else {
                    closeServiceModal();
                }
                await fetchServices();
            } else {
                const text = await response.text().catch(() => '');
                setServiceSyncNotice(`Ошибка сохранения (${response.status}): ${text.slice(0, 200)}`);
            }
        } catch (error) {
            console.error('Error saving service:', error);
            setServiceSyncNotice(`Ошибка сохранения: ${error}`);
        }
    };

    const deleteService = async (id: number) => {
        if (!confirm('Вы уверены, что хотите удалить этот сервис?')) return;
        
        try {
            const response = await fetch(`${API_URL}/service/${id}`, {
                method: 'DELETE'
            });
            
            if (response.ok) {
                await fetchServices();
            }
        } catch (error) {
            console.error('Error deleting service:', error);
        }
    };

    // "Add by container" — fetch agent list on open, poll containers on agent select
    const openContainerModal = async () => {
        setContainers(null);
        setContainersError(null);
        setContainersDockerError(false);
        setContainersStaleAgeSec(null);
        setSelectedContainerName('');
        setContainerServiceName('');
        setContainerPort(0);
        setContainerPortCustom(false);
        setContainerModalOpen(true);
        try {
            const resp = await fetch(`${API_URL}/agent`);
            if (resp.ok) {
                const data = await resp.json();
                const list: AgentListItem[] = Array.isArray(data) ? data : Object.values(data);
                setAgentsList(list);
                // Preselect the first online agent (agent must be reachable
                // to answer the DockerListContainers command)
                const preferred = list.find(a => a.online) || list[0];
                if (preferred) setContainerAgentId(preferred.agent_id);
            }
        } catch (e) { console.error('Error fetching agents:', e); }
    };

    const closeContainerModal = () => {
        setContainerModalOpen(false);
        // Reset the selected agent so reopening the modal re-triggers the
        // containers fetch (data may be stale by then)
        setContainerAgentId('');
    };

    // Poll GET /api/agent/{id}/containers while the modal is open.
    // `pending` means the agent reply hasn't arrived at the backend yet —
    // re-poll up to 3 times ~1s apart, then surface a timeout error.
    useEffect(() => {
        if (!containerModalOpen || !containerAgentId) return;
        const controller = new AbortController();
        let cancelled = false;
        let attempt = 0;

        const poll = async () => {
            setContainers(null);
            setContainersError(null);
            setContainersDockerError(false);
            setContainersStaleAgeSec(null);
            setSelectedContainerName('');
            while (!cancelled) {
                attempt++;
                let resp;
                try {
                    resp = await fetchAgentContainers(containerAgentId, controller.signal);
                } catch (e) {
                    if (cancelled || controller.signal.aborted) return;
                    console.error('Error fetching agent containers:', e);
                    setContainers([]);
                    setContainersError('Не удалось получить список контейнеров');
                    return;
                }
                if (cancelled) return;
                if (resp.state === 'pending' && attempt <= 3) {
                    await new Promise(r => setTimeout(r, 1000));
                    continue;
                }
                if (resp.state === 'pending') {
                    // Agent never replied — offline agent or wedged command path
                    setContainers([]);
                    setContainersError('Агент не ответил на запрос списка контейнеров');
                    return;
                }
                setContainersStaleAgeSec(
                    resp.state === 'stale' && resp.age_ms != null ? Math.round(resp.age_ms / 1000) : null
                );
                if (resp.error) {
                    setContainersDockerError(true);
                    setContainersError(resp.error);
                }
                setContainers(resp.containers);
                return;
            }
        };
        poll();
        return () => { cancelled = true; controller.abort(); };
    }, [containerModalOpen, containerAgentId]);

    const pickContainer = (name: string) => {
        setSelectedContainerName(name);
        const c = containers?.find(x => x.name === name);
        if (!c) return;
        // Service name is just a prefill from the container name — editable
        setContainerServiceName(c.name);
        // Prefer host-published ports (they are what an external client
        // connects to); agent already sorted them, published first below
        const preferred = c.ports.find(p => p.published) || c.ports[0];
        if (preferred) {
            setContainerPortCustom(false);
            setContainerPort(preferred.port);
        } else {
            setContainerPortCustom(true);
            setContainerPort(0);
        }
    };

    const saveContainerService = async () => {
        if (!containerServiceName.trim() || !containerPort) return;
        setCreatingFromContainer(true);
        // Protocol follows the container port report (tcp | udp | sctp);
        // anything but udp degrades to TCP, sctp is not serviceable.
        const selectedContainer = containers?.find(x => x.name === selectedContainerName);
        const preferredPort = selectedContainer?.ports.find(p => p.port === containerPort);
        const protocol = preferredPort?.protocol.toLowerCase() === 'udp' ? 'UDP' : 'TCP';
        try {
            // Same create API and defaults as the manual "add by port" form
            const response = await fetch(`${API_URL}/service`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    name: containerServiceName,
                    port: containerPort,
                    protocol,
                    is_global: true,
                    agent_ids: [],
                    chunked_transfer_encoding: false,
                    urldecode: false,
                    merge_adjacent_packets: false,
                    inflate_websockets: false,
                    decrypt_tls: false,
                    http_parse: false,
                    http2_parse: false,
                    alert_flag_leak: false
                })
            });
            if (response.ok) {
                const body = await response.json().catch(() => null);
                if (body?.sync === 'pending') {
                    alert('Сервис сохранён, но публикация не удалась — синхронизация агентов в ожидании');
                }
                await fetchServices();
                closeContainerModal();
            } else {
                const text = await response.text().catch(() => '');
                alert(`Ошибка создания сервиса (${response.status}): ${text.slice(0, 200)}`);
            }
        } catch (error) {
            console.error('Error creating service from container:', error);
        } finally {
            setCreatingFromContainer(false);
        }
    };

    // Filters CRUD operations
    const fetchFilters = async () => {
        try {
            const response = await fetch(`${API_URL}/tag/names`);
            if (response.ok) {
                const data = await response.json();
                setFilters(Object.values(data));
            }
            const [blocksResp, eventsResp] = await Promise.all([
                fetch(`${API_URL}/blocks?limit=50`),
                fetch(`${API_URL}/filter-events?limit=20`),
            ]);
            if (blocksResp.ok) setActiveBlocks(await blocksResp.json());
            if (eventsResp.ok) setFilterEvents(await eventsResp.json());
        } catch (error) {
            console.error('Error fetching filters:', error);
        }
    };

    const openFilterModal = (filter?: Filter) => {
        setFilterError(null);
        setSanityResult(null);
        setSanitySample('');
        if (filter) {
            setEditingFilter(filter);
            setFilterForm({ ...emptyFilter(), ...filter });
            // Substring matchers prefill the sanity sample from the pattern.
            if (filter.template_type === 'substr' || filter.template_type === 'bsubstr') {
                setSanitySample(filter.template);
            }
        } else {
            setEditingFilter(null);
            setFilterForm(emptyFilter());
        }
        // Agents for the explicit target editor and the sanity target.
        fetch(`${API_URL}/agent`)
            .then(r => (r.ok ? r.json() : []))
            .then(data => setAgentsForAssignment(Array.isArray(data) ? data : Object.values(data)))
            .catch(() => {});
        setFilterModalOpen(true);
    };

    const closeFilterModal = () => {
        setFilterModalOpen(false);
        setEditingFilter(null);
    };

    const buildFilterPayload = (form: Filter) => ({
        name: form.name,
        color: form.color,
        template_type: form.template_type,
        template: form.template_type === 'ip_cidr' || form.template_type === 'expression' ? '' : form.template,
        direction: form.direction,
        special_type: null,
        action: form.action,
        enabled: form.enabled,
        ip_network: form.template_type === 'ip_cidr' ? form.ip_network : null,
        ip_endpoint: form.template_type === 'ip_cidr' ? form.ip_endpoint : null,
        expression_source: form.template_type === 'expression' ? form.expression_source : null,
        service_targets_global: form.service_targets_global,
        service_ids: form.service_targets_global ? [] : form.service_ids,
        agent_targets_global: form.agent_targets_global,
        agent_ids: form.agent_targets_global ? [] : form.agent_ids,
        threshold_count: form.threshold_count,
        threshold_window_secs: form.threshold_window_secs,
        threshold_group_by: form.threshold_group_by,
        threshold_cooldown_secs: form.threshold_cooldown_secs,
        threshold_block_secs: form.threshold_block_secs,
        expires_at: form.expires_at ? new Date(form.expires_at).toISOString() : null,
    });

    const saveFilter = async () => {
        const validationError = validateFilterForm(filterForm);
        if (validationError) {
            setFilterError(validationError);
            return;
        }

        try {
            const url = editingFilter
                ? `${API_URL}/tag/names/${editingFilter.id}`
                : `${API_URL}/tag/names`;
            const method = editingFilter ? 'PUT' : 'POST';

            const response = await fetch(url, {
                method,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(buildFilterPayload(filterForm))
            });

            if (response.ok) {
                const body = await response.json().catch(() => null);
                if (body?.revision && body?.action) {
                    // ok - revision stamped server-side
                }
                await fetchFilters();
                closeFilterModal();
            } else {
                const text = await response.text().catch(() => '');
                setFilterError(`Не удалось сохранить фильтр (${response.status}): ${text.slice(0, 200) || response.statusText}`);
            }
        } catch (error) {
            console.error('Error saving filter:', error);
            setFilterError(`Не удалось сохранить фильтр: ${error}`);
        }
    };

    const toggleFilterEnabled = async (filter: Filter) => {
        try {
            await fetch(`${API_URL}/tag/names/${filter.id}/enabled`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ enabled: !filter.enabled })
            });
            fetchFilters();
        } catch (error) {
            console.error('Error toggling filter:', error);
        }
    };

    const deleteFilter = async (id: number) => {
        if (!confirm('Вы уверены, что хотите удалить этот фильтр?')) return;

        try {
            const response = await fetch(`${API_URL}/tag/names/${id}`, {
                method: 'DELETE'
            });

            if (response.ok) {
                await fetchFilters();
            } else if (response.status !== 404) {
                const text = await response.text().catch(() => '');
                alert(`Не удалось удалить фильтр (${response.status}): ${text || response.statusText}`);
            }
        } catch (error) {
            console.error('Error deleting filter:', error);
            alert(`Не удалось удалить фильтр: ${error}`);
        }
    };

    const unbanBlock = async (id: number) => {
        try {
            const response = await fetch(`${API_URL}/blocks/${id}`, { method: 'DELETE' });
            if (!response.ok && response.status !== 404) {
                const text = await response.text().catch(() => '');
                alert(`Не удалось разблокировать (${response.status}): ${text || response.statusText}`);
            }
            fetchFilters();
        } catch (error) {
            console.error('Error unbanning:', error);
        }
    };

    // Sanity check: generate the replay curl + scoped result link. The
    // command is DISPLAY ONLY - the operator runs it from a suitable host.
    const generateSanity = () => {
        if (!editingFilter && !filters.length) return;
        const filterId = editingFilter?.id;
        const service = services.find(s => String(s.id) === sanityServiceId);
        if (!filterId || !service || !sanityAgentId) {
            setFilterError('Выберите агента и сервис и сохраните фильтр перед проверкой');
            return;
        }
        const host = sanityHost.trim() || '10.10.<team>.<host>';
        const result = buildSanityCheck(
            { id: filterId, template_type: filterForm.template_type, template: filterForm.template, expression_source: filterForm.expression_source, ip_network: filterForm.ip_network },
            sanitySample,
            { agentId: sanityAgentId, serviceName: service.name, serviceHost: host, servicePort: service.port, https: false },
            ''
        );
        setSanityResult(result);
    };

    const streamsScope = editingFilter && !filterInspectsPayload(editingFilter)
        ? buildStreamsScopeLink(editingFilter, sanityAgentId || null, '')
        : null;

    return (
        <div className="SettingsPage">
            <div className="settings-columns">
                {/* Services Column */}
                <div className="settings-column">
                    <div className="column-header">
                        <h2>Сервисы</h2>
                        <div className="header-actions">
                            <button className="add-btn" onClick={() => openServiceModal()}>+ По порту</button>
                            <button className="add-btn" onClick={openContainerModal}>+ По контейнеру</button>
                        </div>
                    </div>
                    <div className="items-list">
                        {services.map((service) => (
                            <div key={service.id} className="settings-card">
                                <div className="card-content">
                                    <h4>{service.name}</h4>
                                    <p className="port-info">
                                        Порт: {service.port}/{(service.protocol || 'TCP').toLowerCase()}
                                        {' · '}
                                        {service.is_global
                                            ? 'Все агенты'
                                            : `Агентов: ${service.agent_ids?.length ?? 0}`}
                                    </p>
                                    <div className="service-options">
                                        {!service.is_global && (service.agent_ids ?? []).slice(0, 3).map(a => (
                                            <span key={a} className="info-badge" title={a}>{a}</span>
                                        ))}
                                        {!service.is_global && (service.agent_ids?.length ?? 0) > 3 && (
                                            <span className="info-badge">+{(service.agent_ids?.length ?? 0) - 3}</span>
                                        )}
                                        {service.chunked_transfer_encoding && <span className="option-badge">Chunked</span>}
                                        {service.urldecode && <span className="option-badge">URLdecode</span>}
                                        {service.merge_adjacent_packets && <span className="option-badge">Merge</span>}
                                        {service.inflate_websockets && <span className="option-badge">WS</span>}
                                        {service.decrypt_tls && <span className="option-badge">TLS</span>}
                                        {service.http_parse && <span className="option-badge">HTTP</span>}
                                        {service.http2_parse && <span className="option-badge">HTTP/2</span>}
                                        {(service.alert_flag_leak ?? false) && <span className="option-badge">FlagAlert</span>}
                                    </div>
                                </div>
                                <div className="card-actions">
                                    <button onClick={() => openServiceModal(service)}>✎ Изменить</button>
                                    <button onClick={() => deleteService(service.id!)}>✕ Удалить</button>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>

                {/* Filters Column */}
                <div className="settings-column">
                    <div className="column-header">
                        <h2>Фильтры</h2>
                        <button className="add-btn" onClick={() => openFilterModal()}>+ Добавить</button>
                    </div>
                    <div className="items-list">
                        {filters.map((filter) => (
                            <div key={filter.id} className="settings-card" style={{ opacity: filter.enabled ? 1 : 0.55 }}>
                                <div className="card-content">
                                    <div className="filter-header">
                                        <h4>{filter.name}</h4>
                                        <div 
                                            className="filter-color-indicator"
                                            style={{ 
                                                backgroundColor: `hsl(${filter.color}, 100%, 50%)`,
                                                color: `hsl(${filter.color}, 100%, 50%)`
                                            }}
                                        ></div>
                                    </div>
                                    <p className="pattern-text">
                                        {filter.template_type === 'ip_cidr'
                                            ? `${filter.ip_endpoint === 'dst' ? 'dst' : 'src'} ${filter.ip_network ?? ''}`
                                            : filter.template_type === 'expression'
                                                ? (filter.expression_source ?? '')
                                                : filter.template}
                                    </p>
                                    <div className="filter-info">
                                        <span className="info-badge">{filter.template_type}</span>
                                        <span className="info-badge">
                                            {filter.direction === 'in' ? 'In' : filter.direction === 'out' ? 'Out' : 'Both'}
                                        </span>
                                        <span className="option-badge">{filter.action}</span>
                                        {!filter.enabled && <span className="option-badge">Выключен</span>}
                                        <span className="info-badge" title="Ревизия конфигурации">rev {filter.revision}</span>
                                        <span className="info-badge">
                                            {filter.service_targets_global
                                                ? 'Все сервисы'
                                                : `Сервисов: ${filter.service_ids.length}`}
                                            {' / '}
                                            {filter.agent_targets_global
                                                ? 'Все агенты'
                                                : `Агентов: ${filter.agent_ids.length}`}
                                        </span>
                                        {!filter.service_targets_global && filter.service_ids.length === 0 && (
                                            <span className="option-badge" title="Явные цели пусты - фильтр отключён">Требует целей</span>
                                        )}
                                        {filter.threshold_count != null && (
                                            <span className="option-badge" title="Порог: count N в окне T секунд">
                                                {filter.threshold_count}/{filter.threshold_window_secs}s
                                            </span>
                                        )}
                                        {filter.expires_at && new Date(filter.expires_at) < new Date() && (
                                            <span className="option-badge">Истёк</span>
                                        )}
                                    </div>
                                </div>
                                <div className="card-actions">
                                    <button onClick={() => toggleFilterEnabled(filter)}>
                                        {filter.enabled ? '⏸ Выключить' : '▶ Включить'}
                                    </button>
                                    <button onClick={() => openFilterModal(filter)}>✎ Изменить</button>
                                    <button onClick={() => deleteFilter(filter.id!)}>✕ Удалить</button>
                                </div>
                            </div>
                        ))}

                        {/* Active blocks: remote ip, agent, source filter,
                            remaining duration, unban control */}
                        {activeBlocks.length > 0 && (
                            <div className="settings-card">
                                <div className="card-content">
                                    <h4>Активные блокировки ({activeBlocks.length})</h4>
                                    {activeBlocks.map((block) => (
                                        <div key={block.id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '4px 0', borderBottom: '1px solid #333', fontSize: '13px' }}>
                                            <span>
                                                <span style={{ color: '#e05555' }}>{block.remote_ip}</span>
                                                {' на '}
                                                <span style={{ color: '#888' }}>{block.agent_id}</span>
                                                {' - '}
                                                {block.filter_name}
                                                {' ('}
                                                {block.remaining_secs}s{')'}
                                            </span>
                                            <button
                                                style={{ background: 'none', border: '1px solid #555', color: '#ccc', borderRadius: '4px', cursor: 'pointer', fontSize: '12px', padding: '2px 8px' }}
                                                onClick={() => unbanBlock(block.id)}
                                            >
                                                Разблокировать
                                            </button>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}

                        {/* Recent threshold alerts/blocks (audit view) */}
                        {filterEvents.length > 0 && (
                            <div className="settings-card">
                                <div className="card-content">
                                    <h4>Пороговые события</h4>
                                    {filterEvents.map((event) => (
                                        <div key={event.id} style={{ fontSize: '12px', color: '#999', padding: '2px 0' }}>
                                            [{event.kind}] фильтр #{event.filter_id} на {event.agent_id}
                                            {event.remote_ip ? ` от ${event.remote_ip}` : ''}
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}
                    </div>
                </div>

                {/* General Settings Column */}
                <div className="settings-column">
                    <div className="column-header">
                        <h2>Общие</h2>
                    </div>
                    <div className="items-list">
                        {/* Versions — drift check, served from the backend's
                            60s-cached /api/version */}
                        <div className="settings-card">
                            <div className="card-content">
                                <h4>Версии сервисов</h4>
                                <VersionsBlock />
                            </div>
                        </div>

                        <div className="settings-card">
                            <div className="card-content">
                                <h4>Flag Farm</h4>
                                <div className="setting-row">
                                    <label className="checkbox-label">
                                        <input
                                            type="checkbox"
                                            checked={farmEnabled}
                                            onChange={(e) => {
                                                setFarmEnabled(e.target.checked);
                                                saveSetting('farm_enabled', e.target.checked ? 'true' : 'false');
                                            }}
                                        />
                                        Отправка флагов включена
                                    </label>
                                </div>
                                <input
                                    type="text"
                                    placeholder="Farm URL (http://farm:8080/submit)"
                                    value={farmUrl}
                                    onChange={(e) => setFarmUrl(e.target.value)}
                                    onBlur={() => saveSetting('farm_url', farmUrl)}
                                    style={{ width: 'calc(100% - 24px)', padding: '10px 12px', background: '#1a1a1a', border: '1px solid transparent', borderRadius: '8px', color: 'white', fontSize: '13px', marginTop: '8px' }}
                                />
                                <p style={{ color: '#888', fontSize: '12px', margin: '8px 0 0' }}>
                                    {farmEnabled
                                        ? farmUrl ? `Флаги отправляются на ${farmUrl}` : 'Укажите URL фермы'
                                        : 'Отправка выключена'}
                                </p>
                            </div>
                        </div>

                        {/* Proto Files */}
                        <div className="settings-card">
                            <div className="card-content">
                                <h4>Proto Files (gRPC)</h4>
                                {protoFiles.map(pf => {
                                    const svc = services.find(s => s.id === pf.service_id);
                                    return (
                                        <div key={pf.id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '4px 0', borderBottom: '1px solid #333' }}>
                                            <span style={{ fontSize: '13px' }}>
                                                <span style={{ color: '#888' }}>{svc?.name || `#${pf.service_id}`}</span>
                                                {' / '}
                                                <span style={{ color: '#ccc' }}>{pf.filename}</span>
                                            </span>
                                            <button style={{ background: 'none', border: 'none', color: '#888', cursor: 'pointer', fontSize: '14px', padding: '0' }} onClick={() => deleteProto(pf.id)}>✕</button>
                                        </div>
                                    );
                                })}
                                {protoFiles.length === 0 && <p style={{ color: '#666', fontSize: '12px' }}>Нет загруженных .proto файлов</p>}
                                <div style={{ marginTop: '10px', borderTop: '1px solid #333', paddingTop: '10px' }}>
                                    <select
                                        value={protoServiceId}
                                        onChange={(e) => setProtoServiceId(e.target.value)}
                                        className="select-input"
                                        style={{ marginBottom: '8px' }}
                                    >
                                        <option value="">Выберите сервис</option>
                                        {services.map(s => <option key={s.id} value={s.id}>{s.name} :{s.port}</option>)}
                                    </select>
                                    <input
                                        type="text"
                                        placeholder="Имя файла (service.proto)"
                                        value={protoFilename}
                                        onChange={(e) => setProtoFilename(e.target.value)}
                                        style={{ width: 'calc(100% - 24px)', padding: '8px 12px', background: '#1a1a1a', border: '1px solid transparent', borderRadius: '8px', color: 'white', fontSize: '13px', marginBottom: '8px' }}
                                    />
                                    <textarea
                                        placeholder="Содержимое .proto файла"
                                        value={protoContent}
                                        onChange={(e) => setProtoContent(e.target.value)}
                                        rows={4}
                                        style={{ width: 'calc(100% - 24px)', padding: '8px 12px', background: '#1a1a1a', border: '1px solid transparent', borderRadius: '8px', color: 'white', fontSize: '12px', fontFamily: 'monospace', resize: 'vertical', marginBottom: '8px' }}
                                    />
                                    <button className="add-btn" onClick={uploadProto} style={{ fontSize: '12px', padding: '6px 14px' }}>Загрузить</button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Service Modal */}
            {serviceModalOpen && (
                <div className="modal-overlay" onClick={closeServiceModal}>
                    <div className="modal-content" onClick={(e) => e.stopPropagation()}>
                        <div className="modal-header">
                            <h3>{editingService ? 'Редактировать сервис' : 'Добавить сервис'}</h3>
                            <button className="close-btn" onClick={closeServiceModal}>✕</button>
                        </div>
                        <div className="modal-body">
                            <input
                                type="text"
                                placeholder="Название сервиса"
                                value={serviceForm.name}
                                onChange={(e) => setServiceForm({ ...serviceForm, name: e.target.value })}
                            />
                            <input
                                type="number"
                                placeholder="Порт"
                                value={serviceForm.port}
                                onChange={(e) => setServiceForm({ ...serviceForm, port: parseInt(e.target.value) || 0 })}
                            />
                            <select
                                className="select-input"
                                value={serviceForm.protocol || 'TCP'}
                                onChange={(e) => setServiceForm({ ...serviceForm, protocol: e.target.value })}
                            >
                                <option value="TCP">TCP</option>
                                <option value="UDP">UDP</option>
                            </select>
                            <label className="checkbox-label">
                                <input
                                    type="checkbox"
                                    checked={serviceForm.is_global}
                                    onChange={(e) => setServiceForm({ ...serviceForm, is_global: e.target.checked })}
                                />
                                Применять ко всем агентам
                            </label>
                            {!serviceForm.is_global && (
                                <div style={{ maxHeight: '160px', overflowY: 'auto', border: '1px solid #333', borderRadius: '6px', padding: '6px', margin: '4px 0' }}>
                                    {agentsForAssignment.map(a => (
                                        <label key={a.agent_id} className="checkbox-label" title={a.online ? 'онлайн' : 'офлайн'}>
                                            <input
                                                type="checkbox"
                                                checked={serviceForm.agent_ids.includes(a.agent_id)}
                                                onChange={(e) => {
                                                    const ids = e.target.checked
                                                        ? [...serviceForm.agent_ids, a.agent_id]
                                                        : serviceForm.agent_ids.filter(id => id !== a.agent_id);
                                                    setServiceForm({ ...serviceForm, agent_ids: ids });
                                                }}
                                            />
                                            {a.agent_id}
                                            {!a.online && <span className="text-muted"> (офлайн)</span>}
                                        </label>
                                    ))}
                                    {agentsForAssignment.length === 0 && (
                                        <span className="text-muted">Нет известных агентов</span>
                                    )}
                                </div>
                            )}
                            {serviceSyncNotice && (
                                <p style={{ color: '#e8a33d', fontSize: '12px', margin: '6px 0' }}>{serviceSyncNotice}</p>
                            )}
                            <div className="checkbox-group">
                                <label className="checkbox-label">
                                    <input
                                        type="checkbox"
                                        checked={serviceForm.chunked_transfer_encoding}
                                        onChange={(e) => setServiceForm({ ...serviceForm, chunked_transfer_encoding: e.target.checked })}
                                    />
                                    Chunked Transfer Encoding
                                </label>
                                <label className="checkbox-label">
                                    <input
                                        type="checkbox"
                                        checked={serviceForm.urldecode}
                                        onChange={(e) => setServiceForm({ ...serviceForm, urldecode: e.target.checked })}
                                    />
                                    URL Decode
                                </label>
                                <label className="checkbox-label">
                                    <input
                                        type="checkbox"
                                        checked={serviceForm.merge_adjacent_packets}
                                        onChange={(e) => setServiceForm({ ...serviceForm, merge_adjacent_packets: e.target.checked })}
                                    />
                                    Merge Adjacent Packets
                                </label>
                                <label className="checkbox-label">
                                    <input
                                        type="checkbox"
                                        checked={serviceForm.inflate_websockets}
                                        onChange={(e) => setServiceForm({ ...serviceForm, inflate_websockets: e.target.checked })}
                                    />
                                    Inflate WebSockets
                                </label>
                                <label className="checkbox-label">
                                    <input
                                        type="checkbox"
                                        checked={serviceForm.decrypt_tls}
                                        onChange={(e) => setServiceForm({ ...serviceForm, decrypt_tls: e.target.checked })}
                                    />
                                    Decrypt TLS
                                </label>
                                <label className="checkbox-label">
                                    <input
                                        type="checkbox"
                                        checked={serviceForm.http_parse}
                                        onChange={(e) => setServiceForm({ ...serviceForm, http_parse: e.target.checked })}
                                    />
                                    HTTP Parse
                                </label>
                                <label className="checkbox-label">
                                    <input
                                        type="checkbox"
                                        checked={serviceForm.http2_parse}
                                        onChange={(e) => setServiceForm({ ...serviceForm, http2_parse: e.target.checked })}
                                    />
                                    HTTP/2 Parse
                                </label>
                                <label className="checkbox-label">
                                    <input
                                        type="checkbox"
                                        checked={serviceForm.alert_flag_leak ?? false}
                                        onChange={(e) => setServiceForm({ ...serviceForm, alert_flag_leak: e.target.checked })}
                                    />
                                    Flag leak alert
                                </label>
                            </div>
                        </div>
                        <div className="modal-footer">
                            <button onClick={closeServiceModal}>Отмена</button>
                            <button onClick={saveService}>Сохранить</button>
                        </div>
                    </div>
                </div>
            )}

            {/* Add Service by Container Modal */}
            {containerModalOpen && (
                <div className="modal-overlay" onClick={closeContainerModal}>
                    <div className="modal-content" onClick={(e) => e.stopPropagation()}>
                        <div className="modal-header">
                            <h3>Добавить сервис по контейнеру</h3>
                            <button className="close-btn" onClick={closeContainerModal}>✕</button>
                        </div>
                        <div className="modal-body">
                            <select
                                className="select-input"
                                value={containerAgentId}
                                onChange={(e) => setContainerAgentId(e.target.value)}
                            >
                                <option value="">Выберите агента</option>
                                {agentsList.map(a => (
                                    <option key={a.agent_id} value={a.agent_id}>
                                        {a.agent_id}{a.online ? '' : ' (офлайн)'}
                                    </option>
                                ))}
                            </select>

                            {containers === null && !containersError && (
                                <div className="containers-status">
                                    <div className="spinner" />
                                    <span>Запрашиваем список контейнеров у агента…</span>
                                </div>
                            )}
                            {containersError && (
                                <div className="containers-status error">
                                    <span>⚠ {containersError}</span>
                                    {containersDockerError && (
                                        <span className="containers-hint">Проверьте, запущен ли Docker на хосте агента.</span>
                                    )}
                                </div>
                            )}

                            {containers !== null && !containersError && containers.length === 0 && (
                                <div className="containers-status">
                                    <span>На хосте агента нет запущенных контейнеров Docker.</span>
                                </div>
                            )}

                            {containers !== null && !containersError && containers.length > 0 && (
                                <>
                                    {containersStaleAgeSec !== null && (
                                        <div className="containers-status">
                                            <span>Данные получены {containersStaleAgeSec} с назад — запрошены свежие у агента.</span>
                                        </div>
                                    )}
                                    <select
                                        className="select-input"
                                        value={selectedContainerName}
                                        onChange={(e) => pickContainer(e.target.value)}
                                    >
                                        <option value="">Выберите контейнер</option>
                                        {containers.map(c => (
                                            <option key={c.name} value={c.name}>
                                                {c.name} — {c.image} ({c.state})
                                            </option>
                                        ))}
                                    </select>
                                    {selectedContainerName && (
                                        <>
                                            <select
                                                className="select-input"
                                                value={containerPortCustom ? 'custom' : String(containerPort)}
                                                onChange={(e) => {
                                                    if (e.target.value === 'custom') {
                                                        setContainerPortCustom(true);
                                                    } else {
                                                        setContainerPortCustom(false);
                                                        setContainerPort(parseInt(e.target.value) || 0);
                                                    }
                                                }}
                                            >
                                                {[...containers.find(c => c.name === selectedContainerName)?.ports || []]
                                                    .sort((a, b) => Number(b.published) - Number(a.published))
                                                    .map(p => (
                                                        <option key={`${p.port}/${p.protocol}`} value={String(p.port)}>
                                                            {p.port}/{p.protocol}{p.published ? ' — публикуется' : ' — внутри контейнера'}
                                                        </option>
                                                    ))}
                                                <option value="custom">Другой порт…</option>
                                            </select>
                                            {containerPortCustom && (
                                                <input
                                                    type="number"
                                                    placeholder="Порт"
                                                    value={containerPort || ''}
                                                    onChange={(e) => setContainerPort(parseInt(e.target.value) || 0)}
                                                />
                                            )}
                                            <input
                                                type="text"
                                                placeholder="Название сервиса"
                                                value={containerServiceName}
                                                onChange={(e) => setContainerServiceName(e.target.value)}
                                            />
                                        </>
                                    )}
                                </>
                            )}
                        </div>
                        <div className="modal-footer">
                            <button onClick={closeContainerModal}>Отмена</button>
                            <button
                                onClick={saveContainerService}
                                disabled={creatingFromContainer || !selectedContainerName || !containerServiceName.trim() || !containerPort}
                            >
                                {creatingFromContainer ? 'Создание…' : 'Создать сервис'}
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {/* Filter Modal - unified model editor */}
            {filterModalOpen && (
                <div className="modal-overlay" onClick={closeFilterModal}>
                    <div className="modal-content" onClick={(e) => e.stopPropagation()} style={{ maxWidth: '560px' }}>
                        <div className="modal-header">
                            <h3>{editingFilter ? 'Редактировать фильтр' : 'Добавить фильтр'}</h3>
                            <button className="close-btn" onClick={closeFilterModal}>✕</button>
                        </div>
                        <div className="modal-body">
                            <input
                                type="text"
                                placeholder="Название фильтра"
                                value={filterForm.name}
                                onChange={(e) => setFilterForm({ ...filterForm, name: e.target.value })}
                            />
                            <div style={{ display: 'flex', gap: '8px' }}>
                                <select
                                    value={filterForm.template_type}
                                    onChange={(e) => {
                                        const kind = e.target.value;
                                        const next: Filter = { ...filterForm, template_type: kind };
                                        // Substring matchers prefill the sanity sample.
                                        if (kind === 'substr' || kind === 'bsubstr') {
                                            setSanitySample(next.template);
                                        }
                                        setFilterForm(next);
                                    }}
                                    className="select-input"
                                    style={{ flex: 1 }}
                                >
                                    <option value="regex">Regex</option>
                                    <option value="substr">Substring</option>
                                    <option value="bsubstr">Binary Substring (HEX)</option>
                                    <option value="ip_cidr">IP / CIDR</option>
                                    <option value="expression">Expression (CEL)</option>
                                </select>
                                <select
                                    value={filterForm.action}
                                    onChange={(e) => setFilterForm({ ...filterForm, action: e.target.value })}
                                    className="select-input"
                                    style={{ flex: 1 }}
                                >
                                    <option value="tag">Tag (метка)</option>
                                    <option value="flag">Flag (в ферму)</option>
                                    <option value="alert">Alert (уведомление)</option>
                                    <option value="block">Block (drop data)</option>
                                    <option value="block_connection">Block connection</option>
                                </select>
                            </div>

                            {/* Matcher-specific controls */}
                            {(filterForm.template_type === 'regex' || filterForm.template_type === 'substr' || filterForm.template_type === 'bsubstr') && (
                                <input
                                    type="text"
                                    placeholder={filterForm.template_type === 'bsubstr' ? 'HEX-шаблон (например deadbeef)' : 'Шаблон'}
                                    value={filterForm.template}
                                    onChange={(e) => setFilterForm({ ...filterForm, template: e.target.value })}
                                />
                            )}
                            {filterForm.template_type === 'ip_cidr' && (
                                <>
                                    <input
                                        type="text"
                                        placeholder="IP или CIDR (10.0.0.5 или 10.0.0.0/8)"
                                        value={filterForm.ip_network ?? ''}
                                        onChange={(e) => setFilterForm({ ...filterForm, ip_network: e.target.value })}
                                    />
                                    <select
                                        value={filterForm.ip_endpoint ?? 'src'}
                                        onChange={(e) => setFilterForm({ ...filterForm, ip_endpoint: e.target.value })}
                                        className="select-input"
                                    >
                                        <option value="src">src - адрес источника пакета</option>
                                        <option value="dst">dst - адрес получателя пакета</option>
                                    </select>
                                </>
                            )}
                            {filterForm.template_type === 'expression' && (
                                <textarea
                                    placeholder={'CEL-выражение, напр.:\nprotocol == "TCP" && ip_in_cidr(remote_ip, "10.0.0.0/8") && payload.contains(b"flag{")'}
                                    value={filterForm.expression_source ?? ''}
                                    onChange={(e) => setFilterForm({ ...filterForm, expression_source: e.target.value })}
                                    rows={3}
                                    style={{ fontFamily: 'monospace', fontSize: '12px', width: 'calc(100% - 24px)', padding: '8px 12px', background: '#1a1a1a', border: '1px solid transparent', borderRadius: '8px', color: 'white', resize: 'vertical' }}
                                />
                            )}
                            <div style={{ display: 'flex', gap: '8px' }}>
                                <select
                                    value={filterForm.direction}
                                    onChange={(e) => setFilterForm({ ...filterForm, direction: e.target.value })}
                                    className="select-input"
                                    style={{ flex: 1 }}
                                >
                                    <option value="in">Входящие</option>
                                    <option value="out">Исходящие</option>
                                    <option value="both">Оба направления</option>
                                </select>
                                <label className="checkbox-label" style={{ flex: 1 }}>
                                    <input
                                        type="checkbox"
                                        checked={filterForm.enabled}
                                        onChange={(e) => setFilterForm({ ...filterForm, enabled: e.target.checked })}
                                    />
                                    Включён
                                </label>
                            </div>

                            {/* Independent service / agent targets */}
                            <div style={{ borderTop: '1px solid #333', marginTop: '6px', paddingTop: '6px' }}>
                                <label className="checkbox-label">
                                    <input
                                        type="checkbox"
                                        checked={filterForm.service_targets_global}
                                        onChange={(e) => setFilterForm({ ...filterForm, service_targets_global: e.target.checked })}
                                    />
                                    Все сервисы (вкл. будущие)
                                </label>
                                {!filterForm.service_targets_global && (
                                    <select
                                        multiple
                                        className="select-input"
                                        value={filterForm.service_ids.map(String)}
                                        onChange={(e) => {
                                            const ids = Array.from(e.target.selectedOptions, (o) => Number(o.value));
                                            setFilterForm({ ...filterForm, service_ids: ids });
                                        }}
                                        style={{ height: '100px', width: '100%' }}
                                    >
                                        {services.map((svc) => (
                                            <option key={svc.id} value={String(svc.id)}>
                                                {svc.name} :{svc.port}/{(svc.protocol || 'TCP').toLowerCase()}
                                            </option>
                                        ))}
                                    </select>
                                )}
                                <label className="checkbox-label">
                                    <input
                                        type="checkbox"
                                        checked={filterForm.agent_targets_global}
                                        onChange={(e) => setFilterForm({ ...filterForm, agent_targets_global: e.target.checked })}
                                    />
                                    Все агенты (вкл. будущие)
                                </label>
                                {!filterForm.agent_targets_global && (
                                    <div style={{ maxHeight: '120px', overflowY: 'auto', border: '1px solid #333', borderRadius: '6px', padding: '6px', margin: '4px 0' }}>
                                        {agentsForAssignment.map((a) => (
                                            <label key={a.agent_id} className="checkbox-label">
                                                <input
                                                    type="checkbox"
                                                    checked={filterForm.agent_ids.includes(a.agent_id)}
                                                    onChange={(e) => {
                                                        const ids = e.target.checked
                                                            ? [...filterForm.agent_ids, a.agent_id]
                                                            : filterForm.agent_ids.filter((id) => id !== a.agent_id);
                                                        setFilterForm({ ...filterForm, agent_ids: ids });
                                                    }}
                                                />
                                                {a.agent_id}
                                            </label>
                                        ))}
                                    </div>
                                )}
                            </div>

                            {/* Threshold controls only for alert/block */}
                            {(filterForm.action === 'alert' || filterForm.action === 'block') && (
                                <div style={{ borderTop: '1px solid #333', marginTop: '6px', paddingTop: '6px' }}>
                                    <label className="checkbox-label">
                                        <input
                                            type="checkbox"
                                            checked={filterForm.threshold_count != null}
                                            onChange={(e) =>
                                                setFilterForm(e.target.checked
                                                    ? { ...filterForm, threshold_count: 5, threshold_window_secs: 10, threshold_group_by: 'remote_ip', threshold_cooldown_secs: 30, threshold_block_secs: 60 }
                                                    : { ...filterForm, threshold_count: null, threshold_window_secs: null, threshold_group_by: null, threshold_cooldown_secs: null, threshold_block_secs: null })
                                            }
                                        />
                                        Порог: сработать после N совпадений за окно
                                    </label>
                                    {filterForm.threshold_count != null && (
                                        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                                            <input type="number" min="1" placeholder="N" value={filterForm.threshold_count ?? ''} onChange={(e) => setFilterForm({ ...filterForm, threshold_count: parseInt(e.target.value) || null })} style={{ width: '70px' }} />
                                            <input type="number" min="1" placeholder="окно, сек" value={filterForm.threshold_window_secs ?? ''} onChange={(e) => setFilterForm({ ...filterForm, threshold_window_secs: parseInt(e.target.value) || null })} style={{ width: '90px' }} />
                                            <input type="number" min="0" placeholder="cooldown, сек" value={filterForm.threshold_cooldown_secs ?? ''} onChange={(e) => setFilterForm({ ...filterForm, threshold_cooldown_secs: parseInt(e.target.value) || 0 })} style={{ width: '110px' }} />
                                            <select
                                                className="select-input"
                                                value={filterForm.threshold_group_by ?? 'remote_ip'}
                                                onChange={(e) => setFilterForm({ ...filterForm, threshold_group_by: e.target.value })}
                                                style={{ flex: 1 }}
                                            >
                                                <option value="remote_ip">по remote IP</option>
                                                <option value="stream">по потоку</option>
                                                <option value="service">по сервису</option>
                                            </select>
                                            {filterForm.action === 'block' && (
                                                <input type="number" min="1" placeholder="блок, сек" value={filterForm.threshold_block_secs ?? ''} onChange={(e) => setFilterForm({ ...filterForm, threshold_block_secs: parseInt(e.target.value) || null })} style={{ width: '90px' }} />
                                            )}
                                        </div>
                                    )}
                                </div>
                            )}

                            {/* Expiry */}
                            <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                                <label className="text-muted" style={{ fontSize: '12px' }}>Истекает:</label>
                                <input
                                    type="datetime-local"
                                    value={filterForm.expires_at ? filterForm.expires_at.slice(0, 16) : ''}
                                    onChange={(e) => setFilterForm({ ...filterForm, expires_at: e.target.value || null })}
                                    style={{ flex: 1, background: '#1a1a1a', border: '1px solid #333', borderRadius: '6px', color: 'white', padding: '6px' }}
                                />
                            </div>

                            <div className="color-picker-wrapper">
                                <label>Цвет (Hue): {filterForm.color}°</label>
                                <input
                                    type="range"
                                    min="0"
                                    max="360"
                                    value={filterForm.color}
                                    onChange={(e) => setFilterForm({ ...filterForm, color: parseInt(e.target.value) })}
                                    className="color-slider"
                                />
                                <div
                                    className="color-preview"
                                    style={{
                                        backgroundColor: `hsl(${filterForm.color}, 100%, 50%)`,
                                        color: `hsl(${filterForm.color}, 100%, 50%)`
                                    }}
                                ></div>
                            </div>

                            {/* Sanity check */}
                            <div style={{ borderTop: '1px solid #333', marginTop: '6px', paddingTop: '6px' }}>
                                <h4 style={{ margin: '4px 0' }}>Проверка (sanity check)</h4>
                                {filterInspectsPayload({ id: editingFilter?.id, template_type: filterForm.template_type, template: filterForm.template }) &&
                                    (filterForm.template_type !== 'expression' || expressionUsesPayload(filterForm.expression_source ?? '')) ? (
                                    <>
                                        <p className="text-muted" style={{ fontSize: '11px', margin: '2px 0' }}>
                                            Подставьте тестовый payload: он уходит curl-ом на выбранный сервис (команда только отображается - запускайте с подходящего хоста). Уникальный nonce добавляется автоматически.
                                        </p>
                                        <textarea
                                            placeholder="Тестовый payload (префиксован шаблоном для substr)"
                                            value={sanitySample}
                                            onChange={(e) => setSanitySample(e.target.value)}
                                            rows={2}
                                            style={{ fontFamily: 'monospace', fontSize: '12px', width: 'calc(100% - 24px)', padding: '8px 12px', background: '#1a1a1a', border: '1px solid #333', borderRadius: '8px', color: 'white', resize: 'vertical' }}
                                        />
                                        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap', margin: '4px 0' }}>
                                            <select className="select-input" value={sanityAgentId} onChange={(e) => setSanityAgentId(e.target.value)}>
                                                <option value="">Агент...</option>
                                                {agentsForAssignment.map((a) => (
                                                    <option key={a.agent_id} value={a.agent_id}>{a.agent_id}</option>
                                                ))}
                                            </select>
                                            <select className="select-input" value={sanityServiceId} onChange={(e) => setSanityServiceId(e.target.value)}>
                                                <option value="">HTTP-сервис...</option>
                                                {services.map((svc) => (
                                                    <option key={svc.id} value={String(svc.id)}>{svc.name} :{svc.port}</option>
                                                ))}
                                            </select>
                                            <input
                                                type="text"
                                                placeholder="host сервиса (например 10.10.2.4)"
                                                value={sanityHost}
                                                onChange={(e) => setSanityHost(e.target.value)}
                                                style={{ flex: 1, minWidth: '160px', background: '#1a1a1a', border: '1px solid #333', borderRadius: '6px', color: 'white', padding: '6px' }}
                                            />
                                        </div>
                                        {editingFilter?.id && (
                                            <button className="add-btn" onClick={generateSanity} style={{ fontSize: '12px', padding: '6px 14px' }}>
                                                Сгенерировать curl
                                            </button>
                                        )}
                                        {!editingFilter?.id && (
                                            <p className="text-muted" style={{ fontSize: '11px' }}>Сохраните фильтр, затем сгенерируйте проверку.</p>
                                        )}
                                        {sanityResult && (
                                            <div style={{ marginTop: '6px' }}>
                                                <code style={{ display: 'block', background: '#111', padding: '8px', borderRadius: '6px', fontSize: '11px', wordBreak: 'break-all', whiteSpace: 'pre-wrap' }}>
                                                    {sanityResult.curl}
                                                </code>
                                                <div style={{ display: 'flex', gap: '6px', marginTop: '4px' }}>
                                                    <button onClick={() => copyText(sanityResult.curl)} className="add-btn" style={{ fontSize: '11px', padding: '4px 10px' }}>Копировать</button>
                                                    <a
                                                        href={sanityResult.resultLink}
                                                        target="_blank"
                                                        rel="noreferrer"
                                                        className="add-btn"
                                                        style={{ fontSize: '11px', padding: '4px 10px', textDecoration: 'none' }}
                                                    >
                                                        Результат (nonce {sanityResult.nonce})
                                                    </a>
                                                </div>
                                                {(filterForm.action === 'block' || filterForm.action === 'block_connection') && (
                                                    <p className="text-muted" style={{ fontSize: '11px' }}>
                                                        Внимание: block enforcement работает на прокси агента и не виден в пассивном replay-трафике, если сервис не за прокси.
                                                    </p>
                                                )}
                                            </div>
                                        )}
                                    </>
                                ) : (
                                    streamsScope && (
                                        <div>
                                            <p className="text-muted" style={{ fontSize: '11px' }}>{streamsScope.explanation}</p>
                                            {editingFilter?.id && (
                                                <a
                                                    href={streamsScope.link}
                                                    target="_blank"
                                                    rel="noreferrer"
                                                    className="add-btn"
                                                    style={{ fontSize: '11px', padding: '4px 10px', textDecoration: 'none', display: 'inline-block' }}
                                                >
                                                    Открыть совпадающие потоки
                                                </a>
                                            )}
                                        </div>
                                    )
                                )}
                            </div>

                            {filterError && (
                                <p style={{ color: '#e8a33d', fontSize: '12px', margin: '6px 0' }}>{filterError}</p>
                            )}
                        </div>
                        <div className="modal-footer">
                            <button onClick={closeFilterModal}>Отмена</button>
                            <button onClick={saveFilter}>Сохранить</button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}