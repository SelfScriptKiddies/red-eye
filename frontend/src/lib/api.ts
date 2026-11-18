import { ApiError, RetryableError } from './queryClient';

export async function apiFetch<T>(url: string, signal?: AbortSignal): Promise<T> {
    const response = await fetch(url, { signal });
    if (response.status === 503) {
        const retryAfter = response.headers.get('Retry-After');
        throw new RetryableError(503, retryAfter ? parseInt(retryAfter) * 1000 : 5000);
    }
    if (!response.ok) {
        throw new ApiError(response.status);
    }
    return response.json();
}

// ── Agent Docker containers (UI "add service by container") ──

/** Mirrors AgentContainerPort in backend/src/api/agent/agent_dto.rs */
export interface AgentContainerPort {
    port: number;
    protocol: string;
    published: boolean;
}

/** Mirrors AgentContainerInfo in backend/src/api/agent/agent_dto.rs */
export interface AgentContainerInfo {
    name: string;
    image: string;
    state: string;
    ports: AgentContainerPort[];
}

/** Mirrors AgentContainersResponse in backend/src/api/agent/agent_dto.rs */
export interface AgentContainersResponse {
    agent_id: string;
    /** ok | error | pending | stale — see the endpoint handler contract */
    state: 'ok' | 'error' | 'pending' | 'stale';
    containers: AgentContainerInfo[];
    error?: string | null;
    age_ms?: number | null;
}

/** Mirrors AgentInfo in backend/src/api/agent/agent_dto.rs (subset used by pickers) */
export interface AgentListItem {
    agent_id: string;
    online: boolean;
}

/**
 * Fetch the Docker containers of an agent host. When the backend answers
 * `state === 'pending'` the agent reply has not arrived yet — re-poll
 * once or twice (~1s apart) instead of looping.
 */
export async function fetchAgentContainers(
    agentId: string,
    signal?: AbortSignal
): Promise<AgentContainersResponse> {
    return apiFetch<AgentContainersResponse>(`/api/agent/${encodeURIComponent(agentId)}/containers`, signal);
}

export function copyText(text: string) {
    if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(text);
    } else {
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
    }
}

// Starred-stream retention (favorites)

/** Mirrors FavoriteStateResponse in backend/src/api/favorite/favorite_dto.rs */
export interface FavoriteState {
    id: number;
    favorite: boolean;
    favorite_revision: number;
    retention_state: string;
    segment_index: number;
    retention_bytes: number;
    archived_through_packet_id: number;
    archive_error?: string | null;
}

/** Mirrors FavoriteUsageResponse in backend/src/api/favorite/favorite_dto.rs */
export interface FavoriteUsage {
    max_total_bytes: number;
    segment_bytes: number;
    disk_reserve_bytes: number;
    saved_bytes: number;
    reserved_bytes: number;
    starred_streams: number;
    active_starred_streams: number;
    pending_cleanup_streams: number;
}

/** 409 favorite_capacity_exceeded: no retention guarantee was created. */
export class FavoriteCapacityError extends Error {
    readonly reservedBytes: number;
    readonly maxTotalBytes: number;
    readonly requiredReservation: number;

    constructor(body: any) {
        super('favorite_capacity_exceeded');
        this.reservedBytes = Number(body?.reserved_bytes ?? 0);
        this.maxTotalBytes = Number(body?.max_total_bytes ?? 0);
        this.requiredReservation = Number(body?.required_reservation ?? 0);
    }
}

/**
 * Server-confirmed star/unstar. The caller must not optimistically show a
 * successful star: a 409 rejection means the stream stays unstarred.
 */
export async function setStreamFavorite(
    streamId: number,
    favorite: boolean
): Promise<FavoriteState> {
    const response = await fetch(`/api/stream/${streamId}/favorite`, {
        method: favorite ? 'POST' : 'DELETE',
    });
    const body = await response.json().catch(() => null);
    if (response.status === 409) throw new FavoriteCapacityError(body);
    if (!response.ok) throw new ApiError(response.status);
    return body as FavoriteState;
}

export function fetchFavoriteUsage(signal?: AbortSignal): Promise<FavoriteUsage> {
    return apiFetch<FavoriteUsage>('/api/favorite/usage', signal);
}

// HTTP User-Agent listing (bounded aggregate buckets)

/** Mirrors UserAgentItem in backend/src/api/l7/user_agent_dto.rs */
export interface UserAgentItem {
    /** Exact value; null for the overflow summary entry. */
    user_agent: string | null;
    message_count: number;
    first_seen: number;
    last_seen: number;
    agent_ids: string[];
    is_overflow: boolean;
}

/** Mirrors UserAgentsResponse in backend/src/api/l7/user_agent_dto.rs */
export interface UserAgentsResponse {
    items: UserAgentItem[];
    next_cursor: string | null;
    has_overflow: boolean;
    /** Unix seconds of the first aggregate write; null before it started. */
    aggregation_started: number | null;
}

export type UserAgentsSort = 'count' | 'last_seen';
export type UserAgentsOrder = 'desc' | 'asc';

export interface UserAgentsQuery {
    agentId?: string;
    limit?: number;
    cursor?: string | null;
    sort?: UserAgentsSort;
    order?: UserAgentsOrder;
}

export function fetchUserAgents(
    serviceId: number,
    params: UserAgentsQuery,
    signal?: AbortSignal
): Promise<UserAgentsResponse> {
    const q = new URLSearchParams();
    if (params.agentId) q.set('agent_id', params.agentId);
    q.set('limit', String(params.limit ?? 100));
    if (params.cursor) q.set('cursor', params.cursor);
    if (params.sort) q.set('sort', params.sort);
    if (params.order) q.set('order', params.order);
    return apiFetch<UserAgentsResponse>(
        `/api/http/service/${serviceId}/user-agents?${q.toString()}`,
        signal
    );
}
