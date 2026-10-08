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
