// LLM-SOC triage frontend helpers. Mirrors backend/src/api/soc/ DTOs, the
// WebSocket event contracts and GET /api/soc/status. All model- and
// operator-produced text renders as React text nodes (default escaping,
// never dangerouslySetInnerHTML).

import { apiFetch } from './api';
import { ApiError, RetryableError } from './queryClient';

/** Mirrors SocResultRow in backend/src/api/soc/repository.rs */
export interface SocResult {
    id: number;
    job_id: number;
    stream_id: number;
    stream_revision: number;
    verdict: 'benign' | 'suspicious' | 'likely_attack' | 'unknown';
    confidence: number;
    severity: number;
    summary: string;
    evidence: SocEvidenceItem[];
    indicators: string[];
    recommended_action: string;
    model: string;
    prompt_version: number;
    input_tokens?: number | null;
    output_tokens?: number | null;
    duration_ms?: number | null;
    created_at_unix: number;
}

export interface SocEvidenceItem {
    packet_id?: number;
    message_id?: number;
    explanation: string;
}

/** Mirrors the matrix Availability block in backend/src/api/soc/matrix.rs */
export interface SocAvailability {
    available: boolean;
    reason: string;
}

export interface SocMatrixState {
    jev_state: string;
    soc_state: string;
    matrix_row: string;
    manual: SocAvailability;
    auto: SocAvailability;
}

/** Mirrors GET /api/soc/status */
export interface SocStatus {
    enabled: boolean;
    auto_enabled: boolean;
    model: string;
    base_url: string;
    api_key_configured: boolean;
    worker_token_configured: boolean;
    mcp_service_token_configured: boolean;
    mcp_service_capability: string;
    alert_threshold: number;
    auto_min_jev_severity: number;
    limits: Record<string, number>;
    prompt_version: number;
    settings_generation: number;
    matrix: SocMatrixState;
    claims_paused_reason?: string | null;
    queue: { depth: number; oldest_wait_secs: number; active_leases: number; jobs_used_today: number };
    worker_last_seen_unix?: number | null;
    mcp_last_seen_unix?: number | null;
    recent_errors: { job_id: number; error: string; at_unix: number }[];
}

export function fetchSocStatus(signal?: AbortSignal): Promise<SocStatus> {
    return apiFetch<SocStatus>('/api/soc/status', signal);
}

export function fetchSocResults(
    streamId?: number,
    limit?: number,
    cursor?: number,
    signal?: AbortSignal
): Promise<SocResult[]> {
    const params = new URLSearchParams();
    if (streamId !== undefined) params.set('stream_id', String(streamId));
    if (limit !== undefined) params.set('limit', String(limit));
    if (cursor !== undefined) params.set('cursor', String(cursor));
    return apiFetch<SocResult[]>(`/api/soc/results?${params.toString()}`, signal);
}

export function fetchSocResult(id: number, signal?: AbortSignal): Promise<SocResult> {
    return apiFetch<SocResult>(`/api/soc/result/${id}`, signal);
}

/** Manual triage. Returns the queued (or deduplicated) job id. A 503
 * carries Retry-After (queue at capacity), a 409 means SOC disabled or
 * the stream has no analyzable revision. */
export async function submitSocTriage(
    streamId: number,
    fresh: boolean
): Promise<{ job_id: number; deduplicated: boolean }> {
    const response = await fetch(
        `/api/soc/triage${fresh ? '?fresh=true' : ''}`,
        {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ stream_id: streamId }),
        }
    );
    if (response.status === 503) {
        const retryAfter = response.headers.get('Retry-After');
        throw new RetryableError(503, retryAfter ? parseInt(retryAfter) * 1000 : 30000);
    }
    const body = await response.json().catch(() => null);
    if (response.status === 409) {
        throw new Error(body?.error ?? 'SOC disabled or stream has no revision');
    }
    if (response.status === 404) {
        throw new Error('stream not found');
    }
    if (!response.ok) throw new ApiError(response.status);
    return body as { job_id: number; deduplicated: boolean };
}

export async function putSocSetting(key: string, value: string): Promise<void> {
    const response = await fetch(`/api/settings/${key}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value }),
    });
    if (response.status === 503) throw new RetryableError(503, 5000);
    if (!response.ok) {
        const body = await response.text().catch(() => '');
        throw new Error(body.slice(0, 200) || `HTTP ${response.status}`);
    }
}

/** One-time credential generation. rotate=true replaces the stored value
 * and invalidates the previous one. */
export async function generateSocCredential(
    kind: 'worker-token' | 'mcp-service-token',
    rotate: boolean
): Promise<string> {
    const response = await fetch(`/api/soc/${kind}/generate${rotate ? '?rotate=true' : ''}`, {
        method: 'POST',
    });
    const body = await response.json().catch(() => null);
    if (!response.ok) {
        throw new Error(body?.error ?? `HTTP ${response.status}`);
    }
    return body.token as string;
}

export interface McpToken {
    id: number;
    name: string;
    capability: string;
    expires_at_unix?: number | null;
    revoked: boolean;
}

export function fetchMcpTokens(signal?: AbortSignal): Promise<McpToken[]> {
    return apiFetch<McpToken[]>('/api/mcp/tokens', signal);
}

export async function createMcpToken(
    name: string,
    capability: string,
    expiresInSecs?: number
): Promise<{ id: number; token: string }> {
    const response = await fetch('/api/mcp/tokens', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            name,
            capability,
            expires_in_secs: expiresInSecs && expiresInSecs > 0 ? expiresInSecs : undefined,
        }),
    });
    const body = await response.json().catch(() => null);
    if (!response.ok) throw new Error(body?.error ?? `HTTP ${response.status}`);
    return body as { id: number; token: string };
}

export async function revokeMcpToken(id: number): Promise<void> {
    const response = await fetch(`/api/mcp/tokens/${id}`, { method: 'DELETE' });
    if (!response.ok) throw new ApiError(response.status);
}

/** The operator-configurable SOC settings with UI metadata. Secrets are
 * absent: soc_api_key has its own write-only field, generated credentials
 * have their own buttons. */
export const SOC_SETTING_ROWS: { key: string; label: string; hint: string; type?: 'float' | 'int' | 'text' }[] = [
    { key: 'soc_base_url', label: 'LLM endpoint', hint: 'OpenAI-совместимый /v1, например http://llm:8080/v1', type: 'text' },
    { key: 'soc_model', label: 'Модель', hint: 'Имя модели на endpoint', type: 'text' },
    { key: 'soc_alert_threshold', label: 'Порог алерта', hint: '0..1: likely_attack с confidence >= порога шлёт soc_triage тост', type: 'float' },
    { key: 'soc_auto_min_jev_severity', label: 'Мин. тяжесть JEV для авто-триажа', hint: '0..3; новые qualifying вердикты ставят ровно одну задачу', type: 'int' },
    { key: 'soc_max_context_tokens', label: 'Лимит контекста', hint: 'токены на запрос модели', type: 'int' },
    { key: 'soc_max_output_tokens', label: 'Лимит вывода', hint: 'токены на ответ модели', type: 'int' },
    { key: 'soc_max_tool_calls', label: 'Лимит tool-вызовов', hint: 'максимум на одну задачу', type: 'int' },
    { key: 'soc_max_tool_result_bytes', label: 'Лимит результата tool', hint: 'байты на один результат', type: 'int' },
    { key: 'soc_job_timeout_secs', label: 'Таймаут задачи', hint: 'сек; lease и бюджет попытки', type: 'int' },
    { key: 'soc_max_attempts', label: 'Максимум попыток', hint: 'после исчерпания задача помечается failed (это не benign)', type: 'int' },
    { key: 'soc_concurrency', label: 'Параллелизм воркера', hint: 'число одновременных задач (по умолчанию 1)', type: 'int' },
    { key: 'soc_daily_job_limit', label: 'Дневной лимит задач', hint: 'по UTC; при достижении claims пауза до следующего дня', type: 'int' },
];

/** True when the endpoint points outside localhost / private networks:
 * the privacy acknowledgement gate keys off this. */
export function isExternalLlmEndpoint(base_url: string): boolean {
    let host = '';
    try {
        host = new URL(base_url).hostname;
    } catch {
        return true;
    }
    if (host === 'localhost' || host === '127.0.0.1' || host === '::1' || host === '[::1]') {
        return false;
    }
    // Private IPv4 ranges and bare hostnames (compose services).
    if (/^10\./.test(host) || /^192\.168\./.test(host)) return false;
    if (/^172\.(1[6-9]|2\d|3[01])\./.test(host)) return false;
    if (!host.includes('.') && !host.includes(':') && host.length > 0) {
        // Bare hostname: a compose service on the internal network.
        return false;
    }
    // Dotted public host, IP literal or empty/opaque hostname: external
    // (fail closed for anything unparsable).
    return true;
}

/** Verdict badge class for the SOC panel; unknown classes stay text. */
export function socVerdictClass(verdict: string): string {
    const known = ['benign', 'suspicious', 'likely_attack', 'unknown'];
    return known.includes(verdict) ? `soc-verdict-${verdict}` : 'soc-verdict-unknown';
}

/** WebSocket event: every completed result (state refresh, no toast). */
export interface SocResultEvent {
    type: 'soc_result';
    job_id: number;
    result_id: number;
    stream_id: number;
    stream_revision: number;
    verdict: string;
    confidence: number;
    severity: number;
}

export function isSocResultEvent(data: unknown): data is SocResultEvent {
    return (
        typeof data === 'object' && data !== null &&
        (data as { type?: unknown }).type === 'soc_result'
    );
}

/** WebSocket alert event: likely_attack above the threshold only. */
export interface SocTriageEvent {
    type: 'soc_triage';
    job_id: number;
    result_id: number;
    stream_id: number;
    stream_revision: number;
    verdict: string;
    confidence: number;
    severity: number;
    summary: string;
}

export function isSocTriageEvent(data: unknown): data is SocTriageEvent {
    return (
        typeof data === 'object' && data !== null &&
        (data as { type?: unknown }).type === 'soc_triage'
    );
}

export function formatSocTime(unix: number): string {
    if (!unix) return '';
    return new Date(unix * 1000).toLocaleTimeString();
}
