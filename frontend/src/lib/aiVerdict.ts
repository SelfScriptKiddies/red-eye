// JEV (AI stream analysis) frontend helpers. Mirrors
// backend/src/api/ai/: verdict DTO shape, the WebSocket event contract and
// the /api/ai/status response. All provider-produced text is rendered as
// text by React default escaping - never HTML.

import { apiFetch } from './api';
import { ApiError, RetryableError } from './queryClient';
import { type SettingRow } from './settingRows';

export interface AiAnswers {
    nouls: Record<string, number>;
    attack_class: { choice: string; confidence: number };
    severity: { score: number; confidence: number };
}

/** Mirrors AiVerdictRow in backend/src/api/ai/repository.rs */
export interface AiVerdict {
    id: number;
    stream_id: number;
    first_packet_id: number;
    last_packet_id: number;
    model: string;
    prompt_version: number;
    transcript_sha256: string;
    attack_class: string;
    confidence: number;
    severity: number;
    is_attack: boolean;
    ai_threshold: number;
    answers: AiAnswers;
    input_tokens?: number | null;
    cost?: number | null;
    analyzed_packet_count: number;
    omitted_packet_count: number;
    omitted_byte_count: number;
    truncated: boolean;
    created_at_unix: number;
}

/** Mirrors the GET /api/ai/status response */
export interface AiStatus {
    enabled: boolean;
    configured: boolean;
    run_state: string;
    model: string;
    threshold: number;
    in_flight: number;
    backlog: number;
    oldest_candidate_age_secs: number;
    cost_spent_today: number;
    cost_limit: number;
    verdicts_total: number;
    attack_verdicts_total: number;
    calls_total: number;
    calls_failed: number;
    invalid_responses: number;
    last_success_unix?: number | null;
}

/** WebSocket event: every successful verdict, attack or not. */
export interface AiVerdictEvent {
    type: 'ai_verdict';
    verdict_id: number;
    stream_id: number;
    last_packet_id: number;
    is_attack: boolean;
    attack_class: string;
    confidence: number;
    severity: number;
    top_nouls: Record<string, number>;
}

export function isAiVerdictEvent(data: unknown): data is AiVerdictEvent {
    return (
        typeof data === 'object' &&
        data !== null &&
        (data as { type?: unknown }).type === 'ai_verdict'
    );
}

export function fetchAiVerdicts(
    streamId?: number,
    onlyAttacks?: boolean,
    limit?: number,
    cursor?: number,
    signal?: AbortSignal
): Promise<AiVerdict[]> {
    const params = new URLSearchParams();
    if (streamId !== undefined) params.set('stream_id', String(streamId));
    if (onlyAttacks) params.set('only_attacks', 'true');
    if (limit !== undefined) params.set('limit', String(limit));
    if (cursor !== undefined) params.set('cursor', String(cursor));
    return apiFetch<AiVerdict[]>(`/api/ai/verdicts?${params.toString()}`, signal);
}

export function fetchAiVerdict(id: number, signal?: AbortSignal): Promise<AiVerdict> {
    return apiFetch<AiVerdict>(`/api/ai/verdict/${id}`, signal);
}

export function fetchAiStatus(signal?: AbortSignal): Promise<AiStatus> {
    return apiFetch<AiStatus>('/api/ai/status', signal);
}

export async function putAiSetting(key: string, value: string): Promise<void> {
    const response = await fetch(`/api/settings/${key}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value }),
    });
    if (response.status === 503) {
        throw new RetryableError(503, 5000);
    }
    if (!response.ok) {
        throw new ApiError(response.status);
    }
}

// The API key stays out of this list: it has its own write-only field.
export const AI_SETTING_ROWS: SettingRow[] = [
    { key: 'ai_model', label: 'Модель', hint: 'Pinned-модель или алиас Decisions API', type: 'text', required: true },
    { key: 'ai_threshold', label: 'Порог атаки', hint: '0..1: вердикт is_attack при max(noul) >= порога или non-benign класс с confidence >= порога', type: 'float' },
    { key: 'ai_max_per_tick', label: 'Стримов за тик', hint: '1..100, тик 5 секунд', type: 'int', advanced: true },
    { key: 'ai_stream_debounce_ms', label: 'Пауза перед анализом, мс', hint: 'мс неактивности перед анализом активного стрима', type: 'int', advanced: true },
    { key: 'ai_stream_min_interval_secs', label: 'Интервал повтора, с', hint: 'сек между попытками одного стрима (закрытые - исключение)', type: 'int', advanced: true },
    { key: 'ai_stream_max_bytes', label: 'Лимит транскрипта', hint: 'байты на весь транскрипт (голова 16 КиБ + хвост)', type: 'int', bytes: true, advanced: true },
    { key: 'ai_stream_max_entries', label: 'Записей в транскрипте', hint: 'максимум пакетов в транскрипте', type: 'int', advanced: true },
    { key: 'ai_entry_max_bytes', label: 'Размер записи', hint: 'байты на одну запись (8 КиБ по умолчанию)', type: 'int', bytes: true, advanced: true },
    { key: 'ai_daily_cost_limit', label: 'Лимит расходов в день, $', hint: 'USD по UTC; при достижении новые вызовы приостанавливаются до следующего дня', type: 'float', advanced: true },
    { key: 'ai_verdict_retention_days', label: 'Хранение вердиктов, дн', hint: 'дней; старые вердикты удаляются ограниченными пачками', type: 'int', advanced: true },
];

export interface AiModelEntry {
    id: string;
    context_length?: number | null;
}

export interface AiModelsResponse {
    models: AiModelEntry[];
    note?: string;
}

export function fetchAiModels(signal?: AbortSignal): Promise<AiModelsResponse> {
    return apiFetch<AiModelsResponse>('/api/ai/models', signal);
}

/** Datalist view: dedupe, prefer larger declared context, cap for usability. */
export function filterAiModels(models: AiModelEntry[], cap = 200): AiModelEntry[] {
    const seen = new Set<string>();
    return models
        .filter(m => {
            if (!m.id || seen.has(m.id)) return false;
            seen.add(m.id);
            return true;
        })
        .sort((a, b) => (b.context_length ?? 0) - (a.context_length ?? 0))
        .slice(0, cap);
}

/** Badge color class per attack class; unknown classes still render as text. */
export function verdictBadgeClass(attackClass: string): string {
    const known = [
        'sqli', 'xss', 'command_injection', 'path_traversal', 'scanner',
        'bruteforce', 'exfil', 'exploit_attempt', 'benign', 'other',
    ];
    return known.includes(attackClass) ? `jev-badge-${attackClass}` : 'jev-badge-other';
}

const SEVERITY_LABELS = ['benign', 'suspicious', 'malicious', 'active exploit'];

export function severityLabel(score: number): string {
    const idx = Math.min(SEVERITY_LABELS.length - 1, Math.max(0, Math.round(score)));
    return SEVERITY_LABELS[idx];
}

/** Top-N noul probabilities, descending, name-ascending tiebreak. */
export function topNouls(nouls: Record<string, number>, n = 3): [string, number][] {
    return Object.entries(nouls)
        .sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
        .slice(0, n);
}

/** RU-formatted analysis time from the unix created_at. */
export function formatVerdictTime(unix: number): string {
    if (!unix) return '';
    return new Date(unix * 1000).toLocaleTimeString();
}
