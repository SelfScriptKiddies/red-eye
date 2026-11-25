import { describe, it, expect } from 'vitest';
import { formatMoney, formatSeen, jevState, socState } from './aiStatus';
import type { AiStatus } from './aiVerdict';
import type { SocStatus } from './socTriage';

const jev = (over: Partial<AiStatus> = {}): AiStatus => ({
    enabled: false,
    configured: false,
    run_state: 'disabled',
    model: 'typesafe/jev-1.13',
    threshold: 0.7,
    in_flight: 0,
    backlog: 0,
    oldest_candidate_age_secs: 0,
    cost_spent_today: 0,
    cost_limit: 5,
    verdicts_total: 0,
    attack_verdicts_total: 0,
    calls_total: 0,
    calls_failed: 0,
    invalid_responses: 0,
    ...over,
});

const available = { available: true, reason: '' };

const soc = (over: Partial<SocStatus> = {}): SocStatus => ({
    enabled: false,
    auto_enabled: false,
    model: 'qwen',
    base_url: 'http://llm:8080/v1',
    api_key_configured: false,
    worker_token_configured: false,
    mcp_service_token_configured: false,
    mcp_service_capability: 'read_only',
    alert_threshold: 0.7,
    auto_min_jev_severity: 2,
    limits: { daily_job_limit: 500 },
    prompt_version: 1,
    settings_generation: 1,
    matrix: { jev_state: '', soc_state: '', matrix_row: '', manual: available, auto: available },
    queue: { depth: 0, oldest_wait_secs: 0, active_leases: 0, jobs_used_today: 0 },
    recent_errors: [],
    ...over,
});

describe('jevState', () => {
    it('asks for the key when nothing is configured', () => {
        expect(jevState(jev())).toEqual({ tone: 'off', label: 'Выключен', note: 'Нужен ключ OpenRouter' });
    });

    it('is quietly off once a key is stored', () => {
        expect(jevState(jev({ configured: true }))).toEqual({ tone: 'off', label: 'Выключен' });
    });

    it('is active while the scheduler runs', () => {
        expect(jevState(jev({ enabled: true, configured: true, run_state: 'active' }))).toEqual({ tone: 'ok', label: 'Активен' });
    });

    it('names the cause of a pause', () => {
        expect(jevState(jev({ enabled: true, configured: true, run_state: 'backoff' })).note).toBe('Запрос не прошёл, скоро повтор');
        expect(jevState(jev({ enabled: true, configured: true, run_state: 'cost_limited' })).note).toBe('Дневной лимит расходов');
    });

    it('falls back to waiting for an unknown run state', () => {
        expect(jevState(jev({ enabled: true, configured: true, run_state: 'something-new' }))).toEqual({ tone: 'warn', label: 'Ждёт' });
    });
});

describe('socState', () => {
    it('is off while disabled even though claims are paused', () => {
        const paused = soc({ claims_paused_reason: 'soc disabled' });
        expect(socState(paused)).toEqual({ tone: 'off', label: 'Выключен' });
    });

    it('is on without a note when a worker checked in', () => {
        expect(socState(soc({ enabled: true, worker_last_seen_unix: 1000 }))).toEqual({ tone: 'ok', label: 'Включён' });
    });

    it('warns about the daily job limit ahead of a missing worker', () => {
        const paused = soc({ enabled: true, claims_paused_reason: 'daily job limit reached until next UTC day' });
        expect(socState(paused)).toEqual({ tone: 'warn', label: 'Включён', note: 'Дневной лимит задач' });
    });

    it('warns when no worker ever checked in', () => {
        expect(socState(soc({ enabled: true }))).toEqual({ tone: 'warn', label: 'Включён', note: 'Воркер ещё не подключался' });
    });
});

describe('formatSeen', () => {
    const now = new Date(2026, 10, 25, 18, 30).getTime();
    const at = (day: number, h: number, m: number) => new Date(2026, 10, day, h, m).getTime() / 1000;

    it('says so when nobody ever connected', () => {
        expect(formatSeen(null, now)).toBe('не подключался');
        expect(formatSeen(undefined, now)).toBe('не подключался');
    });

    it('shows the clock time within the same day', () => {
        expect(formatSeen(at(25, 9, 5), now)).toBe('09:05');
    });

    it('prefixes the date on later days', () => {
        expect(formatSeen(at(24, 23, 59), now)).toBe('24.11 23:59');
    });
});

describe('formatMoney', () => {
    it('always shows cents', () => {
        expect(formatMoney(5)).toBe('$5.00');
        expect(formatMoney(0.5)).toBe('$0.50');
    });
});
