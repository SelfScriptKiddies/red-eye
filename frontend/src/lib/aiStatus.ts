// Operator-facing state of the AI page columns. No IO.

import type { AiStatus } from './aiVerdict';
import type { SocStatus } from './socTriage';

export type StateTone = 'off' | 'ok' | 'warn';

export interface StateView {
    tone: StateTone;
    label: string;
    note?: string;
}

export function jevState(status: AiStatus): StateView {
    if (!status.enabled) {
        return status.configured
            ? { tone: 'off', label: 'Выключен' }
            : { tone: 'off', label: 'Выключен', note: 'Нужен ключ OpenRouter' };
    }
    switch (status.run_state) {
        case 'active':
            return { tone: 'ok', label: 'Активен' };
        case 'backoff':
            return { tone: 'warn', label: 'Пауза', note: 'Запрос не прошёл, скоро повтор' };
        case 'cost_limited':
            return { tone: 'warn', label: 'Лимит', note: 'Дневной лимит расходов' };
        case 'unconfigured':
            return { tone: 'warn', label: 'Ждёт', note: 'Нужен ключ OpenRouter' };
        default:
            return { tone: 'warn', label: 'Ждёт' };
    }
}

export function socState(status: SocStatus): StateView {
    if (!status.enabled) {
        return { tone: 'off', label: 'Выключен' };
    }
    if (status.claims_paused_reason) {
        return { tone: 'warn', label: 'Включён', note: 'Дневной лимит задач' };
    }
    if (!status.worker_last_seen_unix) {
        return { tone: 'warn', label: 'Включён', note: 'Воркер ещё не подключался' };
    }
    return { tone: 'ok', label: 'Включён' };
}

const pad = (n: number): string => String(n).padStart(2, '0');

/** Clock time for a last-seen stamp; a date prefix once the day changes. */
export function formatSeen(unix: number | null | undefined, nowMs: number = Date.now()): string {
    if (!unix) return 'не подключался';
    const at = new Date(unix * 1000);
    const time = `${pad(at.getHours())}:${pad(at.getMinutes())}`;
    const now = new Date(nowMs);
    return at.toDateString() === now.toDateString() ? time : `${pad(at.getDate())}.${pad(at.getMonth() + 1)} ${time}`;
}

export function formatMoney(usd: number): string {
    return `$${usd.toFixed(2)}`;
}
