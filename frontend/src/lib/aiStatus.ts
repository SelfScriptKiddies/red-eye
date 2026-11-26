// Operator-facing state of the AI page columns. No IO.

import type { AiStatus } from './aiVerdict';
import type { SocStatus } from './socTriage';

export type StateTone = 'off' | 'ok' | 'warn';

export interface StateView {
    tone: StateTone;
    label: string;
    note?: string;
}

const FAILURE_NOTES: Record<string, string> = {
    tick: 'Шаг планировщика не выполнился',
    transcript_db: 'Ошибка базы при сборке транскрипта',
    network: 'Провайдер недоступен',
    rate_limited: 'Лимит запросов провайдера',
    provider_server: 'Ошибка на стороне провайдера',
    provider_rejected: 'Провайдер отклонил запрос',
    provider_malformed: 'Провайдер прислал нечитаемый ответ',
    response_schema: 'Ответ провайдера не прошёл проверку',
    verdict_insert: 'Не удалось сохранить вердикт',
};

function plural(n: number, one: string, few: string, many: string): string {
    const abs = Math.abs(n) % 100;
    if (abs >= 11 && abs <= 14) return many;
    const last = abs % 10;
    return last === 1 ? one : last >= 2 && last <= 4 ? few : many;
}

function failureNote(reason: string, count: number): string {
    const note = FAILURE_NOTES[reason] ?? 'Последняя попытка не удалась';
    return count > 1 ? `${note} (${count} ${plural(count, 'раз', 'раза', 'раз')})` : note;
}

export function jevState(status: AiStatus): StateView {
    if (!status.enabled) {
        return status.configured
            ? { tone: 'off', label: 'Выключен' }
            : { tone: 'off', label: 'Выключен', note: 'Нужен ключ OpenRouter' };
    }
    const failure = status.last_error
        ? failureNote(status.last_error.reason, status.last_error.count)
        : undefined;
    switch (status.run_state) {
        case 'active':
            return failure
                ? { tone: 'warn', label: 'Активен', note: failure }
                : { tone: 'ok', label: 'Активен' };
        case 'backoff':
            return { tone: 'warn', label: 'Пауза', note: failure ?? 'Запрос не прошёл, скоро повтор' };
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

/** Cents by default; JEV verdicts cost far less, so sub-cent spend keeps
 * four decimals instead of reading as zero. */
export function formatMoney(usd: number): string {
    const digits = usd > 0 && usd < 0.01 ? 4 : 2;
    return `$${usd.toFixed(digits)}`;
}
