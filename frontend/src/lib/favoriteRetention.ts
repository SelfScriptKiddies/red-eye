// Pure helpers for starred-stream retention display and favorite capacity
// accounting. No IO - everything here is unit-testable without a backend.

export interface RetentionBadge {
    key: string;
    label: string;
    tone: 'ok' | 'warn' | 'error' | 'muted';
}

/**
 * Operator-facing retention badge for a stream card. Mirrors the backend
 * retention states (database/migrations/add_favorite_retention.sql):
 * favorite rows report their archive progress, unstarred rows with saved
 * content report pending bounded cleanup, incomplete rows are honest
 * historical shells (payload expired before retention existed).
 */
export function retentionBadge(
    favorite: boolean,
    retentionState?: string | null,
    archiveError?: string | null
): RetentionBadge | null {
    const state = retentionState ?? 'live';
    if (favorite) {
        if (archiveError) return { key: 'archive_error', label: 'ошибка архивации', tone: 'error' };
        if (state === 'archived') return { key: 'archived', label: 'сохранено', tone: 'ok' };
        if (state === 'archiving') return { key: 'archiving', label: 'архивация', tone: 'warn' };
        if (state === 'incomplete') return { key: 'incomplete', label: 'история неполная', tone: 'muted' };
        return null;
    }
    if (state === 'archiving' || state === 'archived') {
        return { key: 'cleanup_pending', label: 'очистка копии', tone: 'muted' };
    }
    if (state === 'incomplete') return { key: 'incomplete', label: 'история неполная', tone: 'muted' };
    return null;
}

/** Human-readable byte size (decimal-ish, MiB/GiB as operators expect). */
export function formatBytes(bytes?: number | null): string {
    const n = Number(bytes ?? 0);
    if (!Number.isFinite(n) || n < 0) return '0 B';
    if (n < 1024) return `${n} B`;
    const units = ['KiB', 'MiB', 'GiB', 'TiB'];
    let value = n;
    let unit = 'B';
    for (const u of units) {
        if (value < 1024) break;
        value = value / 1024;
        unit = u;
    }
    return `${value >= 100 ? Math.round(value) : value.toFixed(1)} ${unit}`;
}

/**
 * Star admission rule mirrored from the backend (favorite_service.rs):
 * reserved growth plus the new reservation must fit the total budget.
 * Saturating on purpose - overflow means rejection, never a false star.
 */
export function starAdmissible(totalReserved: number, newReservation: number, maxTotal: number): boolean {
    const sum = (a: number, b: number): number => {
        const r = a + b;
        return r > Number.MAX_SAFE_INTEGER ? Number.MAX_SAFE_INTEGER : r;
    };
    return sum(Math.max(0, totalReserved), Math.max(0, newReservation)) <= maxTotal;
}

/** Percent of the favorite budget already reserved, capped for display. */
export function capacityPercent(reserved: number, maxTotal: number): number {
    if (maxTotal <= 0) return 100;
    return Math.min(100, Math.round((reserved / maxTotal) * 100));
}
