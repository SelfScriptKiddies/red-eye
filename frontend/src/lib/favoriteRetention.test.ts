import { describe, expect, it } from 'vitest';
import {
    capacityPercent,
    formatBytes,
    retentionBadge,
    starAdmissible,
} from './favoriteRetention';

describe('retentionBadge', () => {
    it('starred archiving and archived are distinct', () => {
        expect(retentionBadge(true, 'archiving')?.key).toBe('archiving');
        expect(retentionBadge(true, 'archived')?.key).toBe('archived');
    });

    it('archive error outranks the state label', () => {
        const badge = retentionBadge(true, 'archiving', 'db timeout');
        expect(badge?.key).toBe('archive_error');
        expect(badge?.tone).toBe('error');
    });

    it('unstarred with saved content shows pending cleanup', () => {
        expect(retentionBadge(false, 'archived')?.key).toBe('cleanup_pending');
        expect(retentionBadge(false, 'archiving')?.key).toBe('cleanup_pending');
    });

    it('incomplete historical shells are labeled honestly', () => {
        expect(retentionBadge(true, 'incomplete')?.key).toBe('incomplete');
        expect(retentionBadge(false, 'incomplete')?.key).toBe('incomplete');
    });

    it('plain live rows have no badge', () => {
        expect(retentionBadge(false, 'live')).toBeNull();
        expect(retentionBadge(false, undefined)).toBeNull();
        // favorite+live does not exist as a settled backend state (star
        // commits archiving atomically; migration rewrites legacy rows),
        // so no badge is the honest render for that transient input.
        expect(retentionBadge(true, 'live')).toBeNull();
    });
});

describe('formatBytes', () => {
    it('formats the byte budgets readably', () => {
        expect(formatBytes(0)).toBe('0 B');
        expect(formatBytes(512)).toBe('512 B');
        expect(formatBytes(268435456)).toBe('256 MiB');
        expect(formatBytes(10737418240)).toBe('10.0 GiB');
        expect(formatBytes(2147483648)).toBe('2.0 GiB');
        expect(formatBytes(26843545 + 7)).toBe('25.6 MiB');
    });

    it('degrades gracefully on junk input', () => {
        expect(formatBytes(null)).toBe('0 B');
        expect(formatBytes(undefined)).toBe('0 B');
        expect(formatBytes(-5)).toBe('0 B');
        expect(formatBytes(NaN)).toBe('0 B');
    });
});

describe('starAdmissible', () => {
    it('accepts a star that exactly fills the budget', () => {
        expect(starAdmissible(600, 400, 1000)).toBe(true);
    });

    it('rejects overflow and saturated values without false stars', () => {
        expect(starAdmissible(601, 400, 1000)).toBe(false);
        expect(starAdmissible(Number.MAX_SAFE_INTEGER, 1, 1000)).toBe(false);
    });

    it('clamps nonsensical negative inputs to empty, never to credit', () => {
        // A negative total must not admit MORE than an empty budget would.
        expect(starAdmissible(-500, 400, 1000)).toBe(true);
        expect(starAdmissible(-500, 400, 300)).toBe(false);
    });
});

describe('capacityPercent', () => {
    it('caps the displayed percentage', () => {
        expect(capacityPercent(500, 1000)).toBe(50);
        expect(capacityPercent(5000, 1000)).toBe(100);
        expect(capacityPercent(1, 0)).toBe(100);
    });
});
