import { describe, expect, it } from 'vitest';
import { BYTE_UNITS, fromDisplay, toDisplay } from './byteUnits';

describe('toDisplay', () => {
    it('picks the largest unit that fits', () => {
        expect(toDisplay(10 * 1024 ** 3)).toEqual({ value: '10', unit: 'GB' });
        expect(toDisplay(256 * 1024 ** 2)).toEqual({ value: '256', unit: 'MB' });
        expect(toDisplay(2 * 1024 ** 3)).toEqual({ value: '2', unit: 'GB' });
    });

    it('keeps small values in bytes', () => {
        expect(toDisplay(500)).toEqual({ value: '500', unit: 'B' });
        expect(toDisplay(1024)).toEqual({ value: '1', unit: 'KB' });
    });

    it('trims fractional tails', () => {
        expect(toDisplay(1.5 * 1024 ** 2)).toEqual({ value: '1.5', unit: 'MB' });
        expect(toDisplay(1536)).toEqual({ value: '1.5', unit: 'KB' });
    });

    it('returns empty for junk', () => {
        expect(toDisplay(0)).toEqual({ value: '', unit: 'B' });
        expect(toDisplay(-5)).toEqual({ value: '', unit: 'B' });
    });
});

describe('fromDisplay', () => {
    it('converts back to exact bytes', () => {
        expect(fromDisplay('10', 'GB')).toEqual({ bytes: 10 * 1024 ** 3 });
        expect(fromDisplay('256', 'MB')).toEqual({ bytes: 256 * 1024 ** 2 });
        expect(fromDisplay('512', 'B')).toEqual({ bytes: 512 });
    });

    it('accepts commas and fractional values that stay integral', () => {
        expect(fromDisplay('1,5', 'KB')).toEqual({ bytes: 1536 });
    });

    it('rejects junk and fractional bytes', () => {
        expect('error' in fromDisplay('', 'MB')).toBe(true);
        expect('error' in fromDisplay('-3', 'MB')).toBe(true);
        expect('error' in fromDisplay('abc', 'GB')).toBe(true);
        expect('error' in fromDisplay('0.3', 'KB')).toBe(true);
    });

    it('round-trips the default budgets', () => {
        for (const bytes of [10 * 1024 ** 3, 256 * 1024 ** 2, 2 * 1024 ** 3]) {
            const d = toDisplay(bytes);
            expect(fromDisplay(d.value, d.unit)).toEqual({ bytes });
        }
    });
});

describe('BYTE_UNITS', () => {
    it('is ordered small to large with 1024 steps', () => {
        expect(BYTE_UNITS.map(u => u.multiplier)).toEqual([1, 1024, 1024 ** 2, 1024 ** 3]);
    });
});
