import { describe, it, expect } from 'vitest';
import { parseSetting, type SettingRow } from './settingRows';

const row = (over: Partial<SettingRow>): SettingRow => ({ key: 'k', label: 'L', hint: '', ...over });

describe('parseSetting', () => {
    it('accepts finite floats and keeps the typed text', () => {
        expect(parseSetting(row({ type: 'float' }), ' 0.7 ')).toEqual({ value: '0.7', view: '0.7' });
    });

    it('rejects floats that are empty or not a number', () => {
        expect(parseSetting(row({ type: 'float' }), '')).toEqual({ error: 'нужно число' });
        expect(parseSetting(row({ type: 'float' }), 'abc')).toEqual({ error: 'нужно число' });
    });

    it('accepts only whole numbers for int rows', () => {
        expect(parseSetting(row({ type: 'int' }), '12')).toEqual({ value: '12', view: '12' });
        expect(parseSetting(row({ type: 'int' }), '1.5')).toEqual({ error: 'нужно целое число' });
        expect(parseSetting(row({ type: 'int' }), '')).toEqual({ error: 'нужно целое число' });
    });

    it('lets optional text be empty but not required text', () => {
        expect(parseSetting(row({ type: 'text' }), '')).toEqual({ value: '', view: '' });
        expect(parseSetting(row({ type: 'text', required: true }), ' ')).toEqual({ error: 'не может быть пустым' });
    });

    it('stores byte rows as whole bytes', () => {
        expect(parseSetting(row({ type: 'int', bytes: true }), '16', 'KB')).toEqual({ value: String(16 * 1024), view: '16', unit: 'KB' });
        expect(parseSetting(row({ bytes: true }), '1.5', 'MB')).toEqual({ value: String(1.5 * 1024 * 1024), view: '1.5', unit: 'MB' });
    });

    it('rejects byte values that do not resolve to whole bytes', () => {
        expect(parseSetting(row({ bytes: true }), '0', 'B')).toEqual({ error: 'введите положительное число' });
        expect(parseSetting(row({ bytes: true }), '0.0001', 'KB')).toEqual({ error: 'получается дробное число байт' });
    });
});
