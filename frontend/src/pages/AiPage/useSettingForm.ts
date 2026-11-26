import { useCallback, useEffect, useRef, useState } from 'react';
import { errorMessage } from '../../lib/api';
import { toDisplay, type ByteUnit } from '../../lib/byteUnits';
import { parseSetting, type SettingRow } from '../../lib/settingRows';

export interface Feedback {
    kind: 'ok' | 'error';
    text: string;
}

const NOTICE_MS = 3000;

/** Save shows only on rows that differ from the stored value, and saving
 * one row keeps the edits made in other rows. */
export function useSettingForm(rows: SettingRow[], put: (key: string, value: string) => Promise<void>) {
    const [values, setValues] = useState<Record<string, string>>({});
    const [units, setUnits] = useState<Record<string, ByteUnit>>({});
    const [baseline, setBaseline] = useState<Record<string, string>>({});
    const [baselineUnits, setBaselineUnits] = useState<Record<string, ByteUnit>>({});
    const [busy, setBusy] = useState(false);
    const [feedback, setFeedback] = useState<Feedback | null>(null);
    const [rowError, setRowError] = useState<{ key: string; text: string } | null>(null);
    const noticeTimer = useRef<number | undefined>(undefined);

    useEffect(() => () => window.clearTimeout(noticeTimer.current), []);

    const flash = useCallback((text: string) => {
        setFeedback({ kind: 'ok', text });
        window.clearTimeout(noticeTimer.current);
        noticeTimer.current = window.setTimeout(() => setFeedback(null), NOTICE_MS);
    }, []);

    const load = useCallback((settings: Record<string, unknown>) => {
        const nextValues: Record<string, string> = {};
        const nextUnits: Record<string, ByteUnit> = {};
        for (const row of rows) {
            const raw = String(settings[row.key] ?? '');
            if (row.bytes) {
                const shown = toDisplay(Number(raw));
                nextValues[row.key] = shown.value;
                nextUnits[row.key] = shown.unit;
            } else {
                nextValues[row.key] = raw;
            }
        }
        setValues(nextValues);
        setUnits(nextUnits);
        setBaseline(nextValues);
        setBaselineUnits(nextUnits);
    }, [rows]);

    const edit = useCallback((key: string, value: string) => {
        setValues(v => ({ ...v, [key]: value }));
    }, []);

    const setUnit = useCallback((key: string, unit: ByteUnit) => {
        setUnits(u => ({ ...u, [key]: unit }));
    }, []);

    const isDirty = useCallback((row: SettingRow) => {
        const changed = (values[row.key] ?? '') !== (baseline[row.key] ?? '');
        const rescaled = !!row.bytes && units[row.key] !== baselineUnits[row.key];
        return changed || rescaled;
    }, [values, units, baseline, baselineUnits]);

    const save = useCallback(async (row: SettingRow): Promise<boolean> => {
        const parsed = parseSetting(row, values[row.key] ?? '', units[row.key]);
        if ('error' in parsed) {
            setRowError({ key: row.key, text: parsed.error });
            return false;
        }
        setRowError(null);
        setBusy(true);
        try {
            await put(row.key, parsed.value);
        } catch (e) {
            setRowError({ key: row.key, text: `не сохранено (${errorMessage(e)})` });
            return false;
        } finally {
            setBusy(false);
        }
        setValues(v => ({ ...v, [row.key]: parsed.view }));
        setBaseline(b => ({ ...b, [row.key]: parsed.view }));
        if (row.bytes && parsed.unit) {
            setUnits(u => ({ ...u, [row.key]: parsed.unit }));
            setBaselineUnits(u => ({ ...u, [row.key]: parsed.unit }));
        }
        flash('Сохранено');
        return true;
    }, [values, units, put, flash]);

    const saveRaw = useCallback(async (key: string, value: string, notice?: string): Promise<boolean> => {
        setFeedback(null);
        setBusy(true);
        try {
            await put(key, value);
        } catch (e) {
            setFeedback({ kind: 'error', text: `Не сохранено (${errorMessage(e)})` });
            return false;
        } finally {
            setBusy(false);
        }
        if (notice) flash(notice);
        return true;
    }, [put, flash]);

    const fail = useCallback((text: string) => setFeedback({ kind: 'error', text }), []);
    const clearFeedback = useCallback(() => setFeedback(null), []);

    return { values, units, busy, feedback, rowError, load, edit, setUnit, isDirty, save, saveRaw, fail, clearFeedback };
}

export type SettingForm = ReturnType<typeof useSettingForm>;
