import { fromDisplay, type ByteUnit } from './byteUnits';

export interface SettingRow {
    key: string;
    label: string;
    /** Tooltip text, never rendered as visible copy. */
    hint: string;
    type?: 'float' | 'int' | 'text';
    bytes?: boolean;
    required?: boolean;
    advanced?: boolean;
}

export type ParsedSetting =
    | { value: string; view: string; unit?: ByteUnit }
    | { error: string };

/** Client-side mirror of the backend checks; the server still validates. */
export function parseSetting(row: SettingRow, raw: string, unit: ByteUnit = 'B'): ParsedSetting {
    const text = raw.trim();
    if (row.bytes) {
        const parsed = fromDisplay(text, unit);
        return 'error' in parsed ? parsed : { value: String(parsed.bytes), view: text, unit };
    }
    if (row.type === 'float') {
        if (!text || !Number.isFinite(Number(text))) return { error: 'нужно число' };
    } else if (row.type === 'int') {
        if (!text || !Number.isInteger(Number(text))) return { error: 'нужно целое число' };
    } else if (row.required && !text) {
        return { error: 'не может быть пустым' };
    }
    return { value: text, view: text };
}
