export type ByteUnit = 'B' | 'KB' | 'MB' | 'GB';

export const BYTE_UNITS: { unit: ByteUnit; label: string; multiplier: number }[] = [
    { unit: 'B', label: 'Б', multiplier: 1 },
    { unit: 'KB', label: 'КБ', multiplier: 1024 },
    { unit: 'MB', label: 'МБ', multiplier: 1024 * 1024 },
    { unit: 'GB', label: 'ГБ', multiplier: 1024 * 1024 * 1024 },
];

export interface ByteDisplay {
    value: string;
    unit: ByteUnit;
}

export function toDisplay(bytes: number): ByteDisplay {
    if (!Number.isFinite(bytes) || bytes <= 0) return { value: '', unit: 'B' };
    let best: { unit: ByteUnit; multiplier: number } = { unit: 'B', multiplier: 1 };
    for (const u of BYTE_UNITS) {
        if (bytes / u.multiplier >= 1) best = u;
    }
    const value = bytes / best.multiplier;
    const rounded = Math.round(value * 100) / 100;
    return { value: String(Number.isInteger(rounded) ? rounded : rounded.toFixed(2).replace(/0+$/, '').replace(/\.$/, '')), unit: best.unit };
}

export function fromDisplay(value: string, unit: ByteUnit): { bytes: number } | { error: string } {
    const num = Number(value.trim().replace(',', '.'));
    if (!value.trim() || !Number.isFinite(num) || num <= 0) {
        return { error: 'введите положительное число' };
    }
    const multiplier = BYTE_UNITS.find(u => u.unit === unit)?.multiplier ?? 1;
    const bytes = num * multiplier;
    if (!Number.isInteger(bytes)) return { error: 'получается дробное число байт' };
    return { bytes };
}
