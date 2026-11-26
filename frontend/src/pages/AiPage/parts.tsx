import { Fragment, ReactNode, useEffect, useId, useRef, useState, type KeyboardEvent, type RefObject } from 'react';
import { BYTE_UNITS, type ByteUnit } from '../../lib/byteUnits';
import type { StateView } from '../../lib/aiStatus';
import { copyText } from '../../lib/api';
import type { SettingRow } from '../../lib/settingRows';
import type { SettingForm } from './useSettingForm';

export function StateLine({ view, unreachable = false }: { view: StateView | null; unreachable?: boolean }) {
    if (!view) {
        return (
            <p className="ai-status">
                <span className={`ai-state ai-state--${unreachable ? 'warn' : 'off'}`}>
                    {unreachable ? 'Нет данных' : 'Загрузка'}
                </span>
            </p>
        );
    }
    return (
        <p className="ai-status">
            <span className={`ai-state ai-state--${view.tone}`}>{view.label}</span>
            {view.note && <span className="ai-note">{view.note}</span>}
        </p>
    );
}

export interface Fact {
    label: string;
    value: ReactNode;
    title?: string;
}

export function Facts({ items }: { items: Fact[] }) {
    return (
        <dl className="ai-facts">
            {items.map(item => (
                <Fragment key={item.label}>
                    <dt>{item.label}</dt>
                    <dd title={item.title}>{item.value}</dd>
                </Fragment>
            ))}
        </dl>
    );
}

export function FeedbackLine({ feedback }: { feedback: SettingForm['feedback'] }) {
    if (!feedback) return null;
    return (
        <p className={`ai-feedback ai-feedback--${feedback.kind}`} role={feedback.kind === 'error' ? 'alert' : 'status'}>
            {feedback.text}
        </p>
    );
}

/** A credential shown exactly once; the parent drops it on hide. */
export function SecretReveal({ title, secret, onHide }: { title: string; secret: string; onHide: () => void }) {
    const [copied, setCopied] = useState(false);
    const titleId = useId();
    const timer = useRef<number | undefined>(undefined);

    useEffect(() => () => window.clearTimeout(timer.current), []);

    const copy = () => {
        copyText(secret);
        setCopied(true);
        window.clearTimeout(timer.current);
        timer.current = window.setTimeout(() => setCopied(false), 1500);
    };

    return (
        <div className="ai-secret" role="group" aria-labelledby={titleId}>
            <span id={titleId} className="ai-secret-title">{title}</span>
            <code className="ai-secret-value">{secret}</code>
            <div className="ai-secret-actions" aria-live="polite">
                <button type="button" className="ai-btn" onClick={copy}>{copied ? 'Скопировано' : 'Копировать'}</button>
                <button type="button" className="ai-btn" onClick={onHide}>Скрыть</button>
            </div>
        </div>
    );
}

export function Disclosure({ id, label, open, onToggle, children }: {
    id: string;
    label: string;
    open: boolean;
    onToggle: () => void;
    children: ReactNode;
}) {
    return (
        <div className="ai-disclosure">
            <button type="button" className="ai-toggle" aria-expanded={open} aria-controls={id} onClick={onToggle}>
                {label}
            </button>
            {open && <div id={id} className="ai-panel">{children}</div>}
        </div>
    );
}

interface SecretFieldProps {
    label: string;
    value: string;
    onChange: (value: string) => void;
    onSave: () => void;
    configured: boolean;
    saving: boolean;
    placeholder: string;
    inputRef?: RefObject<HTMLInputElement>;
}

export function SecretField({ label, value, onChange, onSave, configured, saving, placeholder, inputRef }: SecretFieldProps) {
    const id = useId();
    const filled = value.trim().length > 0;

    const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
        if (e.key === 'Enter' && filled) {
            e.preventDefault();
            onSave();
        }
    };

    return (
        <div className="ai-field">
            <label className="ai-field-label" htmlFor={id}>{label}</label>
            <div className="ai-field-control">
                <input
                    ref={inputRef}
                    id={id}
                    type="password"
                    autoComplete="off"
                    className="ai-input"
                    value={value}
                    placeholder={placeholder}
                    onChange={e => onChange(e.target.value)}
                    onKeyDown={onKeyDown}
                />
                {filled ? (
                    <div className="ai-field-action">
                        <button type="button" className="ai-btn ai-btn--inline" onClick={onSave} disabled={saving}>Сохранить</button>
                    </div>
                ) : configured ? (
                    <div className="ai-field-action">
                        <button type="button" className="ai-btn ai-btn--inline ai-btn--danger" onClick={onSave} disabled={saving}>Удалить</button>
                    </div>
                ) : null}
            </div>
        </div>
    );
}

interface ModelPicker {
    /** Key of the row that takes the catalog suggestions. */
    rowKey: string;
    listId: string;
    models: { id: string }[];
    onChange: (model: string) => void;
}

interface SettingRowsProps {
    rows: SettingRow[];
    form: SettingForm;
    onSaved: (key: string) => void;
    modelPicker?: ModelPicker;
}

export function SettingRows({ rows, form, onSaved, modelPicker }: SettingRowsProps) {
    return (
        <>
            {rows.map(row => (
                <FieldRow
                    key={row.key}
                    row={row}
                    form={form}
                    onSaved={onSaved}
                    list={modelPicker && modelPicker.rowKey === row.key ? modelPicker.listId : undefined}
                    onValueChange={modelPicker && modelPicker.rowKey === row.key ? modelPicker.onChange : undefined}
                />
            ))}
            {modelPicker && (
                <datalist id={modelPicker.listId}>
                    {modelPicker.models.map(m => <option key={m.id} value={m.id} />)}
                </datalist>
            )}
        </>
    );
}

interface FieldRowProps {
    row: SettingRow;
    form: SettingForm;
    list?: string;
    onSaved: (key: string) => void;
    onValueChange?: (value: string) => void;
}

function FieldRow({ row, form, list, onSaved, onValueChange }: FieldRowProps) {
    const id = useId();
    const hintId = `${id}-hint`;
    const dirty = form.isDirty(row);
    const error = form.rowError?.key === row.key ? form.rowError.text : null;
    const numeric = row.type !== 'text' || !!row.bytes;

    const commit = () => {
        form.save(row).then(ok => {
            if (ok) onSaved(row.key);
        });
    };

    const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
        if (e.key === 'Enter' && dirty) {
            e.preventDefault();
            commit();
        }
    };

    return (
        <div className="ai-field">
            <label className="ai-field-label" htmlFor={id}>{row.label}</label>
            <div className="ai-field-control">
                <input
                    id={id}
                    className="ai-input"
                    type="text"
                    inputMode={row.type === 'int' && !row.bytes ? 'numeric' : numeric ? 'decimal' : undefined}
                    list={list}
                    title={row.hint}
                    value={form.values[row.key] ?? ''}
                    onChange={e => (onValueChange ? onValueChange(e.target.value) : form.edit(row.key, e.target.value))}
                    onKeyDown={onKeyDown}
                    aria-describedby={hintId}
                />
                {row.bytes && (
                    <select
                        className="ai-select ai-select--unit"
                        aria-label="Единица измерения"
                        value={form.units[row.key] ?? 'B'}
                        onChange={e => form.setUnit(row.key, e.target.value as ByteUnit)}
                    >
                        {BYTE_UNITS.map(u => <option key={u.unit} value={u.unit}>{u.label}</option>)}
                    </select>
                )}
                {dirty && (
                    <div className={row.bytes ? 'ai-field-action ai-field-action--flow' : 'ai-field-action'}>
                        <button type="button" className="ai-btn ai-btn--inline" onClick={commit} disabled={form.busy}>Сохранить</button>
                    </div>
                )}
            </div>
            <span id={hintId} className="ai-vh">{row.hint}</span>
            {error && <p className="ai-field-error" role="alert">{row.label}: {error}</p>}
        </div>
    );
}
