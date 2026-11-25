import { describe, it, expect } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import { Facts, SecretReveal, SettingRows, StateLine } from './parts';
import { AI_SETTING_ROWS } from '../../lib/aiVerdict';
import { useSettingForm } from './useSettingForm';

const noop = () => undefined;
const put = () => Promise.resolve();

describe('StateLine', () => {
    it('colours the label by tone and skips an absent note', () => {
        const html = renderToStaticMarkup(<StateLine view={{ tone: 'ok', label: 'Активен' }} />);
        expect(html).toContain('ai-state--ok');
        expect(html).toContain('Активен');
        expect(html).not.toContain('ai-note');
    });

    it('adds the note next to the label', () => {
        const html = renderToStaticMarkup(<StateLine view={{ tone: 'off', label: 'Выключен', note: 'Нужен ключ' }} />);
        expect(html).toContain('ai-note">Нужен ключ');
    });

    it('waits without a state and says so when the status is unreachable', () => {
        expect(renderToStaticMarkup(<StateLine view={null} />)).toContain('Загрузка');
        expect(renderToStaticMarkup(<StateLine view={null} unreachable />)).toContain('Нет данных');
    });
});

describe('Facts', () => {
    it('renders each item as a labelled value', () => {
        const html = renderToStaticMarkup(<Facts items={[{ label: 'Очередь', value: 3, title: 'подробно' }]} />);
        expect(html).toContain('<dt>Очередь</dt>');
        expect(html).toContain('<dd title="подробно">3</dd>');
    });
});

describe('SecretReveal', () => {
    it('shows the secret once with copy and hide actions', () => {
        const html = renderToStaticMarkup(<SecretReveal title="Токен показан один раз" secret="sk-abc" onHide={noop} />);
        expect(html).toContain('aria-labelledby');
        expect(html).toContain('sk-abc');
        expect(html).toContain('Копировать');
        expect(html).toContain('Скрыть');
    });
});

function Harness() {
    const form = useSettingForm(AI_SETTING_ROWS, put);
    return <SettingRows rows={AI_SETTING_ROWS.filter(row => !row.advanced)} form={form} onSaved={noop} />;
}

describe('SettingRows', () => {
    it('labels every input and hides Save until a row changes', () => {
        const html = renderToStaticMarkup(<Harness />);
        expect(html.match(/<label /g)).toHaveLength(2);
        expect(html.match(/aria-describedby=/g)).toHaveLength(2);
        expect(html).not.toContain('Сохранить');
    });
});
