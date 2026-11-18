import { describe, it, expect } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import { StreamSign } from './StreamSign';

describe('StreamSign', () => {
    it('renders nothing without a flag', () => {
        expect(renderToStaticMarkup(<StreamSign containsFlag={false} />)).toBe('');
    });

    it('renders a labelled inline SVG when the stream has a flag', () => {
        const html = renderToStaticMarkup(<StreamSign containsFlag={true} />);
        expect(html).toContain('<svg');
        expect(html).toContain('role="img"');
        expect(html).toContain('aria-label="Stream contains flag"');
    });

    it('stays local: no Font Awesome classes or external references', () => {
        const html = renderToStaticMarkup(<StreamSign containsFlag={true} />);
        expect(html).not.toMatch(/fa-solid|fa-flag|fontawesome/i);
        expect(html).not.toContain('http');
    });
});
