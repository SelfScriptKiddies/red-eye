import { describe, it, expect } from 'vitest';
import { parseStreamsParam, buildAgentLink, buildShareLink, MAX_AGENT_LINK_IDS } from './streamsLink';

describe('parseStreamsParam', () => {
    it('parses comma separated ids', () => {
        expect(parseStreamsParam('1,2,3')).toEqual([1, 2, 3]);
    });

    it('parses a single id and tolerates spaces', () => {
        expect(parseStreamsParam('42')).toEqual([42]);
        expect(parseStreamsParam(' 7 , 8 ')).toEqual([7, 8]);
    });

    it('returns empty for null, empty and junk-only values', () => {
        expect(parseStreamsParam(null)).toEqual([]);
        expect(parseStreamsParam(undefined)).toEqual([]);
        expect(parseStreamsParam('')).toEqual([]);
        expect(parseStreamsParam('abc,def')).toEqual([]);
    });

    it('drops non-numeric and non-positive tokens', () => {
        expect(parseStreamsParam('1,x,2')).toEqual([1, 2]);
        expect(parseStreamsParam('0,-5,3')).toEqual([3]);
    });
});

describe('buildAgentLink', () => {
    it('builds a streams url on the given origin', () => {
        const { url, truncated } = buildAgentLink('https://red-eye.example.com', [1, 2, 3]);
        expect(url).toBe('https://red-eye.example.com/packets?streams=1,2,3');
        expect(truncated).toBe(false);
    });

    it('caps the id list at 25 and flags truncation', () => {
        const ids = Array.from({ length: MAX_AGENT_LINK_IDS + 5 }, (_, i) => i + 1);
        const { url, truncated } = buildAgentLink('https://x', ids);
        expect(url).toBe(`https://x/packets?streams=${Array.from({ length: MAX_AGENT_LINK_IDS }, (_, i) => i + 1).join(',')}`);
        expect(truncated).toBe(true);
    });

    it('keeps a list at the cap intact', () => {
        const ids = Array.from({ length: MAX_AGENT_LINK_IDS }, (_, i) => i + 1);
        const { truncated } = buildAgentLink('https://x', ids);
        expect(truncated).toBe(false);
    });
});

describe('buildShareLink', () => {
    it('builds the MCP-canonical streams url without a note', () => {
        const { url, note } = buildShareLink('https://red-eye.example.com', [1, 2, 3]);
        expect(url).toBe('https://red-eye.example.com/packets?streams=1,2,3');
        expect(note).toBe('');
    });

    it('emits a cap note when the selection is truncated', () => {
        const ids = Array.from({ length: MAX_AGENT_LINK_IDS + 1 }, (_, i) => i + 1);
        const { url, note } = buildShareLink('https://x', ids);
        expect(url).toBe(`https://x/packets?streams=${Array.from({ length: MAX_AGENT_LINK_IDS }, (_, i) => i + 1).join(',')}`);
        expect(note).toContain('25');
    });
});
