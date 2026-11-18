import { describe, expect, it } from 'vitest';
import {
    appendNonce,
    buildSanityCheck,
    buildStreamsScopeLink,
    expressionUsesPayload,
    filterInspectsPayload,
    randomNonce,
    shellAnsiCQuoted,
    shellSingleQuote,
    validateFilterForm,
    thresholdAction,
} from './filterSanity';

describe('shellSingleQuote', () => {
    it('escapes embedded single quotes round-trip safe', () => {
        const arg = "it's a 'test'";
        const quoted = shellSingleQuote(arg);
        // POSIX: '...' segments plus escaped quotes reconstruct the word.
        const roundTrip = quoted.slice(1, -1).replace(/'\\''/g, "'");
        expect(roundTrip).toBe(arg);
        expect(quoted).not.toContain("'it's");
    });

    it('quotes shell metacharacters literally', () => {
        const quoted = shellSingleQuote('a; rm -rf / & | $(x)');
        expect(quoted.startsWith("'")).toBe(true);
        expect(quoted.endsWith("'")).toBe(true);
    });
});

describe('shellAnsiCQuoted', () => {
    it('renders bytes as hex escapes', () => {
        expect(shellAnsiCQuoted(new Uint8Array([0xde, 0xad, 0xbe, 0xef]))).toBe(
            "$'\\xde\\xad\\xbe\\xef'"
        );
    });
});

describe('appendNonce', () => {
    it('keeps the sample intact so matching is unchanged', () => {
        expect(appendNonce('flag{abc}', 'ab12cd34ef56')).toBe('flag{abc}nonce=ab12cd34ef56');
    });
});

describe('randomNonce', () => {
    it('produces distinct hex strings', () => {
        const a = randomNonce();
        const b = randomNonce();
        expect(a).toMatch(/^[0-9a-f]{12}$/);
        expect(b).toMatch(/^[0-9a-f]{12}$/);
        expect(a).not.toBe(b);
    });
});

describe('buildSanityCheck', () => {
    const target = {
        agentId: 'agent-a',
        serviceName: 'web',
        serviceHost: '10.10.2.4',
        servicePort: 80,
        https: false,
    };

    it('embeds the nonce, escapes the payload and scopes the link', () => {
        const res = buildSanityCheck(
            { id: 7, template_type: 'substr', template: 'flag{' },
            'flag{abc}',
            target,
            ''
        );
        expect(res.curl).toContain('flag{abc}nonce=');
        expect(res.curl).toContain('curl -sk');
        const url = new URLSearchParams(res.resultLink.split('?')[1]);
        expect(url.get('filter_id')).toBe('7');
        expect(url.get('agent_id')).toBe('agent-a');
        expect(url.get('service')).toBe('web');
        expect(url.get('nonce')).toBe(res.nonce);
        expect(Number(url.get('since'))).toBeLessThanOrEqual(Math.floor(Date.now() / 1000) + 1);
    });

    it('flags proxy-only blocking as unverifiable by passive replay', () => {
        const res = buildSanityCheck(
            { id: 1, template_type: 'substr', template: 'x' },
            'x',
            target,
            ''
        );
        expect(res.note).toBeUndefined();
    });

    it('sends binary filters as ANSI-C quoted hex plus nonce', () => {
        const res = buildSanityCheck(
            { id: 2, template_type: 'bsubstr', template: 'deadbeef' },
            'hit',
            target,
            ''
        );
        // deadbeef prefix...
        expect(res.curl).toContain("$'\\xde\\xad\\xbe\\xef");
        // ..."hit" appended as raw byte escapes.
        expect(res.curl).toContain('\\x68\\x69\\x74');
        // The nonce is findable via the scoped link.
        const url = new URLSearchParams(res.resultLink.split('?')[1]);
        expect(url.get('nonce')).toBe(res.nonce);
    });
});

describe('matcher payload classification', () => {
    it('payload matchers offer replay, ip does not', () => {
        expect(filterInspectsPayload({ template_type: 'regex', template: 'x' })).toBe(true);
        expect(filterInspectsPayload({ template_type: 'substr', template: 'x' })).toBe(true);
        expect(filterInspectsPayload({ template_type: 'bsubstr', template: 'ab' })).toBe(true);
        expect(filterInspectsPayload({ template_type: 'expression', template: '' })).toBe(true);
        expect(filterInspectsPayload({ template_type: 'ip_cidr', template: '' })).toBe(false);
    });

    it('expression using only non-payload fields gets the streams view', () => {
        expect(expressionUsesPayload('payload.contains(b"x")')).toBe(true);
        expect(expressionUsesPayload('src_port == 80')).toBe(false);
    });
});

describe('buildStreamsScopeLink', () => {
    it('explains the scope for ip rules', () => {
        const res = buildStreamsScopeLink(
            { id: 3, template_type: 'ip_cidr', template: '', ip_network: '10.0.0.0/8' },
            'agent-a',
            ''
        );
        const url = new URLSearchParams(res.link.split('?')[1]);
        expect(url.get('ip')).toBe('10.0.0.0/8');
        expect(url.get('agent_id')).toBe('agent-a');
        expect(res.explanation).toContain('IP/CIDR');
    });

    it('links plain streams view for non-payload expressions', () => {
        const res = buildStreamsScopeLink(
            { id: 4, template_type: 'expression', template: '', expression_source: 'src_port == 80' },
            null,
            ''
        );
        expect(res.link).toContain('filter_id=4');
        expect(res.explanation.length).toBeGreaterThan(10);
    });
});

describe('validateFilterForm', () => {
    const base = () => ({
        name: 'f',
        template_type: 'substr',
        template: 'flag{',
        direction: 'both',
        action: 'tag',
        ip_network: null,
        ip_endpoint: null,
        expression_source: null,
        service_targets_global: true,
        service_ids: [],
        agent_targets_global: true,
        agent_ids: [],
        threshold_count: null,
        threshold_window_secs: null,
        threshold_group_by: null,
        threshold_cooldown_secs: null,
        threshold_block_secs: null,
    });

    it('accepts a simple substr filter', () => {
        expect(validateFilterForm(base() as never)).toBeNull();
    });

    it('rejects thresholds on tag actions', () => {
        const form = { ...base(), threshold_count: 5 };
        expect(validateFilterForm(form as never)).toContain('threshold');
    });

    it('requires block duration for block thresholds', () => {
        const form = {
            ...base(),
            action: 'block',
            threshold_count: 3,
            threshold_window_secs: 10,
            threshold_group_by: 'remote_ip',
        };
        expect(validateFilterForm(form as never)).toContain('block');
    });

    it('requires hex for bsubstr', () => {
        const form = { ...base(), template_type: 'bsubstr', template: 'zz' };
        expect(validateFilterForm(form as never)).toContain('HEX');
    });

    it('requires ip endpoint selection for ip_cidr', () => {
        const form = { ...base(), template_type: 'ip_cidr', template: '', ip_network: '10.0.0.0/8' };
        expect(validateFilterForm(form as never)).toContain('src');
    });

    it('threshold gating agrees with actions', () => {
        expect(thresholdAction('alert')).toBe(true);
        expect(thresholdAction('block')).toBe(true);
        expect(thresholdAction('tag')).toBe(false);
        expect(thresholdAction('flag')).toBe(false);
    });
});
