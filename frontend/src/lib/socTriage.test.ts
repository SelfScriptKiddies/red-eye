import { describe, expect, it } from 'vitest';
import {
    isExternalLlmEndpoint,
    isSocResultEvent,
    isSocTriageEvent,
    socVerdictClass,
    type SocEvidenceItem,
} from './socTriage';

describe('isExternalLlmEndpoint', () => {
    it('treats localhost and loopback as internal', () => {
        expect(isExternalLlmEndpoint('http://localhost:8080/v1')).toBe(false);
        expect(isExternalLlmEndpoint('http://127.0.0.1:8080/v1')).toBe(false);
        expect(isExternalLlmEndpoint('http://[::1]:8080/v1')).toBe(false);
    });

    it('treats private ranges and compose hostnames as internal', () => {
        expect(isExternalLlmEndpoint('http://llm:8080/v1')).toBe(false);
        expect(isExternalLlmEndpoint('http://10.0.0.5:8080/v1')).toBe(false);
        expect(isExternalLlmEndpoint('http://192.168.1.10:8080/v1')).toBe(false);
        expect(isExternalLlmEndpoint('http://172.16.3.7:8080/v1')).toBe(false);
        expect(isExternalLlmEndpoint('http://172.31.255.1:8080/v1')).toBe(false);
    });

    it('treats public hosts and 172.32+ as external', () => {
        expect(isExternalLlmEndpoint('https://api.example.com/v1')).toBe(true);
        expect(isExternalLlmEndpoint('http://172.32.0.1:8080/v1')).toBe(true);
        expect(isExternalLlmEndpoint('http://8.8.8.8/v1')).toBe(true);
    });

    it('treats unparsable urls as external (fail closed)', () => {
        expect(isExternalLlmEndpoint('not a url')).toBe(true);
    });
});

describe('websocket event guards', () => {
    it('accepts only soc_result shapes', () => {
        expect(isSocResultEvent({ type: 'soc_result', stream_id: 1 })).toBe(true);
        expect(isSocResultEvent({ type: 'soc_triage', stream_id: 1 })).toBe(false);
        expect(isSocResultEvent(null)).toBe(false);
        expect(isSocResultEvent('soc_result')).toBe(false);
    });

    it('accepts only soc_triage shapes', () => {
        expect(
            isSocTriageEvent({ type: 'soc_triage', stream_id: 1, verdict: 'likely_attack' })
        ).toBe(true);
        expect(isSocTriageEvent({ type: 'ai_verdict', stream_id: 1 })).toBe(false);
        expect(isSocTriageEvent(undefined)).toBe(false);
    });
});

describe('socVerdictClass', () => {
    it('maps known verdicts and falls back to unknown', () => {
        expect(socVerdictClass('likely_attack')).toBe('soc-verdict-likely_attack');
        expect(socVerdictClass('benign')).toBe('soc-verdict-benign');
        expect(socVerdictClass('totally_new_class')).toBe('soc-verdict-unknown');
    });
});

describe('evidence item shape', () => {
    it('carries exactly one id kind per item', () => {
        const items: SocEvidenceItem[] = [
            { packet_id: 10, explanation: 'x' },
            { message_id: 3, explanation: 'y' },
        ];
        for (const item of items) {
            const kinds = [item.packet_id != null, item.message_id != null].filter(Boolean);
            expect(kinds.length).toBe(1);
        }
    });
});
