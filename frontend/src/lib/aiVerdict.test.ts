import { describe, expect, it } from 'vitest';
import {
    formatVerdictTime,
    isAiVerdictEvent,
    severityLabel,
    topNouls,
    verdictBadgeClass,
    type AiVerdict,
} from './aiVerdict';

describe('isAiVerdictEvent', () => {
    it('accepts the ws contract frame', () => {
        const frame = {
            type: 'ai_verdict',
            verdict_id: 1,
            stream_id: 10,
            last_packet_id: 100,
            is_attack: true,
            attack_class: 'sqli',
            confidence: 0.88,
            severity: 2.7,
            top_nouls: { is_sql_injection: 0.93 },
        };
        expect(isAiVerdictEvent(frame)).toBe(true);
    });

    it('rejects other frame types and garbage', () => {
        expect(isAiVerdictEvent({ type: 'alert' })).toBe(false);
        expect(isAiVerdictEvent({ type: 'activity' })).toBe(false);
        expect(isAiVerdictEvent(null)).toBe(false);
        expect(isAiVerdictEvent('ai_verdict')).toBe(false);
        expect(isAiVerdictEvent(undefined)).toBe(false);
    });
});

describe('verdictBadgeClass', () => {
    it('maps every known class to its own badge', () => {
        for (const cls of [
            'sqli', 'xss', 'command_injection', 'path_traversal', 'scanner',
            'bruteforce', 'exfil', 'exploit_attempt', 'benign', 'other',
        ]) {
            expect(verdictBadgeClass(cls)).toBe(`jev-badge-${cls}`);
        }
    });

    it('falls back to other for unknown provider classes', () => {
        expect(verdictBadgeClass('rootkit')).toBe('jev-badge-other');
        expect(verdictBadgeClass('<script>')).toBe('jev-badge-other');
        expect(verdictBadgeClass('')).toBe('jev-badge-other');
    });
});

describe('severityLabel', () => {
    it('clamps to the four rubric levels', () => {
        expect(severityLabel(0)).toBe('benign');
        expect(severityLabel(0.5)).toBe('suspicious');
        expect(severityLabel(2.4)).toBe('malicious');
        expect(severityLabel(2.7)).toBe('active exploit');
        expect(severityLabel(3)).toBe('active exploit');
        expect(severityLabel(-1)).toBe('benign');
        expect(severityLabel(99)).toBe('active exploit');
    });
});

describe('topNouls', () => {
    it('returns at most three sorted descending with name tiebreak', () => {
        const top = topNouls({
            is_xss: 0.6,
            is_sql_injection: 0.9,
            is_scanner_probe: 0.1,
            is_exfiltration: 0.75,
            is_path_traversal: 0.75,
        });
        expect(top.map(([name]) => name)).toEqual([
            'is_sql_injection',
            'is_exfiltration',
            'is_path_traversal',
        ]);
    });

    it('handles empty maps and short maps', () => {
        expect(topNouls({}, 3)).toEqual([]);
        expect(topNouls({ is_xss: 0.4 }, 3)).toEqual([['is_xss', 0.4]]);
    });
});

describe('formatVerdictTime', () => {
    it('renders empty for zero', () => {
        expect(formatVerdictTime(0)).toBe('');
    });

    it('renders a locale time string for a timestamp', () => {
        expect(formatVerdictTime(1_700_000_000)).toBeTruthy();
    });
});

describe('AiVerdict shape', () => {
    it('carries the audit fields the spec requires', () => {
        const v: AiVerdict = {
            id: 1,
            stream_id: 10,
            first_packet_id: 90,
            last_packet_id: 100,
            model: 'typesafe/jev-1.13',
            prompt_version: 1,
            transcript_sha256: 'ab'.repeat(32),
            attack_class: 'sqli',
            confidence: 0.88,
            severity: 2.7,
            is_attack: true,
            ai_threshold: 0.7,
            answers: {
                nouls: { is_sql_injection: 0.93 },
                attack_class: { choice: 'sqli', confidence: 0.88 },
                severity: { score: 2.7, confidence: 0.9 },
            },
            input_tokens: 275,
            cost: 0.00003,
            analyzed_packet_count: 9,
            omitted_packet_count: 1,
            omitted_byte_count: 64,
            truncated: true,
            created_at_unix: 1_700_000_000,
        };
        expect(v.is_attack).toBe(true);
        expect(v.truncated).toBe(true);
        expect(v.last_packet_id).toBe(100);
    });
});
