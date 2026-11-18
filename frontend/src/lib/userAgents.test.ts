import { describe, expect, it } from 'vitest';
import { buildUaDrilldownUrl, parseUaDrilldown } from './userAgents';

describe('ua drilldown url roundtrip', () => {
    it('builds and parses a complete filter state', () => {
        const url = buildUaDrilldownUrl({
            name: 'User-Agent',
            value: 'curl/8.0',
            serviceId: 7,
            agentId: 'vuln-1',
        });
        expect(url).toBe(
            '/packets?header_name=User-Agent&header_value=curl%2F8.0&service_id=7&agent_id=vuln-1'
        );
        const parsed = parseUaDrilldown(url.slice('/packets'.length));
        expect(parsed).toEqual({
            name: 'User-Agent',
            value: 'curl/8.0',
            serviceId: 7,
            agentId: 'vuln-1',
        });
    });

    it('preserves exact and special-character values through encoding', () => {
        const value = '  Mozilla/5.0 (X11; Linux) a=b&c=d + "?=" ';
        const parsed = parseUaDrilldown(
            buildUaDrilldownUrl({ name: 'User-Agent', value, serviceId: 1 }).slice('/packets'.length)
        );
        // The URL carries the value verbatim (decoded); trimming for the
        // search happens in parse, matching the server's exact compare.
        expect(parsed?.value).toBe(value.trim());
    });

    it('rejects missing or malformed state without a partial fallback', () => {
        expect(parseUaDrilldown('?header_name=User-Agent&header_value=x')).toBeNull();
        expect(parseUaDrilldown('?header_name=User-Agent&service_id=7')).toBeNull();
        expect(parseUaDrilldown('?header_value=x&service_id=7')).toBeNull();
        expect(
            parseUaDrilldown('?header_name=User-Agent&header_value=x&service_id=abc')
        ).toBeNull();
        expect(
            parseUaDrilldown('?header_name=User-Agent&header_value=x&service_id=0')
        ).toBeNull();
        expect(parseUaDrilldown('?header_name=User-Agent&header_value= &service_id=7')).toBeNull();
        // Blank agent_id is dropped rather than sent as a filter.
        expect(
            parseUaDrilldown('?header_name=User-Agent&header_value=x&service_id=7&agent_id= ')
        )?.toEqual({ name: 'User-Agent', value: 'x', serviceId: 7, agentId: undefined });
    });
});
