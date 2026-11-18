// Agent links: an operator copies a Red Eye selection as a URL an MCP
// agent consumes with get_selected_streams (?streams=1,2,3).

export const MAX_AGENT_LINK_IDS = 25;

export function parseStreamsParam(param: string | null | undefined): number[] {
    if (!param) return [];
    return param
        .split(',')
        .map(s => parseInt(s.trim()))
        .filter(n => Number.isFinite(n) && n > 0);
}

export function buildAgentLink(origin: string, ids: number[]): { url: string; truncated: boolean } {
    const capped = ids.slice(0, MAX_AGENT_LINK_IDS);
    return {
        url: `${origin}/packets?streams=${capped.join(',')}`,
        truncated: ids.length > MAX_AGENT_LINK_IDS,
    };
}
