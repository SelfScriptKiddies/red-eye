// Exact User-Agent drill-down between the listing drawer and PacketsPage.
// The complete filter state travels in the URL so reload and sharing
// preserve it (the PacketsPage boot reads the same parameters).

export interface UaDrilldown {
    /** Header name to search; always the canonical casing "User-Agent". */
    name: string;
    /** Exact, case-sensitive header value (never the overflow marker). */
    value: string;
    serviceId: number;
    agentId?: string;
}

export function buildUaDrilldownUrl(drilldown: UaDrilldown): string {
    const params = new URLSearchParams();
    params.set('header_name', drilldown.name);
    params.set('header_value', drilldown.value);
    params.set('service_id', String(drilldown.serviceId));
    if (drilldown.agentId) params.set('agent_id', drilldown.agentId);
    return `/packets?${params.toString()}`;
}

/**
 * Parse the drill-down parameters from a query string. Returns null when
 * any required part is missing or malformed (no partial fallback), so the
 * caller can ignore a non-drill-down URL.
 */
export function parseUaDrilldown(search: string | URLSearchParams): UaDrilldown | null {
    const params = search instanceof URLSearchParams ? search : new URLSearchParams(search);
    const name = params.get('header_name');
    const value = params.get('header_value');
    const serviceIdRaw = params.get('service_id');
    const agentId = params.get('agent_id') ?? undefined;
    if (!name || value == null || !serviceIdRaw) return null;
    const serviceId = Number(serviceIdRaw);
    if (!Number.isInteger(serviceId) || serviceId <= 0) return null;
    const trimmedValue = value.trim();
    if (!trimmedValue) return null;
    return {
        name,
        value: trimmedValue,
        serviceId,
        agentId: agentId && agentId.trim() ? agentId.trim() : undefined,
    };
}
