// Filter sanity-check helpers: nonce generation, shell-safe curl
// generation and scoped result links. The backend never executes the
// generated command (SSRF-by-design avoidance): the operator copies and
// runs it from a suitable host.

export interface FilterLike {
    id?: number;
    template_type: string;
    template: string;
    expression_source?: string | null;
    ip_network?: string | null;
}

export interface CurlTarget {
    agentId: string;
    serviceName: string;
    serviceHost: string;
    servicePort: number;
    https: boolean;
}

/** Payload matchers accept a source-spoofing replay; IP and non-payload
 * expression matchers only get a matching-streams view. */
export function filterInspectsPayload(filter: FilterLike): boolean {
    return ['regex', 'substr', 'bsubstr', 'expression'].includes(filter.template_type);
}

/** Expression matchers that only look at non-payload fields cannot carry a
 * meaningful test payload: they get the streams-view treatment too. */
export function expressionUsesPayload(source: string): boolean {
    return /payload/.test(source);
}

/** Hex nonce: unique per test run, ASCII-safe in URLs and payloads. */
export function randomNonce(): string {
    const bytes = new Uint8Array(6);
    crypto.getRandomValues(bytes);
    return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
}

/** Append the nonce to the sample. Appending never changes whether the
 * sample itself matches, and the nonce makes the run findable. */
export function appendNonce(sample: string, nonce: string): string {
    return `${sample}nonce=${nonce}`;
}

/** POSIX shell single-quote escaping: close the quote, insert an escaped
 * quote, reopen. Round-trip safe for every byte except NUL. */
export function shellSingleQuote(arg: string): string {
    return `'${arg.replace(/'/g, `'\\''`)}'`;
}

/** Binary payload as an ANSI-C quoted shell word: $'\xNN...' */
export function shellAnsiCQuoted(bytes: Uint8Array): string {
    const body = Array.from(bytes, (b) => `\\x${b.toString(16).padStart(2, '0')}`).join('');
    return `$'${body}'`;
}

function decodeBsubstrHex(template: string): Uint8Array {
    const clean = template.trim().toLowerCase();
    const bytes = new Uint8Array(clean.length / 2);
    for (let i = 0; i + 1 < clean.length + 1 && i * 2 + 1 < clean.length; i++) {
        bytes[i] = parseInt(clean.slice(i * 2, i * 2 + 2), 16);
    }
    return bytes;
}

export interface SanityResult {
    curl: string;
    resultLink: string;
    nonce: string;
    startedAtMs: number;
    /** Proxy-only blocking (block/block_connection) cannot be verified by
     * a passive capture replay - the UI must say so. */
    note?: string;
}

/** Build the replay command and scoped result link for one content
 * filter. Pure string work: display and copy only. */
export function buildSanityCheck(
    filter: FilterLike,
    sample: string,
    target: CurlTarget,
    baseUrl: string
): SanityResult {
    const nonce = randomNonce();
    const startedAtMs = Date.now();
    const payload = appendNonce(sample, nonce);

    let bodyArg: string;
    if (filter.template_type === 'bsubstr') {
        const bytes = decodeBsubstrHex(filter.template);
        const withNonce = new Uint8Array(bytes.length + payload.length);
        withNonce.set(bytes, 0);
        for (let i = 0; i < payload.length; i++) {
            withNonce[bytes.length + i] = payload.charCodeAt(i);
        }
        bodyArg = shellAnsiCQuoted(withNonce);
    } else {
        bodyArg = shellSingleQuote(payload);
    }

    const url = `${target.https ? 'https' : 'http'}://${target.serviceHost}:${target.servicePort}/`;
    const curl = [
        'curl -sk',
        `-X POST`,
        `-H ${shellSingleQuote('Content-Type: text/plain')}`,
        `--data-binary ${bodyArg}`,
        shellSingleQuote(url),
    ].join(' ');

    const params = new URLSearchParams({
        filter_id: String(filter.id ?? ''),
        agent_id: target.agentId,
        service: target.serviceName,
        nonce,
        since: String(Math.floor(startedAtMs / 1000)),
    });
    const resultLink = `${baseUrl}/packets?${params.toString()}`;

    return {
        curl,
        resultLink,
        nonce,
        startedAtMs,
    };
}

/** Scoped view for IP/CIDR and non-payload expression filters: the
 * matching-streams query. The scope is explained next to the link. */
export function buildStreamsScopeLink(
    filter: FilterLike,
    agentId: string | null,
    baseUrl: string
): { link: string; explanation: string } {
    const params = new URLSearchParams({ filter_id: String(filter.id ?? '') });
    if (agentId) params.set('agent_id', agentId);
    if (filter.template_type === 'ip_cidr') {
        params.set('ip', filter.ip_network ?? '');
        return {
            link: `${baseUrl}/streams?${params.toString()}`,
            explanation:
                'Откроется представление потоков для этого IP/CIDR: совпадения ищутся по адресу источника или назначения пакета (не по содержимому). Replay-запрос для IP-правил невозможен - подмена источника проверяется только живым трафиком.',
        };
    }
    return {
        link: `${baseUrl}/streams?${params.toString()}`,
        explanation:
            'Выражение не проверяет содержимое пакета: откроется представление потоков, совпавших с фильтром в выбранной области.',
    };
}

export interface ThresholdForm {
    threshold_count: number | null;
    threshold_window_secs: number | null;
    threshold_group_by: string | null;
    threshold_cooldown_secs: number | null;
    threshold_block_secs: number | null;
}

/** Thresholds are valid only for alert and block actions; tag and flag
 * reject them. Block requires a positive block duration. */
export function thresholdAction(action: string): boolean {
    return action === 'alert' || action === 'block';
}

/** Client-side validation mirroring the backend rules: matcher-specific
 * fields, action combos and threshold completeness. */
export function validateFilterForm(form: {
    name: string;
    template_type: string;
    template: string;
    direction: string;
    action: string;
    ip_network: string | null;
    ip_endpoint: string | null;
    expression_source: string | null;
    service_targets_global: boolean;
    service_ids: number[];
    agent_targets_global: boolean;
    agent_ids: string[];
} & ThresholdForm): string | null {
    if (!form.name.trim()) return 'Укажите название фильтра';
    if (!['in', 'out', 'both'].includes(form.direction)) return 'Неверное направление';

    const threshold = thresholdAction(form.action);
    const anyThreshold =
        form.threshold_count != null ||
        form.threshold_window_secs != null ||
        form.threshold_group_by != null ||
        form.threshold_block_secs != null;

    switch (form.template_type) {
        case 'ip_cidr':
            if (!form.ip_network?.trim()) return 'Укажите IP или CIDR (например 10.0.0.5 или 10.0.0.0/8)';
            if (!['src', 'dst'].includes(form.ip_endpoint ?? '')) {
                return 'Выберите конечную точку: src или dst';
            }
            break;
        case 'expression':
            if (!form.expression_source?.trim()) return 'Укажите выражение';
            break;
        default:
            if (!form.template.trim()) return 'Укажите шаблон';
            if (form.template.length > 255) return 'Шаблон длиннее 255 символов';
            if (form.template_type === 'bsubstr' && !/^[0-9a-fA-F]*$/.test(form.template.trim())) {
                return 'bsubstr ожидает HEX (например deadbeef)';
            }
            if (form.template_type === 'bsubstr' && form.template.trim().length % 2 !== 0) {
                return 'HEX должен иметь чётную длину';
            }
    }

    if (!threshold && anyThreshold) {
        return 'Порог (threshold) допустим только для действий alert и block';
    }
    if (threshold && anyThreshold) {
        if (!form.threshold_count || form.threshold_count <= 0) return 'Порог: count должен быть положительным';
        if (!form.threshold_window_secs || form.threshold_window_secs <= 0) {
            return 'Порог: окно должно быть положительным';
        }
        if (!['remote_ip', 'stream', 'service'].includes(form.threshold_group_by ?? '')) {
            return 'Порог: выберите группировку';
        }
        if (form.action === 'block' && (!form.threshold_block_secs || form.threshold_block_secs <= 0)) {
            return 'Порог block требует длительность блокировки (сек)';
        }
    }
    return null;
}
