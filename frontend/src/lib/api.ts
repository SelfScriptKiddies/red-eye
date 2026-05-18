import { ApiError, RetryableError } from './queryClient';

export async function apiFetch<T>(url: string, signal?: AbortSignal): Promise<T> {
    const response = await fetch(url, { signal });
    if (response.status === 503) {
        const retryAfter = response.headers.get('Retry-After');
        throw new RetryableError(503, retryAfter ? parseInt(retryAfter) * 1000 : 5000);
    }
    if (!response.ok) {
        throw new ApiError(response.status);
    }
    return response.json();
}

export function copyText(text: string) {
    if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(text);
    } else {
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
    }
}
