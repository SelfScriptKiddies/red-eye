export async function fetchApi<T = any>(url: string): Promise<T | undefined> {
    try {
        const response = await fetch(url);
        if (Math.floor(response.status / 100) === 5) {
            console.error('Server error:', response.status);
            return undefined;
        }
        return await response.json();
    } catch (error) {
        console.error('Fetch error:', error);
        return undefined;
    }
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
