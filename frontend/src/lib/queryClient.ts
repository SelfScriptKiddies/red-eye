import { QueryClient } from '@tanstack/react-query';

export class ApiError extends Error {
    constructor(public status: number) {
        super(`API error: ${status}`);
    }
}

export class RetryableError extends ApiError {
    constructor(status: number, public retryAfterMs?: number) {
        super(status);
    }
}

export const queryClient = new QueryClient({
    defaultOptions: {
        queries: {
            staleTime: 3_000,
            gcTime: 60_000,
            retry: 3,
            retryDelay: (attempt, error) => {
                if (error instanceof RetryableError && error.retryAfterMs) {
                    return error.retryAfterMs;
                }
                return Math.min(1000 * 2 ** attempt, 10_000);
            },
        },
    },
});
