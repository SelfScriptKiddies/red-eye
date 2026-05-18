import { useQuery } from '@tanstack/react-query';
import { apiFetch } from './api';

export function useHttpMessages(streamId: number | null) {
    return useQuery({
        queryKey: ['httpMessages', streamId],
        queryFn: ({ signal }) => apiFetch<any[]>(`/api/http/stream/${streamId}`, signal),
        enabled: streamId != null,
        staleTime: 10_000,
    });
}
