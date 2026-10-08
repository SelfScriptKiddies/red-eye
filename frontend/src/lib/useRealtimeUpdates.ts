import { useWebSocket } from './useWebSocket';
import { useConnectionStatus } from './useConnectionStatus';
import type { StreamPatch } from './activityEvents';

export function useRealtimeUpdates(
    enabled: boolean,
    onPatchStreams?: (_patches: Map<number, StreamPatch>) => void,
    selectedStreamIds?: number[],
) {
    const wsState = useWebSocket(enabled, onPatchStreams, selectedStreamIds);
    const connectionStatus = useConnectionStatus(wsState);
    return connectionStatus;
}
