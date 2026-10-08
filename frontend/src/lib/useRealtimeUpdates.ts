import { useWebSocket } from './useWebSocket';
import { useConnectionStatus } from './useConnectionStatus';

export function useRealtimeUpdates(
    enabled: boolean,
    onPatchStreams?: (_patches: Map<number, any>) => void,
    selectedStreamIds?: number[],
) {
    const wsState = useWebSocket(enabled, onPatchStreams, selectedStreamIds);
    const connectionStatus = useConnectionStatus(wsState);
    return connectionStatus;
}
