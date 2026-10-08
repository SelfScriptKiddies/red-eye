import { useWebSocket, type ActivityFlush } from './useWebSocket';
import { useConnectionStatus } from './useConnectionStatus';

export function useRealtimeUpdates(
    enabled: boolean,
    onActivity?: (_flush: ActivityFlush) => void,
    selectedStreamIds?: number[],
) {
    const wsState = useWebSocket(enabled, onActivity, selectedStreamIds);
    const connectionStatus = useConnectionStatus(wsState);
    return connectionStatus;
}
