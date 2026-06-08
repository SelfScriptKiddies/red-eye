export function useConnectionStatus(wsState: 'connected' | 'connecting' | 'disconnected') {
    const isConnected = wsState === 'connected';
    return {
        isConnected,
        streamRefetchInterval: isConnected ? false as const : 5_000,
        statsRefetchInterval: isConnected ? false as const : 10_000,
        histogramRefetchInterval: 10_000,
    };
}
