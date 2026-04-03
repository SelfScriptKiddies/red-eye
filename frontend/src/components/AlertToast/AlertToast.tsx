import { useState, useEffect, useRef } from 'react';
import './AlertToast.css';

interface AlertEvent {
    type: string;
    tag_name: string;
    packet_id: number;
    stream_id: number;
}

interface Toast {
    id: number;
    tag_name: string;
    packet_id: number;
    stream_id: number;
    timestamp: number;
}

export default function AlertToast() {
    const [toasts, setToasts] = useState<Toast[]>([]);
    const wsRef = useRef<WebSocket | null>(null);
    const nextId = useRef(0);

    useEffect(() => {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/api/ws`;

        const connect = () => {
            const ws = new WebSocket(wsUrl);
            wsRef.current = ws;

            ws.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    if (data.type === 'alert') {
                        const alert = data as AlertEvent;
                        const toast: Toast = {
                            id: nextId.current++,
                            tag_name: alert.tag_name,
                            packet_id: alert.packet_id,
                            stream_id: alert.stream_id,
                            timestamp: Date.now(),
                        };
                        setToasts(prev => [...prev.slice(-9), toast]); // Keep max 10
                        // Auto-remove after 6 seconds
                        setTimeout(() => {
                            setToasts(prev => prev.filter(t => t.id !== toast.id));
                        }, 6000);
                    }
                } catch {}
            };

            ws.onclose = () => {
                setTimeout(connect, 3000);
            };
        };

        connect();

        return () => {
            if (wsRef.current) wsRef.current.close();
        };
    }, []);

    const removeToast = (id: number) => {
        setToasts(prev => prev.filter(t => t.id !== id));
    };

    if (toasts.length === 0) return null;

    return (
        <div className="alert-toast-container">
            {toasts.map(t => (
                <div key={t.id} className="alert-toast" onClick={() => {
                    window.location.href = `/packets?stream=${t.stream_id}`;
                }}>
                    <div className="alert-toast-icon">!</div>
                    <div className="alert-toast-content">
                        <span className="alert-toast-tag">{t.tag_name}</span>
                        <span className="alert-toast-detail">стрим #{t.stream_id} — пакет #{t.packet_id}</span>
                    </div>
                    <button className="alert-toast-close" onClick={(e) => { e.stopPropagation(); removeToast(t.id); }}>✕</button>
                </div>
            ))}
        </div>
    );
}
