import { useState, useEffect, useRef } from 'react';
import { subscribeWs } from '../../lib/wsBus';
import { isAiVerdictEvent } from '../../lib/aiVerdict';
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
    detail: string;
    link: string;
    timestamp: number;
}

export default function AlertToast() {
    const [toasts, setToasts] = useState<Toast[]>([]);
    const nextId = useRef(0);

    const pushToast = (tag_name: string, detail: string, link: string) => {
        const toast: Toast = {
            id: nextId.current++,
            tag_name,
            detail,
            link,
            timestamp: Date.now(),
        };
        setToasts(prev => [...prev.slice(-9), toast]); // Keep max 10
        // Auto-remove after 6 seconds
        setTimeout(() => {
            setToasts(prev => prev.filter(t => t.id !== toast.id));
        }, 6000);
    };

    useEffect(() => {
        // Reuse the shared per-tab /api/ws socket (wsBus) instead of opening a
        // second connection. Unsubscribing on unmount drops the refcount; the
        // old code's onclose → setTimeout(connect) kept reconnecting forever
        // after unmount (zombie socket).
        const unsubscribe = subscribeWs({
            onMessage: (data) => {
                const alert = data as AlertEvent;
                if (alert && alert.type === 'alert') {
                    pushToast(
                        alert.tag_name,
                        `стрим #${alert.stream_id} — пакет #${alert.packet_id}`,
                        `/packets?stream=${alert.stream_id}`
                    );
                    return;
                }
                // JEV attack verdicts reuse the toast; non-attack verdicts
                // stay badge-only (they never reach this branch). The link
                // pins the exact verdict revision in the URL.
                if (isAiVerdictEvent(data) && data.is_attack) {
                    pushToast(
                        data.attack_class,
                        `JEV: стрим #${data.stream_id} — ревизия до пакета #${data.last_packet_id}`,
                        `/packets?stream=${data.stream_id}&verdict=${data.verdict_id}`
                    );
                }
            },
        });
        return unsubscribe;
    }, []);

    const removeToast = (id: number) => {
        setToasts(prev => prev.filter(t => t.id !== id));
    };

    if (toasts.length === 0) return null;

    return (
        <div className="alert-toast-container">
            {toasts.map(t => (
                <div key={t.id} className="alert-toast" onClick={() => {
                    window.location.href = t.link;
                }}>
                    <div className="alert-toast-icon">!</div>
                    <div className="alert-toast-content">
                        <span className="alert-toast-tag">{t.tag_name}</span>
                        <span className="alert-toast-detail">{t.detail}</span>
                    </div>
                    <button className="alert-toast-close" onClick={(e) => { e.stopPropagation(); removeToast(t.id); }}>✕</button>
                </div>
            ))}
        </div>
    );
}
