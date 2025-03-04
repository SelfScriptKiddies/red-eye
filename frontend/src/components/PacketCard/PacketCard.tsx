import React from 'react';
import './PacketCard.css';

interface PacketProps {
    direction: string;
    id: number;
    packet_data: string;
    stream_id: number;
    timestamp: number;
}

export function PacketCard({ direction, id, packet_data, stream_id, timestamp }: PacketProps) {
    const decodedData = atob(packet_data);
    const formattedTime = new Date(timestamp).toLocaleTimeString();
    
    return (
        <div className={`packet-card ${direction === 'orig' ? 'packet-orig' : 'packet-dest'}`}>
            <div className="packet-header">
                {/* <span className="packet-id">#{id}</span> */}
                <span className="packet-time">{formattedTime}</span>
                <span className={`packet-direction ${direction}`}>
                    {direction === 'orig' ? '← От сервера' : '→ К серверу'}
                </span>
            </div>
            <div className="packet-body">
                {decodedData}
            </div>
        </div>
    );
} 