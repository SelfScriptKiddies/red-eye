import "./StreamCard.css"
import moment from 'moment';
import { StreamSign } from "../StreamSign"
export function StreamCard({ streamObj, onStreamClick, onFavoriteToggle, tagColors = {} }) {
    let containsFlag = streamObj.tags.includes("flag") || streamObj.tags.includes("Flag");
    const isFavorite = !!streamObj.favorite;
    const clickHandler = (event) => {
        let e = event.target;
        while(!e.dataset.streamid)
        {
            e = e.parentElement;
        }
        onStreamClick(e.dataset.streamid);
    }

    const handleFavoriteClick = (event) => {
        event.stopPropagation();
        onFavoriteToggle(streamObj.id, isFavorite);
    }
    
    const getTagColor = (tagName) => {
        const hue = tagColors[tagName];
        if (hue !== undefined) {
            return `hsl(${hue}, 100%, 65%)`; // Lighter version (65% instead of 50%)
        }
        return 'hsl(330, 100%, 65%)'; // Default pink color if not found
    };
    
    const getPacketWord = (count) => {
        const num = count || 0;
        const lastDigit = num % 10;
        const lastTwoDigits = num % 100;
        
        // 11-14 всегда "пакетов"
        if (lastTwoDigits >= 11 && lastTwoDigits <= 14) {
            return 'пакетов';
        }
        
        // 1, 21, 31, ... -> "пакет"
        if (lastDigit === 1) {
            return 'пакет';
        }
        
        // 2, 3, 4, 22, 23, 24, ... -> "пакета"
        if (lastDigit >= 2 && lastDigit <= 4) {
            return 'пакета';
        }
        
        // 5, 6, 7, 8, 9, 0, 25, 26, ... -> "пакетов"
        return 'пакетов';
    };
    
    return (
        <div className="stream-card d-flex jc-sb" onClick={clickHandler} data-streamid={streamObj.id}>
            <div className="row">
                <button 
                    className={`favorite-btn ${isFavorite ? 'favorite-btn--active' : ''}`}
                    aria-label={isFavorite ? 'Remove from favorites' : 'Add to favorites'}
                    onClick={handleFavoriteClick}
                >
                    <svg width="18" height="18" viewBox="0 0 24 24" fill={isFavorite ? 'currentColor' : 'none'} stroke="currentColor" strokeWidth="2" strokeLinejoin="round">
                        <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>
                    </svg>
                </button>
                <p>
                    <StreamSign incoming={streamObj.incoming} containsFlag={containsFlag} />
                    {(() => {
                        // Aggregate tag counts
                        const counts: Record<string, number> = {};
                        for (const t of streamObj.tags) counts[t] = (counts[t] || 0) + 1;
                        return Object.entries(counts).map(([tagName, count]) => {
                            const tagColor = getTagColor(tagName);
                            return (
                                <span
                                    key={tagName}
                                    className="tag noselect"
                                    style={{
                                        color: 'white',
                                        textShadow: `0 0 5px ${tagColor}, 0 0 5px ${tagColor}, 0 0 5px ${tagColor}`
                                    }}
                                >
                                    {tagName}{count > 1 ? ` ×${count}` : ''}
                                </span>
                            );
                        });
                    })()}
                </p>
            </div>
            <div className="info-block column">
                {streamObj.dst_ip && <span className="stream-ip">{streamObj.dst_ip}</span>}
                <span className="stream-time">{moment.unix(streamObj.first_packet_time).format("HH:mm:ss")}-{moment.unix(streamObj.last_packet_time).format("HH:mm:ss")}</span>
                <span className="stream-packet-count">{streamObj.packet_count || 0} {getPacketWord(streamObj.packet_count)}</span>
            </div>
        </div>
    );
}
