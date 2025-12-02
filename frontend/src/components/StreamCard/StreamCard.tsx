import "./StreamCard.css"
import moment from 'moment';
import { StreamSign } from "../StreamSign"
export function StreamCard({ streamObj, onStreamClick, tagColors = {} }) {
    let containsFlag = streamObj.tags.includes("flag") || streamObj.tags.includes("Flag");
    const clickHandler = (event) => {
        let e = event.target;
        while(!e.dataset.streamid)
        {
            e = e.parentElement;
        }
        onStreamClick(e.dataset.streamid);
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
                <p>
                    <StreamSign incoming={streamObj.incoming} containsFlag={containsFlag} />
                    {streamObj.tags.map((tagName, index) => {
                        const tagColor = getTagColor(tagName);
                        return (
                            <span 
                                key={index}
                                className="tag noselect" 
                                style={{
                                    color: 'white',
                                    textShadow: `0 0 5px ${tagColor}, 0 0 5px ${tagColor}, 0 0 5px ${tagColor}`
                                }}
                            >
                                {tagName}
                            </span>
                        );
                    })}
                </p>
            </div>
            <div className="info-block column">
                <span className="stream-time">{moment.unix(streamObj.first_packet_time).format("HH:mm:ss")}-{moment.unix(streamObj.last_packet_time).format("HH:mm:ss")}</span>
                <span className="stream-packet-count">{streamObj.packet_count || 0} {getPacketWord(streamObj.packet_count)}</span>
            </div>
        </div>
    );
}
