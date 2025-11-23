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
                                    textShadow: `0 0 5px ${tagColor}, 0 0 10px ${tagColor}, 0 0 15px ${tagColor}`
                                }}
                            >
                                {tagName}
                            </span>
                        );
                    })}
                </p>
            </div>
            <div className="info-block column">
                <span className="stream-time">{moment(Date(streamObj.first_packet_time)).format("HH:MM:ss")}-{moment(Date(streamObj.last_packet_time)).format("HH:MM:ss")}</span>
                <span className="stream-size"></span>
            </div>
        </div>
    );
}