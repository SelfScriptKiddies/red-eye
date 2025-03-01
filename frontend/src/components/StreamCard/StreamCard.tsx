import "./StreamCard.css"
import moment from 'moment';
import { StreamSign } from "../StreamSign"
export function StreamCard({ streamObj, onStreamClick }) {
    let containsFlag = streamObj.tags.includes("flag") || streamObj.tags.includes("Flag");
    const clickHandler = (event) => {
        let e = event.target;
        while(!e.dataset.streamid)
        {
            e = e.parentElement;
        }
        onStreamClick(e.dataset.streamid);
    }
    return (
        <div className="stream-card d-flex jc-sb" onClick={clickHandler} data-streamid={streamObj.id}>
            <div className="row">
                <p>
                    <StreamSign incoming={streamObj.incoming} containsFlag={containsFlag} />
                    {streamObj.tags.map((tagName, index) => {
                        return <span className={"tag noselect " + tagName}>{tagName}</span>
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