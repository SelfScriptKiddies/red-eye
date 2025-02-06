import "./StreamCard.css"
import moment from 'moment';
import { StreamSign } from "../StreamSign"
export const StreamCard = ({streamObj}) => {
    let containsFlag = streamObj.tags.includes("flag") || streamObj.tags.includes("Flag");
    const clickHandler = (event) => {
        if (event.detail >= 2)
            console.log(`CHANGE PAGE ON /streams/${event.target.dataset.streamid}`);
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
};