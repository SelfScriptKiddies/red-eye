import "./StreamCard.css"
import { StreamArrow } from "../StreamArrow"
export const StreamCard = ({streamObj}) => {
    let containsFlag = streamObj.tags.includes("flag") || streamObj.tags.includes("Flag");
    const clickHandler = (event) => {
        if (event.detail >= 2)
            console.log(`CHANGE PAGE ON /streams/${event.target.dataset.streamid}`);
    }
    return (
        <div className="stream-card" onClick={clickHandler} data-streamid={streamObj.id}>
            <StreamArrow incoming={streamObj.incoming} containsFlag={containsFlag} />
            {streamObj.tags.map((tagName, index) => {
                return <div className={"tag noselect " + tagName}>{tagName}</div>
            })}
        </div>
    );
};