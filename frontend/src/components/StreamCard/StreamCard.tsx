import "./StreamCard.css"
import { StreamArrow } from "../StreamArrow"
export const StreamCard = ({incoming, tags}) => {
    let containsFlag = tags.includes("flag") || tags.includes("Flag");
    return (
        <div className="stream-card">
            <StreamArrow incoming={incoming} containsFlag={containsFlag} />
            {tags.map((tagName, index) => {
                return <div className={"tag " + tagName}>{tagName}</div>
            })}
        </div>
    );
};