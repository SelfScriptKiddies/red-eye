import "./StreamArrow.css"

export const StreamArrow = (props) => {
    let className = "fa-solid stream-arrow ";
    if (props.incoming)
        className += "fa-arrow-down ";
    else
        className += "fa-arrow-up ";

    if(props.containsFlag)
        className += "danger";
    else
        className += "common";
    return (
        <i className={className}/>
    );
};