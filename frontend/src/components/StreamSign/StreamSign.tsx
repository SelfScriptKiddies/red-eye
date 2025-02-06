import "./StreamSign.css"

export const StreamSign = (props) => {
    let className = "fa-solid ";
    if(props.containsFlag)
        className += "flag";
    return (
        <i className={className}/>
    );
};