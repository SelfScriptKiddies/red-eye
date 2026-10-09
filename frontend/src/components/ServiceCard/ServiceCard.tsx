import { useState } from "react";
import "./ServiceCard.css"

export const ServiceCard = ({service, onTagClick}) => {
    const [isActive, setActive] = useState(false);

    let clickHandler = (e) => {
        setActive((isActive) => !isActive);
        
        // console.log("Changes active status in card ", e.tartget, " changed to:", isActive);
        if(!isActive)
        {
            e.target.classList.add('service-choosen');
        }
        else {
            e.target.classList.remove('service-choosen');
        }
        onTagClick(e);
    };
    return (
        <div className="service-card d-inline-block" onClick={clickHandler} data-service={service.id}>
            {service.name}
        </div>
    );
};