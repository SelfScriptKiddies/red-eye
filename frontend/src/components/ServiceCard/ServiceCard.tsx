import { useState } from "react";
import "./ServiceCard.css"

export const ServiceCard = ({service, onTagClick}) => {
    const [isActive, setActive] = useState(false);

    const getUrl = async (url) => {
        const data = await fetch(url, {
            method: 'GET'
        })
            .catch((reason) => {
                document.querySelector(".errorMessage").innerText = `Fuck you, lol. Reason: ${reason}`;
            })
            .then((response) => {
                if (parseInt(response.status / 100) == 5) {
                    document.querySelector(".errorMessage").innerText = "Что-то с сервером";
                    return;
                }
                else
                    return response.json();
            });
        return data;
    };


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
    console.log(service)
    return (
        <div className="service-card d-inline-block" onClick={clickHandler} data-service={service.id}>
            {service.name}
        </div>
    );
};