import { ReactElement } from "react";
import './PacketsPage.css'
import { StreamCard } from '../../components/StreamCard'

export function PacketsPage() {
    fetch("/api/packet/stream_id/1",{
        method: 'GET'
    })
    .catch((reason) => {
        document.querySelector(".errorMessage").innerText = `Fuck you, lol. Reason: ${reason}`;
    })
    .then((response) => {
        if (parseInt(response.status / 100) == 5)
        {
            document.querySelector(".errorMessage").innerText = "Что-то с сервером";
            return;
        }
        else
            return response.json();
    })
    // let 
    // .catch((reason) => {
    //     document.querySelector(".errorMessage").innerText = `Fuck you, lolx2. Reason: ${reason}`;
    // })
    // .then((resp_json) => {
    //     if(!resp_json)
    //         return;
    //     ...
    // })
    // .catch((reason) => {
    //     document.querySelector(".errorMessage").innerText = `Fuck you, lolx3. Reason: ${reason}`;
    // });
    return (
        <div className="packetsPage">
            <div className="streamsTape"></div>
            <div className="streams">
                < StreamCard incoming={false} tags={["Flag", "LFI"]} />
                < StreamCard incoming={false} tags={["XSS"]} />
                < StreamCard incoming={true} tags={["SQLInj"]} />
                < StreamCard incoming={true} tags={["Flag", "RCE"]} />
            </div>
            <div className="streamsTags"></div>
        </div>
    );
}