import { ReactElement } from "react";
import './PacketsPage.css'
import { useState, useEffect } from 'react';
import { StreamCard } from '../../components/StreamCard'

export function PacketsPage() {
    const [streamsArray, setStreams] = useState([]);
    const getStreams = async () => {
        const data = await fetch("/api/stream", {
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
        console.log(data);
        setStreams(Object.values(data));
    };
    if(!streamsArray.length)
        getStreams();


    // let streams = [ // example
    //     {id: 1, incoming: false, tags: ["Flag", "LFI"]},
    //     {id: 2, incoming: false, tags: ["XSS"]},
    //     {id: 3, incoming: true, tags: ["SQLInj"]},
    //     {id: 4, incoming: true, tags: ["Flag", "RCE"]},
    //     {id: 1, incoming: false, tags: ["Flag", "LFI"]},
    //     {id: 2, incoming: false, tags: ["XSS"]},
    //     {id: 3, incoming: true, tags: ["SQLInj"]},
    //     {id: 4, incoming: true, tags: ["Flag", "RCE"]},
    //     {id: 1, incoming: false, tags: ["Flag", "LFI"]},
    //     {id: 2, incoming: false, tags: ["XSS"]},
    //     {id: 3, incoming: true, tags: ["SQLInj"]},
    //     {id: 4, incoming: true, tags: ["Flag", "RCE"]},
    //     {id: 1, incoming: false, tags: ["Flag", "LFI"]},
    //     {id: 2, incoming: false, tags: ["XSS"]},
    //     {id: 3, incoming: true, tags: ["SQLInj"]},
    //     {id: 4, incoming: true, tags: ["Flag", "RCE"]},
    // ];
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
            <div className="streamsTape">
                <button id="toTagsPageBtn" className="side-btn left-side-btn"><i className="fa-solid fa-arrow-down"></i></button>
            </div>
            <div id="streams" className="streams">
                {streamsArray.map((stream, index) => {
                    return < StreamCard streamObj={stream} />
                })}
                {/* <button id="toTagsPageBtn" className="side-btn bottom-side-btn"><i className="fa-solid fa-arrow-down"></i></button> */}
            </div>
            <div className="streamsTags">
                <button id="toTagsPageBtn" className="side-btn right-side-btn"><i className="fa-solid fa-arrow-down"></i></button>
            </div>
        </div>
    );
}