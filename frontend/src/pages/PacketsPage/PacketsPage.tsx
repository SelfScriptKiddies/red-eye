import { ReactElement } from "react";
import './PacketsPage.css'
import { useState, useEffect } from 'react';
import { StreamCard } from '../../components/StreamCard'
import { ServiceCard } from '../../components/ServiceCard'

export function PacketsPage() {
    const [streamsArray, setStreams] = useState([]);
    const [servicesArray, setServices] = useState([]);
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
    const getStreams = async () =>
        {
            let data = await getUrl("/api/stream?limit=20");
            // console.log(data);
            await setStreams(Object.values(data));
        };
    const getServices = async () =>
        {
            let data = await getUrl("/api/service");
            console.log(data);
            await setServices(Object.values(data));
        };
    if (!streamsArray.length)
        getStreams();
    if (!servicesArray.length)
        getServices();
    return (
        <div className="packetsPage">
            <div className="filterTape">
                <div className="tag-filter"></div>
                <div className="service-filter d-inline-block">
                    {servicesArray.map((service, index) => {
                        return < ServiceCard service={service} />
                    })}
                </div>
            </div>
            <div id="streams" className="streams">
                {streamsArray.map((stream, index) => {
                    return < StreamCard streamObj={stream} />
                })}
                {/* <button id="toTagsPageBtn" className="side-btn bottom-side-btn"><i className="fa-solid fa-arrow-down"></i></button> */}
            </div>
            <div className="packetData">
            </div>
        </div>
    );
}