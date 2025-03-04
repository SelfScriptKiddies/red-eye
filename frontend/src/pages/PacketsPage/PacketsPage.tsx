import { ReactElement } from "react";
import './PacketsPage.css'
import { useState, useEffect } from 'react';
import { StreamCard } from '../../components/StreamCard'
import { ServiceCard } from '../../components/ServiceCard'
import { PacketCard } from '../../components/PacketCard';

export function PacketsPage() {
    const [streamsArray, setStreams] = useState([]);
    const [servicesArray, setServices] = useState([]);
    const [tagsArray, setTags] = useState([]);
    const [packetData, setPacketData] = useState(null);
    
    // Получаем состояние RedEye из localStorage
    const isRedEyeActive = localStorage.getItem('redEyeActive') === 'true';

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
    const getStreams = async () => {
        let data = await getUrl("/api/stream?limit=20");
        // console.log(data);
        await setStreams(Object.values(data));
    };
    const getServices = async () => {
        let data = await getUrl("/api/service");
        await setServices(Object.values(data));
    };
    const getTags = async () => {
        let data = await getUrl("/api/tag/names");
        await setTags(Object.values(data));
    };
    if (!streamsArray.length && !isRedEyeActive)
        getStreams();
    if (!servicesArray.length && !isRedEyeActive)
        getServices();
    if (!tagsArray.length && !isRedEyeActive)
        getTags();

    const handleStreamClick = async (streamId) => {
        try {
            const data = await getUrl(`/api/packet/stream_id/${streamId}`);
            await setPacketData(Object.values(data));
        } catch (error) {
            console.error('Ошибка при получении данных пакета:', error);
        }
    };

    return (
        <div className="packetsPage">
            <div className="filterTape">
                <div className="tag-filter d-inline-block">
                    {tagsArray.map((service, index) => {
                        return < ServiceCard service={service} onClick={() => {}}/>
                    })}
                    </div>
                <div className="service-filter d-inline-block">
                    {servicesArray.map((service, index) => {
                        return < ServiceCard service={service} onClick={() => {}}/>
                    })}
                </div>
            </div>
            <div id="streams" className="streams">
                {streamsArray.map((stream, index) => {
                    return < StreamCard 
                        streamObj={stream} 
                        onStreamClick={handleStreamClick}
                    />
                })}
            </div>
            <div className="packetData">
                {packetData && packetData.sort((a, b) => a.timestamp - b.timestamp).map(packet => (
                    <PacketCard 
                        key={packet.id}
                        {...packet}
                    />
                ))}
            </div>
        </div>
    );
}