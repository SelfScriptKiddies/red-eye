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
    const [tagColors, setTagColors] = useState({});
    const [packetData, setPacketData] = useState(null);
    const [pageLoaded, setPageLoaded] = useState(null);
    const [usedTags, setUsedTags] = useState([]);
    const [usedServices, setUsedServices] = useState([]);
    
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
        const tagsData = Object.values(data);
        await setTags(tagsData);
        
        // Create a map of tag names to colors for easy lookup
        const colorMap = {};
        tagsData.forEach(tag => {
            colorMap[tag.name] = tag.color;
        });
        setTagColors(colorMap);
    };
    const getFilteredStreams = async () => {
        let req_url = "/api/stream?limit=20";
        // let usedTags = this.state.usedTags;
        if (usedTags)
            req_url += "&tags="+usedTags.join(',')
        if (usedServices)
            req_url += "&services="+usedServices.join(',')
        
        let data = await getUrl(req_url);
        await setStreams(Object.values(data));
    };
    if (!streamsArray.length && !isRedEyeActive)
    {
        if(!pageLoaded)
        {
            setPageLoaded(true);
            getStreams();
        }
    }
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

    const handleClearStreams = () => {
        setStreams([]);
    };
    const handleTagClick = (e) => {
        let tag_name = e.target.dataset.service;
        let tmp = usedTags;
        if(usedTags.includes(tag_name))
        {
            let idx_tag = tmp.indexOf(tag_name); 
            tmp.splice(idx_tag, 1); 
            console.log("NEW ARRAY:", tmp);
        }
        else
        {
            tmp.push(e.target.dataset.service);
        }
        setUsedTags(tmp);
        getFilteredStreams();
    };
    const handleServiceClick = (e) => {
        let service_name = e.target.dataset.service;
        let tmp = usedServices;
        if(usedServices.includes(service_name))
        {
            let idx_tag = tmp.indexOf(service_name); 
            tmp.splice(idx_tag, 1); 
            console.log("NEW ARRAY:", tmp);
        }
        else
        {
            tmp.push(e.target.dataset.service);
        }
        setUsedServices(tmp);
        getFilteredStreams();
    };

    return (
        <div className="packetsPage">
            <div className="filterTape">
                <button className="clear-button" onClick={handleClearStreams}>
                    Очистить очередь
                </button>
                <div className="tag-filter d-inline-block">
                    {tagsArray.map((service, index) => {
                        return < ServiceCard service={service} onTagClick={handleTagClick}/>
                    })}
                    </div>
                <div className="service-filter d-inline-block">
                    {servicesArray.map((service, index) => {
                        return < ServiceCard service={service} onTagClick={handleServiceClick}/>
                    })}
                </div>
            </div>
            <div id="streams" className="streams">
                {streamsArray.map((stream, index) => {
                    return < StreamCard 
                        streamObj={stream} 
                        onStreamClick={handleStreamClick}
                        tagColors={tagColors}
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