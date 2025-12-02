import { ReactElement } from "react";
import './PacketsPage.css'
import { useState, useEffect, useRef } from 'react';
import { StreamCard } from '../../components/StreamCard'
import { ServiceCard } from '../../components/ServiceCard'
import { PacketCard } from '../../components/PacketCard';

interface PacketEvent {
    packet_id: number;
    stream_id: number;
    timestamp_seconds: number;
    timestamp_microseconds: number;
    direction: string;
}

export function PacketsPage() {
    const [streamsArray, setStreams] = useState([]);
    const [servicesArray, setServices] = useState([]);
    const [tagsArray, setTags] = useState([]);
    const [tagColors, setTagColors] = useState({});
    const [packetData, setPacketData] = useState<any[]>([]);
    const [pageLoaded, setPageLoaded] = useState(null);
    const [usedTags, setUsedTags] = useState([]);
    const [usedServices, setUsedServices] = useState([]);
    const [currentOffset, setCurrentOffset] = useState(0);
    const [isLoadingMore, setIsLoadingMore] = useState(false);
    const [hasMoreStreams, setHasMoreStreams] = useState(true);
    const wsRef = useRef<WebSocket | null>(null);
    const selectedStreamIdRef = useRef<number | null>(null);
    const streamsContainerRef = useRef<HTMLDivElement | null>(null);
    
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
        let data = await getUrl("/api/stream?limit=20&offset=0");
        // console.log(data);
        const streams = Object.values(data);
        await setStreams(streams);
        setCurrentOffset(20);
        setHasMoreStreams(streams.length === 20);
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
    const getFilteredStreams = async (reset: boolean = false) => {
        const offset = reset ? 0 : currentOffset;
        let req_url = `/api/stream?limit=20&offset=${offset}`;
        // let usedTags = this.state.usedTags;
        if (usedTags && usedTags.length > 0)
            req_url += "&tags="+usedTags.join(',')
        if (usedServices && usedServices.length > 0)
            req_url += "&services="+usedServices.join(',')
        
        setIsLoadingMore(true);
        try {
            let data = await getUrl(req_url);
            const newStreams = Object.values(data);
            
            if (reset) {
                await setStreams(newStreams);
                setCurrentOffset(20);
            } else {
                await setStreams((prevStreams: any[]) => [...prevStreams, ...newStreams]);
                setCurrentOffset(prev => prev + 20);
            }
            
            // If we got less than 20 streams, there are no more to load
            setHasMoreStreams(newStreams.length === 20);
        } catch (error) {
            console.error('Error loading streams:', error);
        } finally {
            setIsLoadingMore(false);
        }
    };
    
    // Handle scroll event for infinite scroll
    useEffect(() => {
        const handleScroll = () => {
            if (!streamsContainerRef.current || isLoadingMore || !hasMoreStreams || isRedEyeActive) return;
            
            const container = streamsContainerRef.current;
            const scrollTop = container.scrollTop;
            const scrollHeight = container.scrollHeight;
            const clientHeight = container.clientHeight;
            
            // Load more when user is within 200px of the bottom
            if (scrollTop + clientHeight >= scrollHeight - 200) {
                getFilteredStreams(false);
            }
        };
        
        const container = streamsContainerRef.current;
        if (container) {
            container.addEventListener('scroll', handleScroll);
            return () => container.removeEventListener('scroll', handleScroll);
        }
    }, [isLoadingMore, hasMoreStreams, isRedEyeActive, currentOffset, usedTags, usedServices]);
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

    // WebSocket connection for real-time packet updates
    useEffect(() => {
        if (isRedEyeActive) {
            return; // Don't connect if RedEye is not active
        }

        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/api/ws`;
        const ws = new WebSocket(wsUrl);
        wsRef.current = ws;

        ws.onopen = () => {
            console.log('WebSocket connected');
        };

        ws.onmessage = async (event) => {
            try {
                const packetEvent: PacketEvent = JSON.parse(event.data);
                console.log('Received packet event:', packetEvent);
                
                // Update streams list when new packet arrives
                // This will refresh the stream list to show updated timestamps and move active streams to top
                if (!isRedEyeActive) {
                    setCurrentOffset(0);
                    setHasMoreStreams(true);
                    getFilteredStreams(true);
                }
            } catch (error) {
                console.error('Error parsing WebSocket message:', error);
            }
        };

        ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };

        ws.onclose = () => {
            console.log('WebSocket disconnected, reconnecting...');
            // Reconnect after 3 seconds
            setTimeout(() => {
                if (!isRedEyeActive) {
                    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                    const wsUrl = `${protocol}//${window.location.host}/api/ws`;
                    wsRef.current = new WebSocket(wsUrl);
                }
            }, 3000);
        };

        return () => {
            if (wsRef.current) {
                wsRef.current.close();
                wsRef.current = null;
            }
        };
    }, [isRedEyeActive]);

    const handleStreamClick = async (streamId) => {
        selectedStreamIdRef.current = streamId;
        console.log('Stream selected:', streamId);
        try {
            const data = await getUrl(`/api/packet/stream_id/${streamId}`);
            const packets = Object.values(data);
            console.log('Loaded packets for stream:', packets.length);
            setPacketData(packets);
        } catch (error) {
            console.error('Ошибка при получении данных пакета:', error);
        }
    };

    const handleClearStreams = () => {
        setStreams([]);
        setCurrentOffset(0);
        setHasMoreStreams(true);
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
        setCurrentOffset(0);
        setHasMoreStreams(true);
        getFilteredStreams(true);
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
        setCurrentOffset(0);
        setHasMoreStreams(true);
        getFilteredStreams(true);
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
            <div id="streams" className="streams" ref={streamsContainerRef}>
                {streamsArray.map((stream, index) => {
                    return < StreamCard 
                        key={stream.id}
                        streamObj={stream} 
                        onStreamClick={handleStreamClick}
                        tagColors={tagColors}
                    />
                })}
                {isLoadingMore && (
                    <div style={{ padding: '20px', textAlign: 'center', color: '#ccc' }}>
                        Загрузка...
                    </div>
                )}
                {!hasMoreStreams && streamsArray.length > 0 && (
                    <div style={{ padding: '20px', textAlign: 'center', color: '#888' }}>
                        Все стримы загружены
                    </div>
                )}
            </div>
            <div className="packetData">
                {packetData && packetData.length > 0 && packetData.map(packet => (
                    <PacketCard 
                        key={packet.id}
                        {...packet}
                    />
                ))}
            </div>
        </div>
    );
}