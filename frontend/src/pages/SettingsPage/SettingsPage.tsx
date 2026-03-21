import { ReactElement } from "react";
import './SettingsPage.css'
import { useState, useEffect } from 'react';

interface Service {
    id?: number;
    name: string;
    port: number;
    chunked_transfer_encoding: boolean;
    urldecode: boolean;
    merge_adjacent_packets: boolean;
    inflate_websockets: boolean;
    decrypt_tls: boolean;
    http_parse: boolean;
}

interface Filter {
    id?: number;
    name: string;
    color: number;
    template_type: string;
    template: string;
    direction: string;
    special_type: string | null;
}

export function SettingsPage() {
    // Settings state
    const [farmUrl, setFarmUrl] = useState('');
    const [farmEnabled, setFarmEnabled] = useState(false);
    const [settingsLoaded, setSettingsLoaded] = useState(false);

    // Services state
    const [services, setServices] = useState<Service[]>([]);
    const [serviceModalOpen, setServiceModalOpen] = useState(false);
    const [editingService, setEditingService] = useState<Service | null>(null);
    const [serviceForm, setServiceForm] = useState<Service>({ 
        name: '', 
        port: 8080,
        chunked_transfer_encoding: false,
        urldecode: false,
        merge_adjacent_packets: false,
        inflate_websockets: false,
        decrypt_tls: false,
        http_parse: false
    });
    
    // Filters state
    const [filters, setFilters] = useState<Filter[]>([]);
    const [filterModalOpen, setFilterModalOpen] = useState(false);
    const [editingFilter, setEditingFilter] = useState<Filter | null>(null);
    const [filterForm, setFilterForm] = useState<Filter>({ 
        name: '', 
        color: 0,
        template_type: 'regex',
        template: '',
        direction: 'both',
        special_type: null
    });

    const API_URL = '/api';

    const fetchSettings = async () => {
        try {
            const resp = await fetch(`${API_URL}/settings`);
            if (resp.ok) {
                const data = await resp.json();
                setFarmUrl(data.farm_url || '');
                setFarmEnabled(data.farm_enabled === 'true');
                setSettingsLoaded(true);
            }
        } catch (e) { console.error('Error fetching settings:', e); }
    };

    const saveSetting = async (key: string, value: string) => {
        try {
            await fetch(`${API_URL}/settings/${key}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ value })
            });
        } catch (e) { console.error('Error saving setting:', e); }
    };

    // Fetch on mount
    useEffect(() => {
        fetchServices();
        fetchFilters();
        fetchSettings();
    }, []);

    // Services CRUD operations
    const fetchServices = async () => {
        try {
            const response = await fetch(`${API_URL}/service`);
            if (response.ok) {
                const data = await response.json();
                setServices(Object.values(data));
            }
        } catch (error) {
            console.error('Error fetching services:', error);
        }
    };

    const openServiceModal = (service?: Service) => {
        if (service) {
            setEditingService(service);
            setServiceForm(service);
        } else {
            setEditingService(null);
            setServiceForm({ 
                name: '', 
                port: 8080,
                chunked_transfer_encoding: false,
                urldecode: false,
                merge_adjacent_packets: false,
                inflate_websockets: false,
                decrypt_tls: false
            });
        }
        setServiceModalOpen(true);
    };

    const closeServiceModal = () => {
        setServiceModalOpen(false);
        setEditingService(null);
    };

    const saveService = async () => {
        if (!serviceForm.name.trim()) return;
        
        try {
            const url = editingService 
                ? `${API_URL}/service/${editingService.id}` 
                : `${API_URL}/service`;
            const method = editingService ? 'PUT' : 'POST';
            
            const response = await fetch(url, {
                method,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    name: serviceForm.name,
                    port: serviceForm.port,
                    chunked_transfer_encoding: serviceForm.chunked_transfer_encoding,
                    urldecode: serviceForm.urldecode,
                    merge_adjacent_packets: serviceForm.merge_adjacent_packets,
                    inflate_websockets: serviceForm.inflate_websockets,
                    decrypt_tls: serviceForm.decrypt_tls,
                    http_parse: serviceForm.http_parse
                })
            });
            
            if (response.ok) {
                await fetchServices();
                closeServiceModal();
            }
        } catch (error) {
            console.error('Error saving service:', error);
        }
    };

    const deleteService = async (id: number) => {
        if (!confirm('Вы уверены, что хотите удалить этот сервис?')) return;
        
        try {
            const response = await fetch(`${API_URL}/service/${id}`, {
                method: 'DELETE'
            });
            
            if (response.ok) {
                await fetchServices();
            }
        } catch (error) {
            console.error('Error deleting service:', error);
        }
    };

    // Filters CRUD operations
    const fetchFilters = async () => {
        try {
            const response = await fetch(`${API_URL}/tag/names`);
            if (response.ok) {
                const data = await response.json();
                setFilters(Object.values(data));
            }
        } catch (error) {
            console.error('Error fetching filters:', error);
        }
    };

    const openFilterModal = (filter?: Filter) => {
        if (filter) {
            setEditingFilter(filter);
            setFilterForm(filter);
        } else {
            setEditingFilter(null);
            setFilterForm({ 
                name: '', 
                color: 0,
                template_type: 'regex',
                template: '',
                direction: 'both'
            });
        }
        setFilterModalOpen(true);
    };

    const closeFilterModal = () => {
        setFilterModalOpen(false);
        setEditingFilter(null);
    };

    const saveFilter = async () => {
        if (!filterForm.name.trim() || !filterForm.template.trim()) return;
        
        try {
            const url = editingFilter 
                ? `${API_URL}/tag/names/${editingFilter.id}` 
                : `${API_URL}/tag/names`;
            const method = editingFilter ? 'PUT' : 'POST';
            
            const response = await fetch(url, {
                method,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    name: filterForm.name,
                    color: filterForm.color,
                    template_type: filterForm.template_type,
                    template: filterForm.template,
                    direction: filterForm.direction,
                    special_type: filterForm.special_type || null
                })
            });
            
            if (response.ok) {
                await fetchFilters();
                closeFilterModal();
            }
        } catch (error) {
            console.error('Error saving filter:', error);
        }
    };

    const deleteFilter = async (id: number) => {
        if (!confirm('Вы уверены, что хотите удалить этот фильтр?')) return;
        
        try {
            const response = await fetch(`${API_URL}/tag/names/${id}`, {
                method: 'DELETE'
            });
            
            if (response.ok) {
                await fetchFilters();
            }
        } catch (error) {
            console.error('Error deleting filter:', error);
        }
    };

    return (
        <div className="SettingsPage">
            <div className="settings-columns">
                {/* Services Column */}
                <div className="settings-column">
                    <div className="column-header">
                        <h2>Сервисы</h2>
                        <button className="add-btn" onClick={() => openServiceModal()}>+ Добавить</button>
                    </div>
                    <div className="items-list">
                        {services.map((service) => (
                            <div key={service.id} className="settings-card">
                                <div className="card-content">
                                    <h4>{service.name}</h4>
                                    <p className="port-info">Порт: {service.port}</p>
                                    <div className="service-options">
                                        {service.chunked_transfer_encoding && <span className="option-badge">Chunked</span>}
                                        {service.urldecode && <span className="option-badge">URLdecode</span>}
                                        {service.merge_adjacent_packets && <span className="option-badge">Merge</span>}
                                        {service.inflate_websockets && <span className="option-badge">WS</span>}
                                        {service.decrypt_tls && <span className="option-badge">TLS</span>}
                                        {service.http_parse && <span className="option-badge">HTTP</span>}
                                    </div>
                                </div>
                                <div className="card-actions">
                                    <button onClick={() => openServiceModal(service)}>✎ Изменить</button>
                                    <button onClick={() => deleteService(service.id!)}>✕ Удалить</button>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>

                {/* Filters Column */}
                <div className="settings-column">
                    <div className="column-header">
                        <h2>Фильтры</h2>
                        <button className="add-btn" onClick={() => openFilterModal()}>+ Добавить</button>
                    </div>
                    <div className="items-list">
                        {filters.map((filter) => (
                            <div key={filter.id} className="settings-card">
                                <div className="card-content">
                                    <div className="filter-header">
                                        <h4>{filter.name}</h4>
                                        <div 
                                            className="filter-color-indicator"
                                            style={{ 
                                                backgroundColor: `hsl(${filter.color}, 100%, 50%)`,
                                                color: `hsl(${filter.color}, 100%, 50%)`
                                            }}
                                        ></div>
                                    </div>
                                    <p className="pattern-text">{filter.template}</p>
                                    <div className="filter-info">
                                        <span className="info-badge">{filter.template_type}</span>
                                        <span className="info-badge">
                                            {filter.direction === 'in' ? 'In' : filter.direction === 'out' ? 'Out' : 'Both'}
                                        </span>
                                        {filter.special_type && (
                                            <span className="option-badge">{filter.special_type}</span>
                                        )}
                                    </div>
                                </div>
                                <div className="card-actions">
                                    <button onClick={() => openFilterModal(filter)}>✎ Изменить</button>
                                    <button onClick={() => deleteFilter(filter.id!)}>✕ Удалить</button>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>

                {/* General Settings Column */}
                <div className="settings-column">
                    <div className="column-header">
                        <h2>Общие</h2>
                    </div>
                    <div className="items-list">
                        <div className="settings-card">
                            <div className="card-content">
                                <h4>Flag Farm</h4>
                                <div className="setting-row">
                                    <label className="checkbox-label">
                                        <input
                                            type="checkbox"
                                            checked={farmEnabled}
                                            onChange={(e) => {
                                                setFarmEnabled(e.target.checked);
                                                saveSetting('farm_enabled', e.target.checked ? 'true' : 'false');
                                            }}
                                        />
                                        Отправка флагов включена
                                    </label>
                                </div>
                                <input
                                    type="text"
                                    placeholder="Farm URL (http://farm:8080/submit)"
                                    value={farmUrl}
                                    onChange={(e) => setFarmUrl(e.target.value)}
                                    onBlur={() => saveSetting('farm_url', farmUrl)}
                                    style={{ width: 'calc(100% - 24px)', padding: '10px 12px', background: '#1a1a1a', border: '1px solid transparent', borderRadius: '8px', color: 'white', fontSize: '13px', marginTop: '8px' }}
                                />
                                <p style={{ color: '#888', fontSize: '12px', margin: '8px 0 0' }}>
                                    {farmEnabled
                                        ? farmUrl ? `Флаги отправляются на ${farmUrl}` : 'Укажите URL фермы'
                                        : 'Отправка выключена'}
                                </p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Service Modal */}
            {serviceModalOpen && (
                <div className="modal-overlay" onClick={closeServiceModal}>
                    <div className="modal-content" onClick={(e) => e.stopPropagation()}>
                        <div className="modal-header">
                            <h3>{editingService ? 'Редактировать сервис' : 'Добавить сервис'}</h3>
                            <button className="close-btn" onClick={closeServiceModal}>✕</button>
                        </div>
                        <div className="modal-body">
                            <input
                                type="text"
                                placeholder="Название сервиса"
                                value={serviceForm.name}
                                onChange={(e) => setServiceForm({ ...serviceForm, name: e.target.value })}
                            />
                            <input
                                type="number"
                                placeholder="Порт"
                                value={serviceForm.port}
                                onChange={(e) => setServiceForm({ ...serviceForm, port: parseInt(e.target.value) || 0 })}
                            />
                            <div className="checkbox-group">
                                <label className="checkbox-label">
                                    <input
                                        type="checkbox"
                                        checked={serviceForm.chunked_transfer_encoding}
                                        onChange={(e) => setServiceForm({ ...serviceForm, chunked_transfer_encoding: e.target.checked })}
                                    />
                                    Chunked Transfer Encoding
                                </label>
                                <label className="checkbox-label">
                                    <input
                                        type="checkbox"
                                        checked={serviceForm.urldecode}
                                        onChange={(e) => setServiceForm({ ...serviceForm, urldecode: e.target.checked })}
                                    />
                                    URL Decode
                                </label>
                                <label className="checkbox-label">
                                    <input
                                        type="checkbox"
                                        checked={serviceForm.merge_adjacent_packets}
                                        onChange={(e) => setServiceForm({ ...serviceForm, merge_adjacent_packets: e.target.checked })}
                                    />
                                    Merge Adjacent Packets
                                </label>
                                <label className="checkbox-label">
                                    <input
                                        type="checkbox"
                                        checked={serviceForm.inflate_websockets}
                                        onChange={(e) => setServiceForm({ ...serviceForm, inflate_websockets: e.target.checked })}
                                    />
                                    Inflate WebSockets
                                </label>
                                <label className="checkbox-label">
                                    <input
                                        type="checkbox"
                                        checked={serviceForm.decrypt_tls}
                                        onChange={(e) => setServiceForm({ ...serviceForm, decrypt_tls: e.target.checked })}
                                    />
                                    Decrypt TLS
                                </label>
                                <label className="checkbox-label">
                                    <input
                                        type="checkbox"
                                        checked={serviceForm.http_parse}
                                        onChange={(e) => setServiceForm({ ...serviceForm, http_parse: e.target.checked })}
                                    />
                                    HTTP Parse
                                </label>
                            </div>
                        </div>
                        <div className="modal-footer">
                            <button onClick={closeServiceModal}>Отмена</button>
                            <button onClick={saveService}>Сохранить</button>
                        </div>
                    </div>
                </div>
            )}

            {/* Filter Modal */}
            {filterModalOpen && (
                <div className="modal-overlay" onClick={closeFilterModal}>
                    <div className="modal-content" onClick={(e) => e.stopPropagation()}>
                        <div className="modal-header">
                            <h3>{editingFilter ? 'Редактировать фильтр' : 'Добавить фильтр'}</h3>
                            <button className="close-btn" onClick={closeFilterModal}>✕</button>
                        </div>
                        <div className="modal-body">
                            <input
                                type="text"
                                placeholder="Название фильтра"
                                value={filterForm.name}
                                onChange={(e) => setFilterForm({ ...filterForm, name: e.target.value })}
                            />
                            <select 
                                value={filterForm.template_type}
                                onChange={(e) => setFilterForm({ ...filterForm, template_type: e.target.value })}
                                className="select-input"
                            >
                                <option value="regex">Regex</option>
                                <option value="substr">Substring</option>
                                <option value="bsubstr">Binary Substring</option>
                            </select>
                            <input
                                type="text"
                                placeholder="Шаблон"
                                value={filterForm.template}
                                onChange={(e) => setFilterForm({ ...filterForm, template: e.target.value })}
                            />
                            <select 
                                value={filterForm.direction}
                                onChange={(e) => setFilterForm({ ...filterForm, direction: e.target.value })}
                                className="select-input"
                            >
                                <option value="in">Входящие</option>
                                <option value="out">Исходящие</option>
                                <option value="both">Оба направления</option>
                            </select>
                            <select
                                value={filterForm.special_type || ''}
                                onChange={(e) => setFilterForm({ ...filterForm, special_type: e.target.value || null })}
                                className="select-input"
                            >
                                <option value="">Без спец.типа</option>
                                <option value="flag">Flag (отправка в ферму)</option>
                                <option value="alert">Alert (уведомление)</option>
                                <option value="block">Block (авто-drop на агентах)</option>
                            </select>
                            <div className="color-picker-wrapper">
                                <label>Цвет (Hue): {filterForm.color}°</label>
                                <input
                                    type="range"
                                    min="0"
                                    max="360"
                                    value={filterForm.color}
                                    onChange={(e) => setFilterForm({ ...filterForm, color: parseInt(e.target.value) })}
                                    className="color-slider"
                                />
                                <div 
                                    className="color-preview"
                                    style={{ 
                                        backgroundColor: `hsl(${filterForm.color}, 100%, 50%)`,
                                        color: `hsl(${filterForm.color}, 100%, 50%)`
                                    }}
                                ></div>
                            </div>
                        </div>
                        <div className="modal-footer">
                            <button onClick={closeFilterModal}>Отмена</button>
                            <button onClick={saveFilter}>Сохранить</button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}