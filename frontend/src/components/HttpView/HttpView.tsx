import { useState, useEffect } from 'react';
import './HttpView.css';

interface HttpHeader {
  id: number;
  message_id: number;
  name: string;
  value: string;
}

interface HttpMessage {
  id: number;
  stream_id: number;
  packet_id: number;
  is_request: boolean;
  method: string | null;
  path: string | null;
  status_code: number | null;
  http_version: string | null;
  content_type: string | null;
  content_length: number | null;
  body_offset: number | null;
  body_length: number | null;
  timestamp: number;
}

interface SearchResult {
  stream_ids: number[];
  total: number;
}

interface HeaderFilter {
  name: string;
  value: string;
}

interface Props {
  streamId: number;
  onStreamNavigate?: (streamId: number) => void;
}

function statusClass(code: number | null): string {
  if (!code) return '';
  if (code < 300) return 'status-2xx';
  if (code < 400) return 'status-3xx';
  if (code < 500) return 'status-4xx';
  return 'status-5xx';
}

function generateCurl(msg: HttpMessage, headers: HttpHeader[]): string {
  if (!msg.is_request) return '';
  const parts = [`curl -X ${msg.method} '${msg.path}'`];
  for (const h of headers) {
    if (h.name.toLowerCase() === 'host') continue;
    parts.push(`  -H '${h.name}: ${h.value}'`);
  }
  return parts.join(' \\\n');
}

export default function HttpView({ streamId, onStreamNavigate }: Props) {
  const [messages, setMessages] = useState<HttpMessage[]>([]);
  const [expanded, setExpanded] = useState<number | null>(null);
  const [headers, setHeaders] = useState<Record<number, HttpHeader[]>>({});
  const [searchFilters, setSearchFilters] = useState<HeaderFilter[]>([]);
  const [searchResults, setSearchResults] = useState<SearchResult | null>(null);
  const [searching, setSearching] = useState(false);

  useEffect(() => {
    fetch(`/api/http/stream/${streamId}`)
      .then(r => r.json())
      .then(data => setMessages(Array.isArray(data) ? data : []))
      .catch(() => setMessages([]));
    setExpanded(null);
    setHeaders({});
    setSearchFilters([]);
    setSearchResults(null);
  }, [streamId]);

  const toggleExpand = async (msgId: number) => {
    if (expanded === msgId) {
      setExpanded(null);
      return;
    }
    setExpanded(msgId);
    if (!headers[msgId]) {
      const data = await fetch(`/api/http/headers/${msgId}`).then(r => r.json());
      setHeaders(prev => ({ ...prev, [msgId]: Array.isArray(data) ? data : [] }));
    }
  };

  const addSearchFilter = (name: string, value: string) => {
    const exists = searchFilters.some(f => f.name === name && f.value === value);
    if (!exists) {
      const updated = [...searchFilters, { name, value }];
      setSearchFilters(updated);
      runSearch(updated);
    }
  };

  const removeSearchFilter = (index: number) => {
    const updated = searchFilters.filter((_, i) => i !== index);
    setSearchFilters(updated);
    if (updated.length > 0) {
      runSearch(updated);
    } else {
      setSearchResults(null);
    }
  };

  const runSearch = async (filters: HeaderFilter[]) => {
    setSearching(true);
    try {
      const body = filters.map(f => ({ name: f.name, value: f.value }));
      const data = await fetch('/api/http/search/multi', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      }).then(r => r.json());
      setSearchResults(data);
    } catch {
      setSearchResults(null);
    }
    setSearching(false);
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  if (messages.length === 0) {
    return null; // No HTTP data — parent will show raw packets
  }

  return (
    <div className="http-view">
      {/* Search Filters Bar */}
      {searchFilters.length > 0 && (
        <div className="search-bar">
          <div className="search-filters">
            {searchFilters.map((f, i) => (
              <span key={i} className="search-chip">
                {f.name}: {f.value.length > 30 ? f.value.slice(0, 30) + '...' : f.value}
                <button onClick={() => removeSearchFilter(i)}>✕</button>
              </span>
            ))}
          </div>
          <div className="search-results-info">
            {searching ? 'Поиск...' : searchResults
              ? `${searchResults.total} стримов (AND)`
              : ''}
          </div>
          {searchResults && searchResults.stream_ids.length > 0 && (
            <div className="search-stream-list">
              {searchResults.stream_ids.slice(0, 20).map(sid => (
                <button
                  key={sid}
                  className="search-stream-link"
                  onClick={() => onStreamNavigate?.(sid)}
                >
                  #{sid}
                </button>
              ))}
              {searchResults.stream_ids.length > 20 && (
                <span className="text-muted">
                  +{searchResults.stream_ids.length - 20} more
                </span>
              )}
            </div>
          )}
        </div>
      )}

      {/* HTTP Messages */}
      <div className="http-messages">
        {messages.map(msg => (
          <div key={msg.id} className="http-message">
            <div
              className={`http-message-header ${expanded === msg.id ? 'expanded' : ''}`}
              onClick={() => toggleExpand(msg.id)}
            >
              <span className="expand-icon">{expanded === msg.id ? '▼' : '▶'}</span>
              {msg.is_request ? (
                <>
                  <span className="http-method">{msg.method}</span>
                  <span className="http-path">{msg.path}</span>
                </>
              ) : (
                <>
                  <span className={`http-status ${statusClass(msg.status_code)}`}>
                    {msg.status_code}
                  </span>
                  <span className="http-content-type">
                    {msg.content_type || ''}
                  </span>
                </>
              )}
              <span className="http-meta">
                {msg.body_length != null && msg.body_length > 0 && (
                  <span className="body-size">{msg.body_length}B</span>
                )}
                <span className="http-version">HTTP/{msg.http_version}</span>
              </span>
            </div>

            {expanded === msg.id && headers[msg.id] && (
              <div className="http-message-body">
                <div className="http-headers-list">
                  {headers[msg.id].map(h => (
                    <div key={h.id} className="http-header-row">
                      <span className="header-name">{h.name}:</span>
                      <span className="header-value">{h.value}</span>
                      <button
                        className="search-btn"
                        title={`Найти стримы с ${h.name}: ${h.value}`}
                        onClick={(e) => {
                          e.stopPropagation();
                          addSearchFilter(h.name, h.value);
                        }}
                      >
                        🔍
                      </button>
                    </div>
                  ))}
                </div>
                {msg.is_request && (
                  <div className="http-actions">
                    <button
                      className="curl-btn"
                      onClick={() => copyToClipboard(generateCurl(msg, headers[msg.id]))}
                    >
                      CURL
                    </button>
                  </div>
                )}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
