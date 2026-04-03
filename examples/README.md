# Red Eye Integration Test Examples

Test scripts for verifying Red Eye functionality.

## Prerequisites

- Red Eye stack running (`docker compose up -d`)
- Agent running with sniffer on target interface
- Service created in Settings (port matching your test server)
- Python 3 with `requests` library (`pip install requests`)

## HTTP Tests

```bash
# Start a simple HTTP server on port 31337
python3 -m http.server 31337 &

# Basic HTTP (methods, headers, cookies)
python3 test_http_basic.py http://YOUR_IP:31337

# Exploit patterns (SQLi, RCE, path traversal — for tag testing)
python3 test_exploit_patterns.py http://YOUR_IP:31337

# Multiple sessions (cookie-based grouping)
python3 test_multi_session.py http://YOUR_IP:31337
```

## TLS Tests

```bash
# Generate self-signed cert
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 1 -nodes -subj '/CN=localhost'

# Start HTTPS server
python3 test_tls_service.py --server --port 8443

# Direct HTTPS (without MITM)
python3 test_tls_service.py --client --target https://YOUR_IP:8443 --no-verify

# Through harpoon TLS MITM proxy (if protection enabled with TLS)
python3 test_tls_service.py --client --target https://YOUR_IP:18443 --no-verify
```

## Binary Protocol Tests

```bash
# Start a listener
nc -l -p 9999 &

# Send binary data (tests hex view, non-HTTP protocols)
python3 test_binary_protocol.py YOUR_IP 9999
```

## What to Verify

- [ ] Packets appear in UI
- [ ] HTTP parsing works (HTTP mode in packet view)
- [ ] Headers are clickable (search icon)
- [ ] Cookies are parsed individually
- [ ] Tags trigger on exploit patterns
- [ ] Alert toasts appear for alert-type tags
- [ ] Flags are submitted to farm (if configured)
- [ ] Block tags drop traffic through proxy
- [ ] Path filter works (click on path)
- [ ] IP filter works (click on IP in stream card)
- [ ] Diff view works between two packets
- [ ] Share link preserves HTTP view
- [ ] Export generates valid pwntools/requests/curl code
- [ ] TLS MITM shows decrypted traffic
- [ ] Binary protocol shows correctly in hex view
