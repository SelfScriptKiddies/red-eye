# Harpoon — TODO для интеграции с Red Eye

## Контекст

Harpoon используется как движок внутри Red Eye Agent (`red-eye-agent`). Agent линкует `harpoon-core` как библиотеку и управляет правилами через `run_with_capture()`. Пакеты из CaptureManager отправляются в NATS для анализа.

Текущая архитектура:
```
Клиент → [nftables redirect :PORT → :PROXY_PORT] → Harpoon (proxy) → Сервис (:PORT)
                                                         ↓
                                                  CaptureManager → NATS → Analyzer
```

## Баг 1: rustls CryptoProvider panic (CRITICAL)

**Проблема:** При создании proxy-правила с `tls_mode` (или без — если rustls фичи включены) происходит panic:
```
thread 'tokio-rt-worker' panicked at rustls/src/crypto/mod.rs:249:
Could not automatically determine the process-level CryptoProvider
```

**Причина:** rustls 0.23+ требует явной инициализации `CryptoProvider::install_default()` до первого использования TLS.

**Решение:** В `harpoon-core`, в функции `run_with_capture()` или `run()`, добавить в начало:
```rust
#[cfg(feature = "tls")]
{
    let _ = rustls::crypto::ring::default_provider().install_default();
    // или aws_lc_rs, в зависимости от используемого backend
}
```

Или в `Cargo.toml` harpoon-core указать конкретный feature:
```toml
rustls = { version = "0.23", features = ["ring"] }  # или aws-lc-rs
```

## Баг 2: HTTP/2 gRPC проксирование (HIGH)

**Проблема:** Harpoon TCP proxy (FastForward tier) закрывает соединение при получении HTTP/2 traffic. gRPC клиент получает `Connection reset by peer`.

**Ожидаемое поведение:** FastForward (copy_bidirectional) должен прозрачно копировать любые байты, включая HTTP/2 binary frames. Он не должен парсить или валидировать содержимое.

**Воспроизведение:**
```bash
# Сервер
python3 -c "
import grpc; from concurrent import futures
# ... gRPC server on :31338
"

# Правило harpoon: listen :41338 → target :31338
# Клиент
python3 -c "
import grpc
channel = grpc.insecure_channel('localhost:41338')
# ... → 'Connection reset by peer'
"
```

**Возможные причины:**
1. Harpoon пытается парсить входящий трафик как HTTP/1.1 и ломается на `PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n` (HTTP/2 connection preface)
2. Pipeline compilation выбирает не FastForward tier, а Linear с HTTP parsing, которое фейлит
3. Какой-то timeout или buffer overflow при HTTP/2 binary data

**Решение:** Для `copy_bidirectional` (FastForward tier) — просто копировать байты без парсинга. Убедиться, что pipeline compilation для простого TCP proxy без фильтров/TLS выбирает FastForward.

## Фича 1: HTTP/2 aware проксирование (MEDIUM)

**Цель:** Для gRPC/HTTP/2 сервисов harpoon должен уметь декодировать HTTP/2 фреймы и записывать application-layer данные в CaptureManager.

**Текущее поведение:** CaptureManager записывает raw TCP bytes. Для HTTP/2 это бинарные фреймы (HPACK compressed headers + DATA frames), которые analyzer не может распарсить через `httparse` (только HTTP/1.x).

**Желаемое поведение:**
- Если правило помечено как `http2: true` (или автодетект по HTTP/2 connection preface):
  - Harpoon декодирует HTTP/2 фреймы
  - Для каждого HTTP/2 stream (request/response pair):
    - Извлекает заголовки (HPACK decompress)
    - Извлекает DATA (body)
    - Записывает в CaptureManager как отдельный CapturedPacket с plaintext HTTP/1.1-like данными
- CapturedPacket.payload должен выглядеть как:
  ```
  POST /UserService/Login HTTP/2\r\n
  content-type: application/grpc\r\n
  te: trailers\r\n
  \r\n
  <body bytes>
  ```
  Чтобы analyzer мог распарсить через существующий `httparse` + `grpc_parser`.

**Реализация:**
- Использовать crate `h2` для HTTP/2 frame parsing
- Или ручной парсинг: SETTINGS, HEADERS (HPACK), DATA frames
- Не обязательно полная HTTP/2 реализация — достаточно extracting headers + body per stream

**Альтернатива (проще):** Вместо HTTP/2 decode в harpoon, можно сделать HTTP/2→HTTP/1.1 downgrade proxy:
- Harpoon принимает HTTP/2 от клиента
- Конвертирует каждый stream в HTTP/1.1 request
- Проксирует к серверу как HTTP/1.1
- Ответ HTTP/1.1 → HTTP/2 обратно клиенту
- CaptureManager записывает HTTP/1.1 plaintext → analyzer парсит как обычно

## Фича 2: DropConnection FilterAction (LOW)

**Текущее состояние:** В `harpoon-core/src/types/filter.rs` есть `FilterAction::Drop` и `FilterAction::TapOnly`. Red Eye Agent использует `FilterAction::DropConnection` для полного закрытия TCP-соединения при срабатывании фильтра.

**Проверить:** Существует ли `DropConnection` в текущем harpoon-core? Если нет — добавить:
```rust
pub enum FilterAction {
    Pass,
    Drop,           // Drop the matched data, keep connection alive
    DropConnection, // Close the entire TCP connection
    TapOnly,        // Allow through but record
}
```

При `DropConnection` — после матча фильтра, закрыть обе стороны TCP (client + upstream).

## Фича 3: Backpressure для CaptureManager (LOW)

**Проблема:** При высоком трафике (1000+ pkt/sec) broadcast channel CaptureManager может отставать. `RecvError::Lagged(n)` означает потерю пакетов.

**Текущее:** Channel capacity = 4096. При lagged — agent логирует warning.

**Возможные улучшения:**
1. Увеличить capacity до 16384
2. Добавить метрику dropped packets в EngineHandle
3. Adaptive sampling: при lagged > threshold — пропускать каждый N-й пакет

## Файловая структура (для контекста)

```
harpoon/
  crates/
    harpoon-core/
      src/
        capture/
          manager.rs    ← CaptureManager, record(), subscribe(), broadcast channel
        engine/
          mod.rs        ← run_with_capture(), EngineHandle
          executor.rs   ← spawn_plan(), TCP/UDP executors, FastForward/Linear/DAG
        tls/
          mod.rs        ← TLS MITM (rustls)
          cert.rs       ← CertAuthority
        types/
          rule.rs       ← Rule, TlsConfig, TlsMode, FilterAction
          filter.rs     ← Filter, FilterKind, Direction
          endpoint.rs   ← Endpoint, Protocol
        pipeline/       ← DAG model, compile, simulate
```

## Приоритеты

1. **Баг 1 (CryptoProvider)** — критический, без этого TLS proxy не работает
2. **Баг 2 (HTTP/2 proxy)** — высокий, gRPC не проксируется
3. **Фича 1 (HTTP/2 decode)** — средний, для L7 анализа gRPC через proxy
4. **Фича 2 (DropConnection)** — низкий, проверить наличие
5. **Фича 3 (Backpressure)** — низкий, для стабильности под нагрузкой
