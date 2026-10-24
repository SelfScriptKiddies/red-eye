use crate::api::stream::stream_dto::{HistogramBucket, Stream, StreamListResponse};
use crate::api::stream::stream_repository::StreamRepository;
use sqlx::PgPool;

pub(crate) struct StreamService {
    stream_repository: StreamRepository,
}

impl StreamService {
    pub(crate) fn new(stream_repository: StreamRepository) -> Self {
        StreamService { stream_repository }
    }

    pub(crate) fn new_from_pgpool(pg_pool: PgPool) -> Self {
        StreamService::new(StreamRepository::new(pg_pool))
    }

    pub async fn get_stream_by_id(&self, stream_id: i64) -> Result<Stream, sqlx::Error> {
        self.stream_repository.get_packet_by_id(stream_id).await
    }

    pub async fn get_all_streams_with_details(
        &self,
        limit: Option<i64>,
        offset: Option<i64>,
        cursor: Option<(i64, i64)>,
        tags: Option<Vec<i64>>,
        services: Option<Vec<i64>>,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<Vec<StreamListResponse>, sqlx::Error> {
        let limit = limit.unwrap_or(10);
        let offset = offset.unwrap_or(0);

        let streams = self
            .stream_repository
            .get_streams_with_tags(limit, offset, cursor, tags, services, start_time, end_time)
            .await?;

        let responses = streams
            .into_iter()
            .map(
                |(
                    id,
                    first_time,
                    last_time,
                    packet_count,
                    tags,
                    service_name,
                    service_id,
                    favorite,
                    l7_protocol,
                    src_ip,
                    dst_ip,
                )| {
                    StreamListResponse {
                        id,
                        first_packet_time: first_time.unwrap_or(0),
                        last_packet_time: last_time.unwrap_or(0),
                        packet_count: packet_count as i32,
                        tags,
                        service_id: service_id.unwrap_or(-1),
                        service_name: service_name.unwrap_or("Unknown".to_string()),
                        favorite,
                        l7_protocol,
                        src_ip: src_ip.unwrap_or_default(),
                        dst_ip: dst_ip.unwrap_or_default(),
                    }
                },
            )
            .collect();

        Ok(responses)
    }

    pub async fn get_stream_histogram(
        &self,
        bucket_secs: i32,
    ) -> Result<Vec<HistogramBucket>, sqlx::Error> {
        let rows = self
            .stream_repository
            .get_stream_histogram(bucket_secs)
            .await?;
        Ok(rows
            .into_iter()
            .map(|(timestamp, packet_count, flag_count)| HistogramBucket {
                timestamp,
                packet_count,
                flag_count,
            })
            .collect())
    }

    pub async fn set_favorite(&self, stream_id: i64, favorite: bool) -> Result<bool, sqlx::Error> {
        self.stream_repository
            .set_favorite(stream_id, favorite)
            .await
    }
}

/// Parse the opaque keyset cursor "last_time:id" (both i64, seconds).
/// Returns None for absent/empty; Err(()) for malformed values — the handler
/// maps that to a 400 rather than silently returning page 1 (a silently
/// reset cursor would duplicate rows the caller already has).
pub(crate) fn parse_cursor(raw: &str) -> Result<Option<(i64, i64)>, ()> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    let (time_part, id_part) = raw.split_once(':').ok_or(())?;
    let last_time: i64 = time_part.trim().parse().map_err(|_| ())?;
    let id: i64 = id_part.trim().parse().map_err(|_| ())?;
    Ok(Some((last_time, id)))
}

#[cfg(test)]
mod tests {
    use super::parse_cursor;

    #[test]
    fn parses_valid_cursor() {
        assert_eq!(parse_cursor("1738222782:42"), Ok(Some((1738222782, 42))));
        assert_eq!(parse_cursor(" 0 : 7 "), Ok(Some((0, 7))));
    }

    #[test]
    fn empty_is_absent_not_malformed() {
        assert_eq!(parse_cursor(""), Ok(None));
        assert_eq!(parse_cursor("   "), Ok(None));
    }

    #[test]
    fn rejects_malformed_cursors() {
        // Missing id / time, non-numeric parts, extra segments, negatives id.
        assert!(parse_cursor("1738222782").is_err());
        assert!(parse_cursor(":42").is_err());
        assert!(parse_cursor("abc:42").is_err());
        assert!(parse_cursor("123:def").is_err());
        assert!(parse_cursor("1:2:3").is_err());
    }

    #[test]
    fn accepts_negative_values_as_i64() {
        // Defensive: COALESCE(last_packet_time,0) can produce 0; a negative
        // hand-crafted cursor is still well-formed i64 input.
        assert_eq!(parse_cursor("-5:-1"), Ok(Some((-5, -1))));
    }
}
