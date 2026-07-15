use sqlx::PgPool;
use crate::api::stream::stream_dto::{HistogramBucket, Stream, StreamListResponse};
use crate::api::stream::stream_repository::StreamRepository;

pub(crate) struct StreamService {
    stream_repository: StreamRepository
}

impl StreamService {
    pub(crate) fn new(stream_repository: StreamRepository) -> Self {
        StreamService {
            stream_repository
        }
    }

    pub(crate) fn new_from_pgpool(pg_pool: PgPool) -> Self {
        StreamService::new(
            StreamRepository::new(pg_pool)
        )
    }

    pub async fn get_stream_by_id(&self, stream_id: i32) -> Result<Stream, sqlx::Error> {
        self.stream_repository.get_packet_by_id(stream_id).await
    }

    pub async fn get_all_streams_with_details(
        &self,
        limit: Option<i32>,
        offset: Option<i32>,
        tags: Option<Vec<i32>>,
        services: Option<Vec<i32>>,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<Vec<StreamListResponse>, sqlx::Error> {
        let limit = limit.unwrap_or(10);
        let offset = offset.unwrap_or(0);

        let streams = self.stream_repository.get_streams_with_tags(
            limit,
            offset,
            tags,
            services,
            start_time,
            end_time,
        ).await?;
        
        let responses = streams.into_iter()
            .map(|(id, first_time, last_time, packet_count, tags, service_name, service_id, favorite, l7_protocol, src_ip, dst_ip)| {
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
            })
            .collect();
        
        Ok(responses)
    }

    pub async fn get_stream_histogram(&self, bucket_secs: i32) -> Result<Vec<HistogramBucket>, sqlx::Error> {
        let rows = self.stream_repository.get_stream_histogram(bucket_secs).await?;
        Ok(rows.into_iter().map(|(timestamp, service_id, service_name, count)| {
            HistogramBucket { timestamp, service_id, service_name, count }
        }).collect())
    }

    pub async fn set_favorite(&self, stream_id: i32, favorite: bool) -> Result<bool, sqlx::Error> {
        self.stream_repository.set_favorite(stream_id, favorite).await
    }
}