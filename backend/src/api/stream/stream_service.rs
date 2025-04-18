use sqlx::PgPool;
use crate::api::stream::stream_dto::{Stream, StreamListResponse};
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
        services: Option<Vec<i32>>
    ) -> Result<Vec<StreamListResponse>, sqlx::Error> {
        let limit = limit.unwrap_or(10);
        let offset = offset.unwrap_or(0);
        
        let streams = self.stream_repository.get_streams_with_tags(
            limit, 
            offset,
            tags,
            services
        ).await?;
        
        let responses = streams.into_iter()
            .map(|(id, first_time, last_time, tags, service_name, service_id)| {
                StreamListResponse {
                    id,
                    first_packet_time: first_time.unwrap_or(0),
                    last_packet_time: last_time.unwrap_or(0),
                    tags,
                    service_id: service_id.unwrap_or(-1),
                    service_name: service_name.unwrap_or("Unknown".to_string()),
                }
            })
            .collect();
        
        Ok(responses)
    }
}