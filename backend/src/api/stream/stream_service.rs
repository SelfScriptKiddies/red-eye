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

    pub async fn get_all_streams_with_details(&self, offset: Option<i32>, limit: Option<i32>) -> Result<Vec<StreamListResponse>, sqlx::Error> {
        let offset = offset.unwrap_or(0);
        let limit = limit.unwrap_or(10);
        
        let streams = self.stream_repository.get_all_streams().await?;
        let mut result = Vec::new();
        
        for stream in streams {
            let (first_time, last_time) = self.stream_repository
                .get_stream_packet_timestamps(stream.id)
                .await?;
                
            let service_id = format!("{}:{} -> {}:{}", 
                stream.src_ip, 
                stream.src_port, 
                stream.dst_ip, 
                stream.dst_port
            );
            
            let tags = self.tag_service
                .get_tags_by_stream_id(stream.id)
                .await?
                .into_iter()
                .map(|tag| tag.name)
                .collect::<Vec<String>>();
                
            result.push(StreamListResponse {
                id: stream.id,
                service_id,
                tags,
                first_packet_time: first_time,
                last_packet_time: last_time
            });
        }
        
        Ok(result)
    }
}