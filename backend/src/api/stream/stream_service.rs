use sqlx::PgPool;
use crate::api::stream::stream_dto::Stream;
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
}