use sqlx::PgPool;
use crate::api::tag::tag_dto::{Tag, TagName, TagNameCreate};
use crate::api::tag::tag_repository::TagRepository;

pub(crate) struct TagService {
    repository: TagRepository,
}

impl TagService {
    pub fn new(repository: TagRepository) -> Self {
        Self {
            repository
        }
    }

    pub fn new_from_pgpool(pool: PgPool) -> Self {
        Self {
            repository: TagRepository::new(pool)
        }
    }

    pub async fn create_tag_name(&self, tag_name: TagNameCreate) -> Result<TagName, sqlx::Error> {
        self.repository.create_tag_name(tag_name).await
    }

    pub async fn get_tags_by_packet_id(&self, packet_id: i64) -> Result<Vec<Tag>, sqlx::Error> {
        self.repository.get_tags_by_packet_id(packet_id).await
    }

    pub async fn get_tags_by_stream_id(&self, stream_id: i64) -> Result<Vec<Tag>, sqlx::Error> {
        self.repository.get_tags_by_stream_id(stream_id).await
    }

    pub async fn get_tag_name_by_id(&self, id: i64) -> Result<TagName, sqlx::Error> {
        self.repository.get_tag_name_by_id(id).await
    }

    pub async fn update_tag_name(&self, id: i64, tag_name: TagNameCreate) -> Result<TagName, sqlx::Error> {
        self.repository.update_tag_name(id, tag_name).await
    }

    pub async fn delete_tag(&self, id: i64) -> Result<(), sqlx::Error> {
        self.repository.delete_tag(id).await
    }

    pub async fn delete_tag_name(&self, id: i64) -> Result<(), sqlx::Error> {
        self.repository.delete_tag_name(id).await
    }

    pub async fn get_all_tag_names(&self) -> Result<Vec<TagName>, sqlx::Error> {
        self.repository.get_all_tag_names().await
    }

    pub async fn get_tag_metrics(&self, minutes: i32) -> Result<Vec<(i64, String, i64, i64)>, sqlx::Error> {
        self.repository.get_tag_metrics(minutes).await
    }

    pub async fn get_flag_balance(&self, minutes: i32) -> Result<Vec<(i64, String, i64, i64)>, sqlx::Error> {
        self.repository.get_flag_balance(minutes).await
    }
}
