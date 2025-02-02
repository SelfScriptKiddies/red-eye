use sqlx::PgPool;
use crate::api::service::service_dto::Service;

pub struct ServiceRepository {
    pgpool: PgPool
}

impl ServiceRepository {
    pub fn new(pgpool: PgPool) -> Self {
        ServiceRepository {
            pgpool
        }
    }

    pub async fn get_service_by_id(&self, id: i32) -> Result<Service, sqlx::Error> {
        sqlx::query_as::<_, Service>(
            r#"
            SELECT id, name, port, chunked_transfer_encoding, urldecode, 
                   merge_adjacent_packets, inflate_websockets, decrypt_tls
            FROM service 
            WHERE id = $1
            "#)
            .bind(id)
            .fetch_one(&self.pgpool)
            .await
    }

    pub async fn get_all_services(&self) -> Result<Vec<Service>, sqlx::Error> {
        sqlx::query_as::<_, Service>(
            r#"
            SELECT id, name, port, chunked_transfer_encoding, urldecode,
                   merge_adjacent_packets, inflate_websockets, decrypt_tls
            FROM service
            "#)
            .fetch_all(&self.pgpool)
            .await
    }

    pub async fn create_service(&self, service: &Service) -> Result<Service, sqlx::Error> {
        sqlx::query_as::<_, Service>(
            r#"
            INSERT INTO service (name, port, chunked_transfer_encoding, urldecode,
                               merge_adjacent_packets, inflate_websockets, decrypt_tls)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id, name, port, chunked_transfer_encoding, urldecode,
                      merge_adjacent_packets, inflate_websockets, decrypt_tls
            "#)
            .bind(&service.name)
            .bind(service.port)
            .bind(service.chunked_transfer_encoding)
            .bind(service.urldecode)
            .bind(service.merge_adjacent_packets)
            .bind(service.inflate_websockets)
            .bind(service.decrypt_tls)
            .fetch_one(&self.pgpool)
            .await
    }

    pub async fn update_service(&self, service: &Service) -> Result<Service, sqlx::Error> {
        sqlx::query_as::<_, Service>(
            r#"
            UPDATE service 
            SET name = $2, port = $3, chunked_transfer_encoding = $4, urldecode = $5,
                merge_adjacent_packets = $6, inflate_websockets = $7, decrypt_tls = $8
            WHERE id = $1
            RETURNING id, name, port, chunked_transfer_encoding, urldecode,
                      merge_adjacent_packets, inflate_websockets, decrypt_tls
            "#)
            .bind(service.id)
            .bind(&service.name)
            .bind(service.port)
            .bind(service.chunked_transfer_encoding)
            .bind(service.urldecode)
            .bind(service.merge_adjacent_packets)
            .bind(service.inflate_websockets)
            .bind(service.decrypt_tls)
            .fetch_one(&self.pgpool)
            .await
    }

    pub async fn delete_service(&self, id: i32) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            DELETE FROM service WHERE id = $1
            "#)
            .bind(id)
            .execute(&self.pgpool)
            .await?;
        Ok(())
    }
}
