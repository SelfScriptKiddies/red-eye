use crate::api::service::service_dto::Service;
use sqlx::{PgConnection, PgPool};

pub struct ServiceRepository {
    pgpool: PgPool,
}

impl ServiceRepository {
    pub fn new(pgpool: PgPool) -> Self {
        ServiceRepository { pgpool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pgpool
    }

    pub async fn get_service_by_id(
        conn: &mut PgConnection,
        id: i64,
    ) -> Result<Service, sqlx::Error> {
        sqlx::query_as::<_, Service>(
            r#"
            SELECT id, name, port, chunked_transfer_encoding, urldecode,
                   merge_adjacent_packets, inflate_websockets, decrypt_tls, http_parse, http2_parse,
                   alert_flag_leak, protocol, is_global
            FROM service
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_one(conn)
        .await
    }

    pub async fn get_all_services(&self) -> Result<Vec<Service>, sqlx::Error> {
        sqlx::query_as::<_, Service>(
            r#"
            SELECT id, name, port, chunked_transfer_encoding, urldecode,
                   merge_adjacent_packets, inflate_websockets, decrypt_tls, http_parse, http2_parse,
                   alert_flag_leak, protocol, is_global
            FROM service
            "#,
        )
        .fetch_all(&self.pgpool)
        .await
    }

    /// Explicit agent ids per service id, keyed by service id (empty when
    /// every service is global).
    pub async fn get_all_agent_bindings(&self) -> Result<Vec<(i64, String)>, sqlx::Error> {
        sqlx::query_as::<_, (i64, String)>(
            "SELECT service_id, agent_id FROM agent_service ORDER BY service_id, agent_id",
        )
        .fetch_all(&self.pgpool)
        .await
    }

    pub async fn get_agent_ids_by_service(
        &self,
        service_id: i64,
    ) -> Result<Vec<String>, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT agent_id FROM agent_service WHERE service_id = $1 ORDER BY agent_id",
        )
        .bind(service_id)
        .fetch_all(&self.pgpool)
        .await
    }

    pub async fn insert_service(
        conn: &mut PgConnection,
        service: &Service,
    ) -> Result<Service, sqlx::Error> {
        sqlx::query_as::<_, Service>(
            r#"
            INSERT INTO service (name, port, chunked_transfer_encoding, urldecode,
                               merge_adjacent_packets, inflate_websockets, decrypt_tls, http_parse, http2_parse,
                               alert_flag_leak, protocol, is_global)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING id, name, port, chunked_transfer_encoding, urldecode,
                      merge_adjacent_packets, inflate_websockets, decrypt_tls, http_parse, http2_parse,
                      alert_flag_leak, protocol, is_global
            "#,
        )
        .bind(&service.name)
        .bind(service.port)
        .bind(service.chunked_transfer_encoding)
        .bind(service.urldecode)
        .bind(service.merge_adjacent_packets)
        .bind(service.inflate_websockets)
        .bind(service.decrypt_tls)
        .bind(service.http_parse)
        .bind(service.http2_parse)
        .bind(service.alert_flag_leak)
        .bind(&service.protocol)
        .bind(service.is_global)
        .fetch_one(conn)
        .await
    }

    pub async fn update_service_row(
        conn: &mut PgConnection,
        service: &Service,
    ) -> Result<Service, sqlx::Error> {
        sqlx::query_as::<_, Service>(
            r#"
            UPDATE service
            SET name = $2, port = $3, chunked_transfer_encoding = $4, urldecode = $5,
                merge_adjacent_packets = $6, inflate_websockets = $7, decrypt_tls = $8, http_parse = $9,
                http2_parse = $10, alert_flag_leak = $11, protocol = $12, is_global = $13
            WHERE id = $1
            RETURNING id, name, port, chunked_transfer_encoding, urldecode,
                      merge_adjacent_packets, inflate_websockets, decrypt_tls, http_parse, http2_parse,
                      alert_flag_leak, protocol, is_global
            "#,
        )
        .bind(service.id)
        .bind(&service.name)
        .bind(service.port)
        .bind(service.chunked_transfer_encoding)
        .bind(service.urldecode)
        .bind(service.merge_adjacent_packets)
        .bind(service.inflate_websockets)
        .bind(service.decrypt_tls)
        .bind(service.http_parse)
        .bind(service.http2_parse)
        .bind(service.alert_flag_leak)
        .bind(&service.protocol)
        .bind(service.is_global)
        .fetch_one(conn)
        .await
    }

    pub async fn delete_service_row(conn: &mut PgConnection, id: i64) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM service WHERE id = $1")
            .bind(id)
            .execute(conn)
            .await?;
        Ok(())
    }

    /// Replace the explicit assignment set: agent_service rows become the
    /// complete agent set whenever a service is not global.
    pub async fn replace_assignments(
        conn: &mut PgConnection,
        service_id: i64,
        agent_ids: &[String],
    ) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM agent_service WHERE service_id = $1")
            .bind(service_id)
            .execute(&mut *conn)
            .await?;
        for agent_id in agent_ids {
            sqlx::query(
                "INSERT INTO agent_service (agent_id, service_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            )
            .bind(agent_id)
            .bind(service_id)
            .execute(&mut *conn)
            .await?;
        }
        Ok(())
    }

    pub async fn clear_assignments(
        conn: &mut PgConnection,
        service_id: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM agent_service WHERE service_id = $1")
            .bind(service_id)
            .execute(conn)
            .await?;
        Ok(())
    }

    pub async fn bind_agent(
        conn: &mut PgConnection,
        agent_id: &str,
        service_id: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO agent_service (agent_id, service_id) VALUES ($1, $2) ON CONFLICT DO NOTHING")
            .bind(agent_id)
            .bind(service_id)
            .execute(conn)
            .await?;
        Ok(())
    }

    pub async fn unbind_agent(
        conn: &mut PgConnection,
        agent_id: &str,
        service_id: i64,
    ) -> Result<u64, sqlx::Error> {
        let res = sqlx::query("DELETE FROM agent_service WHERE agent_id = $1 AND service_id = $2")
            .bind(agent_id)
            .bind(service_id)
            .execute(conn)
            .await?;
        Ok(res.rows_affected())
    }
}
