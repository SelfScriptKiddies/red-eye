use sqlx::PgPool;
use crate::api::stream::stream_dto::Stream;

pub struct StreamRepository {
    pgpool: PgPool
}

impl StreamRepository {
    pub(super) fn new(pgpool: PgPool) -> Self {
        StreamRepository {
            pgpool
        }
    }

    pub async fn get_packet_by_id(&self, stream_id: i32) -> Result<Stream, sqlx::Error> {
        sqlx::query_as::<_, Stream>(
            r#"
            SELECT id, src_ip, src_port, dst_ip, dst_port, protocol, active, packet_count, favorite, l7_protocol
            FROM stream
            WHERE id = $1
            "#)
            .bind(stream_id)
            .fetch_one(&self.pgpool)
            .await
    }

    pub async fn get_all_streams(&self) -> Result<Vec<Stream>, sqlx::Error> {
        sqlx::query_as::<_, Stream>(
            r#"
            SELECT id, src_ip, src_port, dst_ip, dst_port, protocol, active, packet_count, favorite, l7_protocol
            FROM stream
            "#)
            .fetch_all(&self.pgpool)
            .await
    }

    pub async fn get_stream_packet_timestamps(&self, stream_id: i32) -> Result<(Option<i64>, Option<i64>), sqlx::Error> {
        sqlx::query_as::<_, (Option<i64>, Option<i64>)>(
            r#"
            SELECT MIN(timestamp) as first_time, MAX(timestamp) as last_time
            FROM packets
            WHERE stream_id = $1
            "#)
            .bind(stream_id)
            .fetch_one(&self.pgpool)
            .await
    }

    pub async fn get_streams_with_tags(&self, limit: i32, offset: i32, tags: Option<Vec<i32>>, services: Option<Vec<i32>>) -> Result<Vec<(i32, Option<i64>, Option<i64>, i64, Vec<String>, Option<String>, Option<i32>, bool, String, Option<String>, Option<String>)>, sqlx::Error> {
        let mut query_parts = Vec::new();
        let mut params = Vec::new();

        query_parts.push(r#"
            SELECT
                p.stream_id,
                MIN(p.timestamp) as first_time,
                MAX(p.timestamp) as last_time,
                COUNT(DISTINCT p.id) as packet_count,
                COALESCE(array_agg(tn.name) FILTER (WHERE tn.name IS NOT NULL), ARRAY[]::text[]) as tags,
                s.name as service_name,
                s.id as service_id,
                str.favorite as favorite,
                str.l7_protocol as l7_protocol,
                str.src_ip::text as src_ip,
                str.dst_ip::text as dst_ip
            FROM packets p
            LEFT JOIN tag t ON t.packet_id = p.id
            LEFT JOIN tag_name tn ON tn.id = t.tag_id
            LEFT JOIN stream str ON str.id = p.stream_id
            LEFT JOIN service s ON s.id = str.service_id"#.to_string());

        let mut where_clauses = Vec::new();

        // Добавление фильтрации по тегам, если они указаны
        if let Some(tag_ids) = &tags {
            if !tag_ids.is_empty() {
                where_clauses.push(r#"p.stream_id IN (
                    SELECT p2.stream_id 
                    FROM packets p2
                    JOIN tag t2 ON t2.packet_id = p2.id 
                    JOIN tag_name tn2 ON tn2.id = t2.tag_id 
                    WHERE tn2.id = ANY($1)
                    GROUP BY p2.stream_id
                    HAVING COUNT(DISTINCT tn2.id) = array_length($1, 1)
                )"#.to_string());
                params.push(tag_ids.clone());
            }
        }

        // Добавление фильтрации по сервисам, если они указаны
        if let Some(service_ids) = &services {
            if !service_ids.is_empty() {
                let param_idx = params.len() + 1;
                where_clauses.push(format!("s.id = ANY(${}) ", param_idx));
                params.push(service_ids.clone());
            }
        }

        // Добавление WHERE к запросу, если есть условия
        if !where_clauses.is_empty() {
            query_parts.push(" WHERE ".to_string());
            query_parts.push(where_clauses.join(" AND "));
        }

        // Добавление GROUP BY и других частей запроса
        query_parts.push(r#"
            GROUP BY p.stream_id, s.name, s.id, str.favorite, str.l7_protocol, str.src_ip, str.dst_ip
            ORDER BY last_time DESC"#.to_string());

        // Добавление LIMIT и OFFSET
        let limit_idx = params.len() + 1;
        let offset_idx = params.len() + 2;
        query_parts.push(format!(" LIMIT ${} OFFSET ${}", limit_idx, offset_idx));
        
        // Построение полного запроса
        let query_string = query_parts.join(" ");
        
        // Вывод запроса в лог для отладки
        log::debug!("SQL Query: {}", query_string);
        log::debug!("Params: tags={:?}, services={:?}, limit={}, offset={}", tags, services, limit, offset);
        
        // Выполнение запроса с параметрами
        let mut query = sqlx::query_as::<_, (i32, Option<i64>, Option<i64>, i64, Vec<String>, Option<String>, Option<i32>, bool, String, Option<String>, Option<String>)>(&query_string);
        
        // Биндинг параметров tags и services
        for param in &params {
            query = query.bind(param);
        }
        
        // Биндинг limit и offset
        query = query.bind(limit).bind(offset);
        
        query.fetch_all(&self.pgpool).await
    }

    pub async fn set_favorite(&self, stream_id: i32, favorite: bool) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar::<_, bool>(
            r#"
            UPDATE stream
            SET favorite = $2
            WHERE id = $1
            RETURNING favorite
            "#,
        )
        .bind(stream_id)
        .bind(favorite)
        .fetch_one(&self.pgpool)
        .await
    }

    pub async fn delete_old_streams(&self, age_minutes: i64) -> Result<u64, sqlx::Error> {
        // Определяем порог в секундах для сравнения с полем timestamp в packets
        sqlx::query(
            r#"
            DELETE FROM stream s
            WHERE s.favorite = FALSE
            AND NOT EXISTS (
                SELECT 1 FROM packets p
                WHERE p.stream_id = s.id
                  AND p.timestamp >= (extract(epoch from now())::bigint - ($1 * 60))
            )
            "#,
        )
        .bind(age_minutes)
        .execute(&self.pgpool)
        .await
        .map(|res| res.rows_affected())
    }
}


