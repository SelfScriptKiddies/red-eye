use actix_identity::Identity;
use actix_web::{HttpResponse, get, Responder, web};
use sqlx::PgPool;

#[get("/{id}")]
async fn get_packet(
    identity: Identity,
    path: web::Path<(u32,)>,
    pool: web::Data<PgPool>
) -> impl Responder {
    let packet_id: u32 = path.into_inner().0;
    HttpResponse::Ok().body(format!("User detail: {packet_id}"))
}



pub fn packet_config(cfg: &mut web::ServiceConfig) {
    cfg
        .service(
            web::scope("/packet")
                .service(get_packet)
        )

    ;
}