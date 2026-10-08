use sqlx::PgPool;
use crate::api::service::service_dto::Service;
use crate::api::service::service_repository::ServiceRepository;

pub struct ServiceService {
    repository: ServiceRepository
}

impl ServiceService {
    pub fn new_from_pgpool(pgpool: PgPool) -> Self {
        ServiceService {
            repository: ServiceRepository::new(pgpool)
        }
    }

    pub async fn get_service_by_id(&self, id: i64) -> Result<Service, sqlx::Error> {
        self.repository.get_service_by_id(id).await
    }

    pub async fn get_all_services(&self) -> Result<Vec<Service>, sqlx::Error> {
        self.repository.get_all_services().await
    }

    pub async fn create_service(&self, service: &Service) -> Result<Service, sqlx::Error> {
        self.repository.create_service(service).await
    }

    pub async fn update_service(&self, service: &Service) -> Result<Service, sqlx::Error> {
        self.repository.update_service(service).await
    }

    pub async fn delete_service(&self, id: i64) -> Result<(), sqlx::Error> {
        self.repository.delete_service(id).await
    }
}
