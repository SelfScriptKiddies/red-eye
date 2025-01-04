use log::{debug, error, trace};
use rdkafka::producer::{FutureProducer, FutureRecord};
use crate::configuration::Configuration;

const KAFKA_TIMEOUT: &str = "5000";

#[derive(Clone)]
pub struct KafkaManager {
    kafka_producer: FutureProducer,
    broker: String,
    topic: String
}

impl KafkaManager {
    pub fn new(config: &Configuration) -> KafkaManager {
        let kafka_producer: FutureProducer = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", config.kafka_broker_host.clone())
            .set("message.timeout.ms", KAFKA_TIMEOUT)
            .create()
            .expect("Failed to create Kafka producer");

        KafkaManager {
            kafka_producer,
            broker: config.kafka_broker_host.clone(),
            topic: config.kafka_topic.clone()
        }
    }

    pub async fn send_packet<'a>(self: &Self, packet_data: Vec<u8>) {
        let topic = self.topic.clone();
        let kafka_producer = self.kafka_producer.clone();

        tokio::spawn(
            async move {
                let payload = packet_data;

                let record = FutureRecord::to(&topic)
                    .payload(&payload)
                    .key("raw");

                match kafka_producer.send(record, std::time::Duration::from_secs(0)).await {
                    Ok((partition, offset)) => {
                        debug!("Sent to Kafka topic {} partition {} at offset {}", &topic, partition, offset);
                    },
                    Err((e, _)) => {
                        error!("Failed to send message to Kafka: {}", e);
                    },
                }
            }
        );


    }
}