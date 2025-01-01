# Red eye
## Traffic analyzer for A/D competition

## Backend
Backend consists of:
- Client - microservice, which captures packets and transfer them to analyzers through Kafka
- Analyzer - microservice, that processes packets, got from client, checking pattern matches and loading them to database
- Backend - microservice, that handles business logic for frontend and exposing REST-API
- Kafka - message broker between microservices
- Postgresql - database for packets
