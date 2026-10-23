# docker-compose.yaml path
DOCKER_COMPOSE_FILE := docker-compose.yaml

# Stamp every image with the building commit — /api/version uses it to
# spot drift between backend / analyzer / frontend / agents. `export`
# propagates it into compose build args.
export RED_EYE_GIT_SHA ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)

# docker-compose command
DC := docker-compose -f $(DOCKER_COMPOSE_FILE)

# Output colors
GREEN=\033[0;32m
YELLOW=\033[0;33m
NC=\033[0m

.PHONY: help build up down restart clean logs ps prune

help:
	@echo -e "\n${GREEN}Available Commands:${NC}"
	@echo -e "  ${YELLOW}make build${NC}           - Build all Docker images"
	@echo -e "  ${YELLOW}make up${NC}              - Start containers with image rebuild"
	@echo -e "  ${YELLOW}make down${NC}            - Stop and remove containers"
	@echo -e "  ${YELLOW}make restart${NC}         - Restart containers with image rebuild"
	@echo -e "  ${YELLOW}make clean${NC}           - Stop containers and remove the database volume"
	@echo -e "  ${YELLOW}make logs${NC}            - View logs of all services"
	@echo -e "  ${YELLOW}make ps${NC}              - Show container status"
	@echo -e "  ${YELLOW}make prune${NC}           - Clean up unused Docker resources"
	@echo -e "  ${YELLOW}make help${NC}            - Show this help\n"

# Build all docker containers
build:
	@echo -e "${GREEN}building docker images...${NC}"
	$(DC) build --no-cache
	@echo -e "${GREEN}Building completed.${NC}\n"

# Starting containers with rebuild
up: build
	@echo -e "${GREEN}Building and starting containers...${NC}"
	$(DC) up -d --build
	@echo -e "${GREEN}Containers up.${NC}\n"

# Stopping and deleting containers
down:
	@echo -e "${GREEN}Stopping and deleting containers...${NC}"
	$(DC) down
	@echo -e "${GREEN}Containers stopped and deleted.${NC}\n"

# Restart containers with rebuild
restart: down up
	@echo -e "${GREEN}Containers restarted.${NC}\n"

# Cleaning: stopping containers and cleaning volumes
clean: down
	@echo -e "${GREEN}Deleting volume...${NC}"
	docker volume rm $(shell docker volume ls -q | grep db_data) || echo "${YELLOW}Volume db_data not found.${NC}"
	@echo -e "${GREEN}Database volume was cleaned.${NC}\n"

# Check container logs
logs:
	@echo -e "${GREEN}View all logs in real time. Enter Ctrl+C for exit.${NC}"
	$(DC) logs -f

# Container status
ps:
	@echo -e "${GREEN}Container status:${NC}"
	$(DC) ps
	@echo -e ""

# Run integration tests
test:
	@echo -e "${GREEN}Running integration tests...${NC}"
	python3 tests/integration_test.py --spawn-targets
	@echo -e "${GREEN}Tests completed.${NC}\n"

# Run stress test (5 min, 100 rps)
stress:
	@echo -e "${GREEN}Running stress test (5 min, 100 rps)...${NC}"
	python3 tests/stress_test.py --duration 300 --rps 100 --spawn-targets --output tests/stress_results.json
	@echo -e "${GREEN}Stress test completed.${NC}\n"

# Run full CTF simulation (1 hour, 500 rps)
stress-full:
	@echo -e "${GREEN}Running full stress test (1 hour, 500 rps)...${NC}"
	python3 tests/stress_test.py --duration 3600 --rps 500 --spawn-targets --output tests/stress_results_full.json
	@echo -e "${GREEN}Stress test completed.${NC}\n"

# Clean docker resorces
prune:
	@echo -e "${GREEN}Cleaning useless Docker-resources...${NC}"
	@echo -e "  - Deleting stopped containers, old networks and build cache."
	docker system prune -f
	@echo -e "${GREEN}Cleaning completed.${NC}\n"
