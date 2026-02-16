#!/bin/bash
# compatible with Linux & Git Bash (Windows 11)
set -e

# --- KONFIGURATION ---
PROJECT_NAME="fluxforge"
COMPOSE_FILE="./docker/test-env.yml"
MYSQL_DB="mysql_ref_db"
PG_DB="postgres_ref_db"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'

# Helper for  Docker Compose with Project-Prefix
function dcomp() {
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
}

case "$1" in
  start)
    echo -e "${GREEN}>>> Starting fluxforge Container...${NC}"
    dcomp up -d

    echo -e "${GREEN}>>> Waiting for Healthchecks...${NC}"

    # IDs of the containers
    MYSQL_ID=$(dcomp ps -q mysql-reference-db)
    PG_ID=$(dcomp ps -q postgres-reference-db)

    if [ -z "$MYSQL_ID" ] || [ -z "$PG_ID" ]; then
        echo -e "${RED}Error: Container could not be started.${NC}"
        exit 1
    fi

    # Waiting for MySQL
    until [ "$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_ID")" == "healthy" ]; do
        printf "${YELLOW}m${NC}"; sleep 1;
    done
    # Waiting for Postgres
    until [ "$(docker inspect -f '{{.State.Health.Status}}' "$PG_ID")" == "healthy" ]; do
        printf "${GREEN}p${NC}"; sleep 1;
    done

    echo -e "\n${GREEN}>>> Validating Testdata...${NC}"

    # validating MySQL
    M_CHECK=$(docker exec "$MYSQL_ID" mysql -uroot -proot -e "SHOW TABLES FROM $MYSQL_DB LIKE 'bla';" | grep "bla" || true)
    # validating Postgres
    P_CHECK=$(docker exec "$PG_ID" psql -U postgres -d "$PG_DB" -c "\dt" | grep "foo" || true)

    [ -z "$M_CHECK" ] && echo -e "${RED}MySQL ($MYSQL_DB) leer!${NC}" || echo -e "${GREEN}MySQL OK.${NC}"
    [ -z "$P_CHECK" ] && echo -e "${RED}Postgres ($PG_DB) leer!${NC}" || echo -e "${GREEN}Postgres OK.${NC}"
    ;;

  test)
    echo -e "${GREEN}>>> Preparing Infrastructure envirinment for Rust-Tests vor...${NC}"

    # URLs for Rust-Integration-Tests
    export MYSQL_URL_REFERENCE="mysql://root:root@127.0.0.1:3306/$MYSQL_DB"
    export POSTGRES_URL_REFERENCE="postgres://postgres:root@127.0.0.1:5432/$PG_DB"

    export MYSQL_URL_ADMIN="mysql://root:root@127.0.0.1:3306"
    export POSTGRES_URL_ADMIN="postgres://postgres:root@127.0.0.1:5432"

    cargo test --features integration-tests -- --nocapture
    ;;

  stop)
    echo -e "${YELLOW}>>> Stoping fluxforge Test Environment...${NC}"
    dcomp down -v
    ;;

  reset)
    $0 stop
    $0 start
    ;;

  *)
    echo -e "Usage: $0 {start|test|stop|reset}"
    exit 1
    ;;
esac




