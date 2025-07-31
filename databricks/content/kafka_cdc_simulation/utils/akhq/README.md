# AKHQ Kafka UI

This directory contains a `docker-compose.yml` file to run [AKHQ](https://akhq.io/), a web UI for viewing Kafka topics and browsing consumer groups.

## Prerequisites

- Docker and Docker Compose installed
- A `.env` file in the project root with the following variables:
  - `KAFKA_BROKERS`
  - `KAFKA_USERNAME`
  - `KAFKA_PASSWORD`

## How to Run

1.  **Navigate to this directory:**
    ```bash
    cd utils/akhq
    ```

2.  **Start the AKHQ container:**
    ```bash
    docker-compose up -d
    ```

3.  **Access the UI:**
    Open your web browser and go to [http://localhost:8080](http://localhost:8080).

4.  **To stop the container:**
    ```bash
    docker-compose down
    ``` 