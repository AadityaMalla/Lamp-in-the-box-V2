<!--
  Author : Aaditya Malla (CSCI 312 - Group 4)
  File   : README.md
-->
# Docker + Delta Lake: CSCI 312

An automated, containerized deployment of a distributed system on an AWS EC2 VPS (AlmaLinux 9).
A client web page sends queries that pass through three Docker containers and end at a
**Delta Lake** table stored in an **AWS S3 bucket**.

| Container | Technology | Role |
|---|---|---|
| `nginx` | NGINX | Serves the client page, reverse-proxies `/api/` to Rocket. Only public port (80). |
| `rocket` | Rust (Rocket) | Validates client input, forwards queries to Spark. |
| `spark` | Spark SQL + Delta Lake (PySpark) | Reads and writes the Delta table at `s3a://csci312-delta-aaditya-2026/delta/students`. |

## Request flow

    Browser -> nginx:80 -> rocket:8000 -> spark:9000 -> Delta Lake table in S3
               \___________ Docker network "deltanet" ___________/

Containers find each other by name (`rocket`, `spark`) on a user-defined bridge network.

## AWS prerequisites

1. S3 bucket `csci312-delta-aaditya-2026` (us-east-2, public access blocked).
2. IAM policy `DeltaLakeBucketAccess` granting List/Get/Put/Delete on that bucket only.
3. IAM role `EC2-DeltaLake-Role` (trusted by EC2) with that policy, attached to the instance.
4. Instance metadata hop limit set to 2, so containers can use the role's credentials.

No AWS access keys are stored anywhere in this project.

## Deploy

    ./deploy.sh            # or: docker compose up -d --build

## API (through NGINX)

| Method | Path | Description |
|---|---|---|
| GET | `/api/students?major=&min_gpa=&version=` | Filtered Spark SQL query; `version` = Delta time travel |
| POST | `/api/students` | Add a student (JSON: name, major, gpa, year); creates a new Delta version |
| GET | `/api/history` | Delta Lake table history |

## Security notes

- Rocket validates all input (letters/spaces only, numeric ranges) before it reaches Spark.
- Spark uses named SQL parameters (`:major`, `:min_gpa`), never string concatenation.
- The client page renders data with `textContent`, preventing HTML/script injection.
- Bucket access comes from a least-privilege IAM role; the Spark UI is bound to localhost.

## Files

- `docker-compose.yml`: automated multi-container deployment
- `deploy.sh`: one-command deploy with readiness check
- `spark/`: `server.py`, `warmup.py`, `Dockerfile`
- `rocket/`: `src/main.rs`, `Cargo.toml`, `Dockerfile`
- `nginx/`: `index.html`, `default.conf.template`, `Dockerfile`
