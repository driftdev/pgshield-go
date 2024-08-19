# PgShield Go

PgShield Go is a powerful rate limiting package for Go, utilizing PostgreSQL or YugabyteDB to manage request rates and handle traffic surges efficiently. 
It leverages the token bucket algorithm to provide smooth and reliable rate limiting.

## Features

- **Database Scalability**: Operates with PostgreSQL for single-node scenarios or YugabyteDB for distributed horizontal scalability, adapting to your infrastructure needs.

- **Token Bucket Algorithm**: Implements rate limiting by allowing requests to accumulate tokens at a fixed rate, handling bursts and smoothing traffic while enforcing a maximum burst size.

- **Multi-System Support**: Compatible with both PostgreSQL and YugabyteDB, offering flexibility for integration with your existing database systems.

- **High Performance**: Engineered for high performance and low latency, making it well-suited for high-throughput applications.

- **Configurable Limits**: Easily set rate limits according to your application’s requirements, including maximum request rates and burst capacities.

- **Transactional Reliability**: Utilizes PL/pgSQL functions to execute all logic within the database, ensuring transactional reliability and ACID compliance.

## Installation

To install PgShield Go, use the following command:

```bash
go get github.com/driftdev/pgshield-go
```