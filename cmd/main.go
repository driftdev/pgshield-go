package main

import (
	"context"
	"fmt"
	"github.com/driftdev/pgshield-go"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
)

func main() {
	pgxPool, err := pgxpool.New(context.Background(), "postgresql://dev:dev@localhost:5432/postgres")
	if err != nil {
		log.Fatal(err)
	}

	limiter := pgshield.NewTokenBucketRateLimiter(pgxPool, pgshield.DefaultOptions())

	for i := 0; i < 100; i++ {
		res, err := limiter.ConsumeTokens(context.Background(), "user:1234", pgshield.PerSecond(10))
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(res.Limit.RefillPerSec, res.Limit.WindowSeconds, res.Tokens)
	}
}
