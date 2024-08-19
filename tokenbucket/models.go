package tokenbucket

type Limit struct {
	RefillPerSec  int
	WindowSeconds int
}

// LimitPerSecond creates a TokenLimit configuration for a rate limit that allows a specified number of tokens
// to be refilled per second with a burst capacity equal to the refill rate.
// Example: LimitPerSecond(5) creates a TokenLimit allowing 5 tokens per second with a burst capacity of 5.
func LimitPerSecond(limit int) Limit {
	return Limit{
		RefillPerSec:  limit,
		WindowSeconds: 1,
	}
}

// LimitPerMinute creates a TokenLimit configuration for a rate limit that allows a specified number of tokens
// to be refilled per minute with a burst capacity equal to the refill rate.
// Example: LimitPerMinute(300) creates a TokenLimit allowing 300 tokens per minute with a burst capacity of 300.
func LimitPerMinute(limit int) Limit {
	return Limit{
		RefillPerSec:  limit / 60,
		WindowSeconds: 60,
	}
}

// LimitPerHour creates a TokenLimit configuration for a rate limit that allows a specified number of tokens
// to be refilled per hour with a burst capacity equal to the refill rate.
// Example: LimitPerHour(18000) creates a TokenLimit allowing 18000 tokens per hour with a burst capacity of 18000.
func LimitPerHour(limit int) Limit {
	return Limit{
		RefillPerSec:  limit / 3600,
		WindowSeconds: 3600,
	}
}

type Result struct {
	Limit  Limit
	Tokens int
}
