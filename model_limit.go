package pgshield

import (
	"fmt"
	"time"
)

// Limit represents the configuration for rate limiting. It includes the rate (number of allowed
// requests), burst (maximum number of requests allowed in a burst), and the period (duration for
// which the rate limit applies).
type Limit struct {
	// Rate specifies the number of requests allowed per period.
	Rate int
	// Burst specifies the maximum number of requests that can be handled in a burst.
	Burst int
	// Period specifies the duration for which the rate limit is applied (e.g., 1 minute, 1 hour).
	Period time.Duration
}

// String returns a string representation of the Limit, showing the rate, period, and burst capacity.
// Example: "10 req/1m (burst 10)" indicates 10 requests per minute with a burst capacity of 10 requests.
func (l Limit) String() string {
	return fmt.Sprintf("%d req/%s (burst %d)", l.Rate, fmtDur(l.Period), l.Burst)
}

// IsZero checks if the Limit is equal to the zero value for the Limit type.
// Returns true if Rate, Burst, and Period are all zero.
func (l Limit) IsZero() bool {
	return l == Limit{}
}

// fmtDur returns a string representation of the time.Duration for common periods.
// Example: "1s" for one second, "1m" for one minute, "1h" for one hour. For other durations, it returns the default string format.
func fmtDur(d time.Duration) string {
	switch d {
	case time.Second:
		return "s"
	case time.Minute:
		return "m"
	case time.Hour:
		return "h"
	}
	return d.String()
}

// PerSecond creates a Limit configuration for a rate limit that allows a specified number of requests
// per second with a burst capacity equal to the rate.
// Example: PerSecond(5) creates a Limit allowing 5 requests per second with a burst capacity of 5.
func PerSecond(rate int) Limit {
	return Limit{
		Rate:   rate,
		Period: time.Second,
		Burst:  rate,
	}
}

// PerMinute creates a Limit configuration for a rate limit that allows a specified number of requests
// per minute with a burst capacity equal to the rate.
// Example: PerMinute(100) creates a Limit allowing 100 requests per minute with a burst capacity of 100.
func PerMinute(rate int) Limit {
	return Limit{
		Rate:   rate,
		Period: time.Minute,
		Burst:  rate,
	}
}

// PerHour creates a Limit configuration for a rate limit that allows a specified number of requests
// per hour with a burst capacity equal to the rate.
// Example: PerHour(5000) creates a Limit allowing 5000 requests per hour with a burst capacity of 5000.
func PerHour(rate int) Limit {
	return Limit{
		Rate:   rate,
		Period: time.Hour,
		Burst:  rate,
	}
}

// Result holds the outcome of a rate limit check. It includes the limit configuration, the number of requests
// that were allowed, the remaining allowed requests, and the durations for retrying or resetting the rate limit.
type Result struct {
	// Limit contains the rate limiting configuration used for this result.
	Limit Limit
	// Allowed specifies the number of requests allowed in the current check.
	Allowed int
	// Remaining specifies the number of requests that can still be made before hitting the limit.
	Remaining int
	// RetryAfter indicates how long to wait before retrying if the limit is exceeded.
	RetryAfter time.Duration
	// ResetAfter indicates the time until the rate limit resets.
	ResetAfter time.Duration
}
