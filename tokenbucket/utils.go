package tokenbucket

import "time"

// dur converts a floating-point number representing seconds into a time.Duration.
// If the input is -1, the function returns -1, indicating an indefinite or unbounded duration.
func dur(f float64) time.Duration {
	if f == -1 {
		return -1
	}
	return time.Duration(f * float64(time.Second))
}

// keyWithPrefix creates a key by appending a specified key to a given prefix.
// This helps to ensure that the keys used for rate limiting are namespaced properly,
func keyWithPrefix(keyPrefix string, key string) string {
	return keyPrefix + ":" + key
}
