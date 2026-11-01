package queue

import (
	"encoding/json"
	"fmt"
	"regexp"
	"unicode/utf8"
)

// Attributes are string key/value metadata carried with a message, apart
// from its body: trace context (W3C traceparent, tracestate), a content
// type, a correlation id. Kept as sent through retries, dead-lettering and
// replay. Not kept in memory: they are read with the body on delivery.
type Attributes map[string]string

const (
	MaxAttributes     = 16
	MaxAttributeBytes = 4096 // keys + values
)

// attrKey: starts with a letter or digit; letters, digits, '.', '_', '-'.
var attrKey = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// ValidateAttributes checks the limits: at most MaxAttributes keys of up to
// 64 characters, UTF-8 values, MaxAttributeBytes in total.
func ValidateAttributes(a Attributes) error {
	if len(a) > MaxAttributes {
		return fmt.Errorf("%w: at most %d attributes", ErrInvalidMessage, MaxAttributes)
	}
	size := 0
	for k, v := range a {
		if !attrKey.MatchString(k) {
			return fmt.Errorf("%w: attribute name %q: letters, digits, '.', '_', '-', up to 64, starting with a letter or digit", ErrInvalidMessage, k)
		}
		if !utf8.ValidString(v) {
			return fmt.Errorf("%w: attribute %q is not valid UTF-8", ErrInvalidMessage, k)
		}
		size += len(k) + len(v)
	}
	if size > MaxAttributeBytes {
		return fmt.Errorf("%w: attributes take %d bytes, max %d", ErrInvalidMessage, size, MaxAttributeBytes)
	}
	return nil
}

// encode is the stored form: JSON with sorted keys, "" for none.
func (a Attributes) encode() string {
	if len(a) == 0 {
		return ""
	}
	b, _ := json.Marshal(map[string]string(a)) // string map: can't fail
	return string(b)
}

func decodeAttributes(s string) (Attributes, error) {
	if s == "" {
		return nil, nil
	}
	var a Attributes
	if err := json.Unmarshal([]byte(s), &a); err != nil {
		return nil, fmt.Errorf("stored attributes: %w", err)
	}
	return a, nil
}

func (a Attributes) clone() Attributes {
	if len(a) == 0 {
		return nil
	}
	out := make(Attributes, len(a))
	for k, v := range a {
		out[k] = v
	}
	return out
}
