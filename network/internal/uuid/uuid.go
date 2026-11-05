// Package uuid provides minimal RFC 4122 version 4 UUID helpers — the
// identifiers only need to be unique, so no external dependency is pulled
// in for this.
package uuid

import "crypto/rand"

// New returns a random (version 4) UUID string.
func New() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("uuid: crypto/rand failed: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return format(b)
}

func format(b [16]byte) string {
	const hexDigits = "0123456789abcdef"
	buf := make([]byte, 36)
	pos := 0
	for i, v := range b {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			buf[pos] = '-'
			pos++
		}
		buf[pos] = hexDigits[v>>4]
		buf[pos+1] = hexDigits[v&0x0f]
		pos += 2
	}
	return string(buf)
}

// IsValid reports whether s looks like a UUID (8-4-4-4-12 hex digits).
func IsValid(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
			if !isHex {
				return false
			}
		}
	}
	return true
}
