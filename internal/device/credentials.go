package device

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
)

const secretBytes = 32

// SecretDigest is the only credential representation accepted by repositories.
type SecretDigest [sha256.Size]byte

func HashSecret(secret string) SecretDigest {
	return sha256.Sum256([]byte(secret))
}

func GenerateSecret() (string, error) {
	value := make([]byte, secretBytes)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
