package util

import (
	"crypto/rand"

	crypto "github.com/libp2p/go-libp2p-crypto"
)

// RandomIdentity generates a random identity (default behaviour)
func RandomIdentity() (crypto.PrivKey, error) {
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	if err != nil {
		return nil, err
	}
	return priv, err
}
