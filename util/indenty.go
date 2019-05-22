package util

import (
	"bytes"

	"github.com/mr-tron/base58"

	crypto "github.com/libp2p/go-libp2p-crypto"
	ci "github.com/yottachain/YTCrypto"
)

// RandomIdentity generates a random identity (default behaviour)
func RandomIdentity() (crypto.PrivKey, error) {
	privstr, _ := ci.CreateKey()
	pr, _ := base58.Decode(privstr)
	priv, _, err := crypto.GenerateSecp256k1Key(bytes.NewBuffer(pr))
	// fmt.Println(privstr)
	// priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	if err != nil {
		return nil, err
	}
	return priv, err
}
