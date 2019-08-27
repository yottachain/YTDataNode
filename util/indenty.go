package util

import (
	peer "github.com/libp2p/go-libp2p-peer"
	"log"

	"github.com/mr-tron/base58"

	crypto "github.com/libp2p/go-libp2p-crypto"
	ci "github.com/yottachain/YTCrypto"
)

// RandomIdentity generates a random identity (default behaviour)
func RandomIdentity() (crypto.PrivKey, error) {
	privstr, _ := ci.CreateKey()
	pr, _ := base58.Decode(privstr)
	priv, err := crypto.UnmarshalSecp256k1PrivateKey(pr[1:33])
	// log.Println(privstr)
	// priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	if err != nil {
		return nil, err
	}
	return priv, err
}

func Libp2pPkey2eosPkey(privkey string) (string, error) {
	pr, err := base58.Decode(privkey)
	if err == nil {
		pr = append([]byte{0x80}, pr[0:32]...)
	} else {
		return "", err
	}
	return base58.Encode(pr), nil
}

// RandomIdentity2 generates a random identity (default behaviour)
func RandomIdentity2() (crypto.PrivKey, string, error) {
	privstr, pubstr := ci.CreateKey()
	pr, _ := base58.Decode(privstr)
	log.Println(pr[1:33])
	priv, err := crypto.UnmarshalSecp256k1PrivateKey(pr[1:33])
	// log.Println(privstr)
	// priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	if err != nil {
		return nil, "", err
	}
	return priv, pubstr, err
}

func GetPublicKey(privkey string) (string, error) {
	return ci.GetPublicKeyByPrivateKey(privkey)
}

func IdFromPublicKey(publicKey string) (peer.ID, error) {
	bytes, err := base58.Decode(publicKey)
	if err != nil {
		return "", err
	}
	rawpk, err := crypto.UnmarshalSecp256k1PublicKey(bytes[0:33])
	if err != nil {
		return "", err
	}
	id, err := peer.IDFromPublicKey(rawpk)
	return id, nil
}
