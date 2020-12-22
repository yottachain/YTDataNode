package transaction

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"time"
)

func PKCS7Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func PKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

//AES加密
func AesEncrypt(origData, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	blockSize := block.BlockSize()
	origData = PKCS7Padding(origData, blockSize)
	blockMode := cipher.NewCBCEncrypter(block, key[:blockSize])
	crypted := make([]byte, len(origData))
	blockMode.CryptBlocks(crypted, origData)
	return crypted, nil
}

//AES解密
func AesDecrypt(crypted, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	blockSize := block.BlockSize()
	blockMode := cipher.NewCBCDecrypter(block, key[:blockSize])
	origData := make([]byte, len(crypted))
	blockMode.CryptBlocks(origData, crypted)
	origData = PKCS7UnPadding(origData)

	return origData, nil
}

func MakeKey(key string) []byte {
	m5 := md5.New()
	m5.Reset()
	m5.Write([]byte(key))
	buf := m5.Sum(nil)
	return buf[:]
}

func MakeTimeKey() []byte {
	return MakeKey(time.Now().Format("y2006o02t01t03a"))
}

func Encode(data string) (string, error) {
	key := MakeTimeKey()
	buf, err := AesEncrypt([]byte(data), key)
	if err != nil {
		return "", err
	}
	return "TD" + hex.EncodeToString(buf), nil
}

func Decode(data string) (string, error) {
	key := MakeTimeKey()

	head := data[:2]
	if head != "TD" {
		return "", fmt.Errorf("head no TD")
	}

	ctx := data[2:]
	buf, err := hex.DecodeString(ctx)
	if err != nil {
		return "", nil
	}

	res, err := AesDecrypt(buf, key)
	if err != nil {
		return "", err
	}
	return string(res), nil
}
