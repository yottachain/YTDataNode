package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/yottachain/YTDataNode/instance"
	log "github.com/yottachain/YTDataNode/logger"
	storage "github.com/yottachain/YTDataNode/storageNodeInterface"
	"github.com/yottachain/YTFS/util"
)

var srcData string

var searchCmd = &cobra.Command{
	Use:   "search",
	Short: "search src data in disk",
	Run: func(cmd *cobra.Command, args []string) {
		_ = search()
	},
}

func search() error {
	var sn storage.StorageNode

	sn = instance.GetStorageNode()
	if nil == sn {
		log.Printf("get storage node fail!")
		return fmt.Errorf("get storage node fail")
	}

	hexSrc, err := hex.DecodeString(srcData)
	if err != nil {
		fmt.Printf("hex DecodeString: %v\n", err)
		return err
	}

	hash := sha256.New()
	hash.Write(hexSrc)
	hashValue := hash.Sum(nil)

	chalenge, err := util.Bytes32XorUint32(hashValue, sn.Config().IndexID)
	if err != nil {
		fmt.Printf("xor cacle fail err: %s\n", err.Error())
		return err
	}

	res, err := sn.YTFS().NewCapProofGetAnswerByChallenge(chalenge, sn.Config().IndexID)

	if bytes.Equal(res, hexSrc) {
		fmt.Printf("src data: %s writen to disk by cap proof!\n", srcData)
	}

	return nil
}

func main() {
	searchCmd.Flags().StringVarP(&srcData, "src", "s", "1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c61a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6",
		"Is src data written into a capacity proof?")

	RootCommand := &cobra.Command{
		Short: "ytfs verify",
	}
	RootCommand.AddCommand(searchCmd)

	RootCommand.Execute()
}
