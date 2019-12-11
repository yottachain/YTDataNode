rm -rf ./out
go build -o ./out/ytfs-node ./cmd/cmd.go
go build -o ./out/ytfs-signer ./cmd/signer/signer.go