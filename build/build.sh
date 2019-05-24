OS=$1
ARCH=$2

VERSION=0.0.1
FILENAME=ytfs-node
OUTDIR=./out
MAINFILE=./cmd/cmd.go
DIRNAME=${OUTDIR}/${OS}-${ARCH}-${VERSION}
FULLNAME=${DIRNAME}/${FILENAME}

# GOOS=$OS GOARCH=$ARCH go build -o ${FULLNAME} ${MAINFILE}
docker run -i --rm -v $GOPATH:/go -w /go/src/github.com/yottachain/YTDataNode/ golang go build -o ${FULLNAME} ${MAINFILE}