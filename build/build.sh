OS=$1
ARCH=$2

VERSION=0.0.1
FILENAME=ytfs-node
OUTDIR=./out
MAINFILE=./cmd/cmd.go
DIRNAME=${OUTDIR}/${OS}-${ARCH}-${VERSION}
FULLNAME=${DIRNAME}/${FILENAME}

if [ "$1" = "linux" ];then
docker run -i --rm -v $GOPATH:/go -w /go/src/github.com/yottachain/YTDataNode/ golang go build -o ${FULLNAME} ${MAINFILE}
# CGO_ENABLED=1 GOOS=$OS GOARCH=$ARCH CXX=/Users/mac/go/src/github.com/yottachain/YTDataNode/gcc/g++-linux-amd64 CC=/Users/mac/go/src/github.com/yottachain/YTDataNode/gcc/gcc-linux-amd64 go build -ldflags "-s -w" -o ${FULLNAME} ${MAINFILE}
else
GOOS=$OS GOARCH=$ARCH go build -ldflags "-s -w" -o ${FULLNAME} ${MAINFILE}
upx -9 ${FULLNAME}
fi
