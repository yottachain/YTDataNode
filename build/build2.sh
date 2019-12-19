OS=$1
ARCH=$2

VERSION=0.0.1
FILENAME=ytfs-signer
OUTDIR=out2
MAINFILE=cmd/signer/signer.go
DIRNAME=${OUTDIR}/${OS}-${ARCH}-${VERSION}
FULLNAME=${DIRNAME}/${FILENAME}

mkdir ${DIRNAME}

GOOS=$OS GOARCH=$ARCH go build -ldflags "-s -w" -o ${FULLNAME} ${MAINFILE}

