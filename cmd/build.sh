xgo -out ytfs-node --targets linux/amd64 .
mv ytfs-node-linux-amd64 ytfs-node
docker run -it --rm -v `pwd`:/src upx upx -9 /src/ytfs-node
