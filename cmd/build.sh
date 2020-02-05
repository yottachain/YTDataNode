xgo -targets=linux/amd64 -out=ytfs-node .
mv ytfs-node-linux-amd64 ytfs-node
docker run -it --rm -v `pwd`:/src upx upx -9 /src/ytfs-node
# upx -9 ytfs-node