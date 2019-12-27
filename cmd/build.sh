xgo --targets linux/amd64 .
mv cmd-linux-amd64 ytfs-node
 docker run -it --rm -v `pwd`:/src upx upx -9 /src/ytfs-node
