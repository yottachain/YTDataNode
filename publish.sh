#!/usr/bin/env bash

version=0.0.3

rm -rf ./out/
./build/build.sh linux amd64

#scp -i /Users/mac/Desktop/miyao/ytl.pem -P 52485 ./out/darwin-amd64-0.0.1/ytfs-node root@39.97.41.155:/mnt/www/download/ytfs-node-darwin
scp -i /Users/mac/Desktop/miyao/ytl.pem -P 52485 ./out/linux-amd64-0.0.1/ytfs-node root@39.97.41.155:/mnt/www/download/test-miner/ytfs-node
#scp -i /Users/mac/Desktop/miyao/ytl.pem -P 52485 ./out/windows-amd64-0.0.1/ytfs-node.exe root@39.97.41.155:/mnt/www/do