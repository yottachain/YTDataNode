BDSH:=./build/build.sh
clear:
	rm -rf ./out
chmodx:
	$(shell chmod +x ./build/build.sh)
linux: 
	$(BDSH)  linux amd64
darwin: 
	$(BDSH)  darwin amd64
build: clear linux
upload:
    scp ./out/linux-amd64-0.0.1/ytfs-node root@152.136.13.254:/root/
publish: build upload