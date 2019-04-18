BDSH:=./build/build.sh
clear:
	rm -rf ./out
chmodx:
	$(shell chmod +x ./build/build.sh)
linux: 
	$(BDSH)  linux amd64
	$(BDSH)  linux arm
darwin: 
	$(BDSH)  darwin amd64
build: clear linux darwin