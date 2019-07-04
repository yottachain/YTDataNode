BDSH:=./build/build.sh
clear:
	rm -rf ./out
chmodx:
	$(shell chmod +x ./build/build.sh)
linux: 
	$(BDSH)  linux amd64
darwin: 
	$(BDSH)  darwin amd64
windows: 
	$(BDSH)  amd64
build: clear linux


publish: 
	./publish.sh