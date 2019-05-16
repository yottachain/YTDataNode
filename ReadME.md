# YTDataNode

```
ytf矿机存储节点
```

使用
```bash
# 编译
make build
# 使用示例
1. 初始化节点
./out/linux-amd64-0.0.1/ytfs-node init
2. 修改node-config.json bplist 
vim ~/YTFS/node-config.json 
3. 启动
./out/linux-amd64-0.0.1/ytfs-node daemon
```

restful接口

serverHost = “127.0.0.1:9002”
+ 获取节点id “/api/v0/node/id”
+ 获取节点收益 “/api/v0/node/income”
+ 获取存储空间使用状况 “/api/v0/ytfs/state”