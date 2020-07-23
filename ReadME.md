# YTDataNode
version: 1.0.1

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

## config
```bash
# node-config.json
{
  "ID": "16Uiu2HAm1V3Jzg4bCzjgcW91nMnssySz7bHxo363zyLzNajSH6QN",
#   超级节点列表
  "BPList": [
    {
      "ID": "16Uiu2HAkyHhwuzkR6fRhKbhUBVMySBKKtLCRkReYTJQEyfCkPSfN",
      "Addrs": [
        "/ip4/152.136.16.118/tcp/9999"
      ]
    },
    {
      "ID": "16Uiu2HAm9fBJNUzSD5V9aFJQQHbxE3rPsTiyrYk7vju18JCf3xm8",
      "Addrs": [
        "/ip4/152.136.17.115/tcp/9999"
      ]
    },
    {
      "ID": "16Uiu2HAkwNCD9HSH5hh36LmzgLjRcQiQFpT9spwspaAM5AH3rqA9",
      "Addrs": [
        "/ip4/152.136.18.185/tcp/9999"
      ]
    }
  ],
#   服务监听地址
  "ListenAddr": "/ip4/0.0.0.0/tcp/9001",
#   api监听地址
  "APIListen": ":9002",
}

```

当前版本1.0.2 功能：调整性能
