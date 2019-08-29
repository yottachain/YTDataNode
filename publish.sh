# scp -r ~/.ssh/id_rsa.pub root@152.136.11.50:/root/.ssh/authorized_keys
# scp -r ~/.ssh/id_rsa.pub root@152.136.13.254:/root/.ssh/authorized_keys
# scp -r ~/.ssh/id_rsa.pub root@152.136.18.74:/root/.ssh/authorized_keys
# scp -r ~/.ssh/id_rsa.pub root@152.136.17.121:/root/.ssh/authorized_keys
# scp -r ~/.ssh/id_rsa.pub root@152.136.18.176:/root/.ssh/authorized_keys

# ssh root@152.136.11.50 "pkill ytfs-node"
# ssh root@152.136.11.50 "pkill ytfs-node"
# ssh root@152.136.11.50 "pkill ytfs-node"
# ssh root@152.136.11.50 "pkill ytfs-node"
# ssh root@152.136.11.50 "pkill ytfs-node"

# scp -r ./out/linux-amd64-0.0.1/ytfs-node root@152.136.11.50:/root/ 
# scp -r ./out/linux-amd64-0.0.1/ytfs-node root@152.136.13.254:/root/ 
# scp -r ./out/linux-amd64-0.0.1/ytfs-node root@152.136.18.74:/root/ 
# scp -r ./out/linux-amd64-0.0.1/ytfs-node root@152.136.17.121:/root/ 
# scp -r ./out/linux-amd64-0.0.1/ytfs-node root@152.136.18.176:/root/ 

# ssh root@152.136.11.50 "cd /root/;nohup ytfs-node daemon &"
# ssh root@152.136.11.50 "cd /root/;nohup ytfs-node daemon &"
# ssh root@152.136.11.50 "cd /root/;nohup ytfs-node daemon &"
# ssh root@152.136.11.50 "cd /root/;nohup ytfs-node daemon &"
# ssh root@152.136.11.50 "cd /root/;nohup ytfs-node daemon &"

version=0.0.3


#scp -i /Users/mac/Desktop/miyao/ytl.pem -P 52485 ./out/darwin-amd64-0.0.1/ytfs-node root@39.97.41.155:/mnt/www/download/ytfs-node-darwin
scp -i /Users/mac/Desktop/miyao/ytl.pem -P 52485 ./out/linux-amd64-0.0.1/ytfs-node root@39.97.41.155:/mnt/www/download/ytfs-node
#scp -P 52485 ./out/windows-amd64-0.0.1/ytfs-node.exe root@39.97.41.155:/mnt/www/download/ytfs-node.exe

