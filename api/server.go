package api

import (
	"net/http"

	node "github.com/yottachain/YTDataNode/storageNodeInterface"
)

// Server api服务
type Server struct {
	*http.Server
	sn node.StorageNode
}

// NewHTTPServer 创建http api服务器
func NewHTTPServer(sn node.StorageNode) *Server {
	var srv Server
	srv.Server = new(http.Server)
	//srv.sn = instance.GetStorageNode()
	srv.sn = sn
	return &srv
}

// Daemon 启动http服务
func (srv *Server) Daemon() error {
	srv.Addr = srv.sn.Config().APIListen
	srv.Handler = NewAPIHandler(srv)
	return srv.ListenAndServe()
}
