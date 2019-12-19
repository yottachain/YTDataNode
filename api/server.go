package api

import (
	"net/http"

	node "github.com/yottachain/YTDataNode"
	"github.com/yottachain/YTDataNode/instance"
)

// Server api服务
type Server struct {
	*http.Server
	sn node.StorageNode
}

// NewHTTPServer 创建http api服务器
func NewHTTPServer() *Server {
	var srv Server
	srv.Server = new(http.Server)
	srv.sn = instance.GetStorageNode()
	return &srv
}

// Daemon 启动http服务
func (srv *Server) Daemon() error {
	srv.Addr = srv.sn.Config().APIListen
	srv.Handler = NewAPIHandler(srv)
	return srv.ListenAndServe()
}
