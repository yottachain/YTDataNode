package api

import (
	"log"
	"net/http"
)

// APIHandler api处理器
var handler = NewHandler()
var srv *Server

// NewAPIHandler 创建新的api处理器
func NewAPIHandler(server *Server) *Handler {
	srv = server
	return handler
}

func init() {
	// 获取矿机id
	handler.HandleAPI("node/id", func(rw *ResponseWriter, rq *http.Request) {
		type res struct {
			ID      string `json:"id"`
			IndexID uint32 `json:iddexId`
		}
		rw.WriteJSON(res{srv.sn.Host().Config().ID.Pretty(), srv.sn.Config().IndexID})
	})
	// ytfs配置
	handler.HandleAPI("node/config", func(rw *ResponseWriter, rq *http.Request) {
		rw.WriteJSON(srv.sn.Config())
	})
	// addres
	handler.HandleAPI("node/address", func(rw *ResponseWriter, rq *http.Request) {
		rw.WriteJSON(srv.sn.Addrs())
	})
	// 查询硬盘使用状况
	handler.HandleAPI("ytfs/state", func(rw *ResponseWriter, rq *http.Request) {
		type Res struct {
			Used         uint64 `json:"Used"`
			Unused       uint64 `json:"Unused"`
			Total        uint64 `json:"Total"`
			ProductSpace uint64 `json:"ProductSpace"`
		}
		const GB = 1024 * 1024 * 1024
		res := new(Res)
		res.Total = srv.sn.Config().AllocSpace
		res.Used = srv.sn.Config().AllocSpace
		res.Unused = res.Total - res.Used
		res.ProductSpace = srv.sn.Config().AllocSpace
		log.Println(srv.sn.Config().AllocSpace)
		rw.WriteJSON(res)
	})
}
