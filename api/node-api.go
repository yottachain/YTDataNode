package api

import (
	"math/rand"
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
			ID string `json:"id"`
		}
		rw.WriteJSON(res{srv.sn.Host().ID().Pretty()})
	})
	// ytfs配置
	handler.HandleAPI("node/config", func(rw *ResponseWriter, rq *http.Request) {
		rw.WriteJSON(srv.sn.Config())
	})
	// addres
	handler.HandleAPI("node/address", func(rw *ResponseWriter, rq *http.Request) {
		rw.WriteJSON(srv.sn.Addrs())
	})
	// 查询收入
	handler.HandleAPI("node/income", func(rw *ResponseWriter, rq *http.Request) {
		type Res struct {
			Yesterday uint64 `json:"yesterday"`
			Total     uint64 `json:"total"`
		}
		// 未实现查询，先返回mock数据
		rw.WriteJSON(Res{10, 50})
	})
	// 查询硬盘使用状况
	handler.HandleAPI("ytfs/state", func(rw *ResponseWriter, rq *http.Request) {
		type Res struct {
			Used   uint64 `json:"Used"`
			Unused uint64 `json:"Unused"`
			Total  uint64 `json:"Total"`
		}
		const GB = 1024 * 1024 * 1024
		// 未实现查询，先返回mock数据
		res := new(Res)
		res.Total = srv.sn.YTFS().Meta().YtfsSize
		res.Used = rand.Uint64() % res.Total
		res.Unused = res.Total - res.Used
		rw.WriteJSON(res)
	})
}
