package api

import (
	"fmt"
	"github.com/yottachain/YTDataNode/cmd/update"
	"github.com/yottachain/YTDataNode/logger"
	"net/http"
	"os"
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
	// 查询收入
	handler.HandleAPI("node/income", func(rw *ResponseWriter, rq *http.Request) {
		type Res struct {
			Yesterday uint64 `json:"yesterday"`
			Total     uint64 `json:"total"`
		}
		// 未实现查询，先返回mock数据
		rw.WriteJSON(Res{10, 50})
	})
	// 已连接节点
	handler.HandleAPI("node/conns", func(rw *ResponseWriter, rq *http.Request) {
		//var res []string
		//conns := srv.sn.Host().Network().Conns()
		//if len(conns) > 0 {
		//	res = make([]string, len(conns))
		//}
		//for k, v := range conns {
		//	res[k] = fmt.Sprintf("%s/p2p/%s", v.RemoteMultiaddr(), v.RemotePeer().Pretty())
		//}
		//// 未实现查询，先返回mock数据
		//rw.WriteJSON(res)
	})
	// 已添加节点
	handler.HandleAPI("node/peers", func(rw *ResponseWriter, rq *http.Request) {
		//var res []string
		//conns := srv.sn.Host().Peerstore().Peers()
		//if len(conns) > 0 {
		//	res = make([]string, len(conns))
		//}
		//for k, v := range conns {
		//	res[k] = v.Pretty()
		//}
		//// 未实现查询，先返回mock数据
		//rw.WriteJSON(res)
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
		// 未实现查询，先返回mock数据
		res := new(Res)
		res.Total = srv.sn.YTFS().Meta().YtfsSize
		res.Used = srv.sn.YTFS().Len() * uint64(srv.sn.YTFS().Meta().DataBlockSize)
		res.Unused = res.Total - res.Used
		res.ProductSpace = srv.sn.Owner().BuySpace * uint64(srv.sn.YTFS().Meta().DataBlockSize)
		log.Println(srv.sn.YTFS().Meta().DataBlockSize, "data size")
		rw.WriteJSON(res)
	})
	// 强制更新
	handler.HandleAPI("debug/update", func(rw *ResponseWriter, rq *http.Request) {
		fmt.Println("准备更新")
		if err := update.UpdateForce(); err != nil {
			rw.Write([]byte(err.Error()))
		} else {
			rw.Write([]byte("ok"))
		}
		defer os.Exit(0)
	})
}
