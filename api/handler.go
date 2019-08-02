package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Handler 请求处理器
type Handler struct {
	prefix  string
	version uint
	http.ServeMux
}

// NewHandler 创建新的处理器
func NewHandler() *Handler {
	var handler = new(Handler)
	handler.prefix = "/api"
	return handler
}

// HandleAPI 处理一个api
func (handler *Handler) HandleAPI(pattern string, handleFunc func(rwapi *ResponseWriter, rqapi *http.Request)) {
	handler.HandleFunc(fmt.Sprintf("%s/v%d/%s", handler.prefix, handler.version, pattern), func(rw http.ResponseWriter, rq *http.Request) {
		handleFunc(&ResponseWriter{rw}, rq)
	})
}

// ResponseWriter ..
type ResponseWriter struct {
	http.ResponseWriter
}

// WriteJSON 返回json
func (rw *ResponseWriter) WriteJSON(data interface{}) {
	res, err := json.Marshal(data)
	if err != nil {
		rw.WriteHeader(500)
		rw.Write([]byte("json format fail"))
	} else {
		rw.Write(res)
	}
}
