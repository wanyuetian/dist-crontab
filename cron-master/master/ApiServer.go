package master

import (
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"

	"cron-master/common"
)

// APIServer 任务的HTTP接口
type APIServer struct {
	httpServer *http.Server
}

var (
	// GAPIServer 单例对象
	GAPIServer *APIServer
)

// 保存任务接口
func handleJobSave(w http.ResponseWriter, r *http.Request) {
	// 任务保存到etcd中
	var (
		err     error
		postJob string
		job     common.Job
		oldJob  *common.Job
		bytes   []byte
	)
	// 1 解析POST表单
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	// 2 取表单中的job字段
	postJob = r.PostForm.Get("job")
	// 3 反序列化job
	if err = json.Unmarshal([]byte(postJob), &job); err != nil {
		goto ERR
	}
	// 4 保存对象
	if oldJob, err = GJobMgr.SaveJob(&job); err != nil {
		goto ERR
	}
	// 5 返回正常应答
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		w.Write(bytes)
	}
	return
ERR:
	// 异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}

func handleJobDelete(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		name   string
		oldJob *common.Job
		bytes  []byte
	)
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	name = r.PostForm.Get("name")
	if oldJob, err = GJobMgr.DeleteJob(name); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		w.Write(bytes)
	}
	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), ""); err == nil {
		w.Write(bytes)
	}
}

func handleJobList(w http.ResponseWriter, r *http.Request) {
	var (
		jobList []*common.Job
		err     error
		bytes   []byte
	)
	if jobList, err = GJobMgr.ListJob(); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildResponse(0, "success", jobList); err == nil {
		w.Write(bytes)
	}
	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), ""); err == nil {
		w.Write(bytes)
	}
}

func handleJobKill(w http.ResponseWriter, r *http.Request) {
	var (
		err   error
		name  string
		bytes []byte
	)
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	name = r.PostForm.Get("name")
	if err = GJobMgr.KillJob(name); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildResponse(0, "success", nil); err == nil {
		w.Write(bytes)
	}
	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), ""); err == nil {
		w.Write(bytes)
	}
}

// 查询任务日志
func handleJobLog(w http.ResponseWriter, r *http.Request) {
	var (
		err        error
		name       string // 任务名
		skipParam  string
		limitParam string
		skip       int
		limit      int
		bytes      []byte
		logArr     []*common.JobLog
	)
	// 解析GET参数
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	name = r.Form.Get("name")
	skipParam = r.Form.Get("skip")
	limitParam = r.Form.Get("limit ")
	if skip, err = strconv.Atoi(skipParam); err != nil {
		skip = 0
	}

	if limit, err = strconv.Atoi(limitParam); err != nil {
		limit = 20
	}
	if logArr, err = GLogMgr.ListLog(name, int64(skip), int64(limit)); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildResponse(0, "success", logArr); err == nil {
		w.Write(bytes)
	}
	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), ""); err == nil {
		w.Write(bytes)
	}
}

// 获取健康worker节点
func handleWorkList(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		bytes     []byte
		workerArr []string
	)
	if workerArr, err = GWorkerMgr.ListWorkers(); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildResponse(0, "success", workerArr); err == nil {
		w.Write(bytes)
	}
	return
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), ""); err == nil {
		w.Write(bytes)
	}
}

// InitAPIServer 初始化服务
func InitAPIServer() (err error) {
	var (
		mux           *http.ServeMux
		listener      net.Listener
		httpServer    *http.Server
		staticDIR     http.Dir     // 静态文件根目录
		staticHandler http.Handler // 静态文件的HTTP回调
	)
	// 配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/job/log", handleJobLog)
	mux.HandleFunc("/worker/list", handleWorkList)
	staticDIR = GConfig.WebRoot
	staticHandler = http.FileServer(staticDIR)
	mux.Handle("/", http.StripPrefix("/", staticHandler))
	// 启动TCP监听
	if listener, err = net.Listen("tcp", ":"+strconv.Itoa(GConfig.APIPort)); err != nil {
		return
	}
	// 创建一个HTTP服务
	httpServer = &http.Server{
		ReadTimeout:  time.Duration(GConfig.APIReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(GConfig.APIWriteTimeout) * time.Millisecond,
		Handler:      mux,
	}
	// 赋值单例
	GAPIServer = &APIServer{
		httpServer: httpServer,
	}
	// 启动服务端
	go httpServer.Serve(listener)
	return
}
