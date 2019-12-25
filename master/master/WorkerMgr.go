package master

import (
	"context"
	"master/common"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

type WorkerMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	GWorkerMgr *WorkerMgr
)

// ListWorkers 获取在线worker列表
func (workerMgr *WorkerMgr) ListWorkers() (workerArr []string, err error) {
	var (
		getResp  *clientv3.GetResponse
		kvPair   *mvccpb.KeyValue
		workerIP string
	)
	// 初始化数组
	workerArr = make([]string, 0)
	// 获取目录下所有kv
	if getResp, err = workerMgr.kv.Get(context.TODO(), common.JobWorkerDir, clientv3.WithPrefix()); err != nil {
		return
	}
	for _, kvPair = range getResp.Kvs {
		workerIP = common.ExtractWorkerIP(string(kvPair.Key))
		workerArr = append(workerArr, workerIP)
	}
	return
}

func InitWorkerMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
	)

	config = clientv3.Config{
		Endpoints:   GConfig.EtcdEndpoints,
		DialTimeout: time.Duration(GConfig.EtcdDialTimeout) * time.Millisecond,
	}

	if client, err = clientv3.New(config); err != nil {
		return
	}

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	GWorkerMgr = &WorkerMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return
}
