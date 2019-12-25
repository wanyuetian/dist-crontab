package master

import (
	"context"
	"encoding/json"
	"time"

	"go.etcd.io/etcd/mvcc/mvccpb"

	"cron-master/common"

	"go.etcd.io/etcd/clientv3"
)

// JobMgr 任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	// GJobMgr 单例对象
	GJobMgr *JobMgr
)

// InitJobMgr 初始化管理器
func InitJobMgr() (err error) {
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

	GJobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	return
}

// SaveJob 保存job到etcd
func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	// 把任务保存到 /cron/jobs/任务名 -> json
	var (
		jobKey   string
		jobValue []byte
		putResp  *clientv3.PutResponse
	)
	jobKey = common.JobSaveDIR + job.Name
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}
	// 保存到etcd
	if putResp, err = jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}
	if putResp.PrevKv != nil {
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJob); err != nil {
			err = nil
			return
		}
	}
	return
}

// DeleteJob 删除任务
func (jobMgr *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {
	var (
		jobKey     string
		deleteResp *clientv3.DeleteResponse
	)
	jobKey = common.JobSaveDIR + name
	if deleteResp, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}
	if len(deleteResp.PrevKvs) != 0 {
		if err = json.Unmarshal(deleteResp.PrevKvs[0].Value, &oldJob); err != nil {
			err = nil
			return
		}
	}
	return
}

// ListJob 删除任务
func (jobMgr *JobMgr) ListJob() (jobList []*common.Job, err error) {
	var (
		dirKey  string
		getResp *clientv3.GetResponse
		kvPair  *mvccpb.KeyValue
		job     *common.Job
	)
	dirKey = common.JobSaveDIR
	if getResp, err = jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}
	jobList = make([]*common.Job, 0)
	for _, kvPair = range getResp.Kvs {
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value, job); err != nil {
			continue
		}
		jobList = append(jobList, job)
	}
	return
}

// KillJob 杀死任务
func (jobMgr *JobMgr) KillJob(name string) (err error) {
	// 跟新一下key=/cron/killer/任务名
	var (
		killKey        string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseID        clientv3.LeaseID
	)
	killKey = common.JobKillerDIR + name

	// 让worker监听到一次put操作，创建一个租约让其稍后自动过期即可
	if leaseGrantResp, err = jobMgr.lease.Grant(context.TODO(), 1); err != nil {
		return
	}
	leaseID = leaseGrantResp.ID
	if _, err = jobMgr.kv.Put(context.TODO(), killKey, "", clientv3.WithLease(leaseID)); err != nil {
		return
	}
	return
}
