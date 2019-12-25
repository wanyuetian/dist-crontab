package worker

import (
	"context"
	"time"

	"crontab/worker/common"
	"go.etcd.io/etcd/mvcc/mvccpb"

	"go.etcd.io/etcd/clientv3"
)

// JobMgr 任务管理器
type JobMgr struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var (
	// GJobMgr 单例对象
	GJobMgr *JobMgr
)

// InitJobMgr 初始化管理器
func InitJobMgr() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		watcher clientv3.Watcher
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
	watcher = clientv3.NewWatcher(client)

	// 赋值单例
	GJobMgr = &JobMgr{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}
	// 启动任务监听
	err = GJobMgr.WatchJobs()
	GJobMgr.WatchKillers()
	return
}

// WatchKillers 监听强杀任务通知
func (jobMgr *JobMgr) WatchKillers(){
	var (
		watchChan     clientv3.WatchChan
		watchResp     clientv3.WatchResponse
		watchEvent    *clientv3.Event
		jobEvent      *common.JobEvent
		jobName string
		job *common.Job
	)
	// 监听 /cron/killer 目录
	go func() {
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JobKillerDIR, clientv3.WithPrefix())
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT:  // 强杀任务
					jobName = common.ExtractKillerName(string(watchEvent.Kv.Key))
					job = &common.Job{
						Name:     jobName,
					}
					jobEvent = common.BuildJobEvent(common.JobEventKill, job)
					// 事件推送给scheduler
					GScheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE:  // killer标记过期

				}
			}
		}
	}()


	return
}

// WatchJobs 监听任务变化
func (jobMgr *JobMgr) WatchJobs() (err error) {
	var (
		getResp       *clientv3.GetResponse
		kvPair        *mvccpb.KeyValue
		job           *common.Job
		watchStartRev int64
		watchChan     clientv3.WatchChan
		watchResp     clientv3.WatchResponse
		watchEvent    *clientv3.Event
		jobName       string
		jobEvent      *common.JobEvent
	)
	// get /cron/jobs/目录下的所有任务 并获取当前集群的revision
	if getResp, err = jobMgr.kv.Get(context.TODO(), common.JobSaveDIR, clientv3.WithPrefix()); err != nil {
		return
	}
	for _, kvPair = range getResp.Kvs {
		// 反序列化
		//fmt.Println(string(kvPair.Value))
		if job, err = common.UnpackJob(kvPair.Value); err == nil {
			jobEvent = common.BuildJobEvent(common.JobEventSave, job)
			// TODO: 把这个job同步给sehedule(调度协程)
			GScheduler.PushJobEvent(jobEvent)
		} else {
			continue
		}
	}
	// 2 从该revision向后监听变化事件
	go func() {
		watchStartRev = getResp.Header.Revision + 1
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JobSaveDIR, clientv3.WithRev(watchStartRev), clientv3.WithPrefix())
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT:
					// TODO 反序列化Job 推送一个更新事件给schedule
					if job, err = common.UnpackJob(watchEvent.Kv.Value); err != nil {
						// TODO: 把这个job同步给sehedule(调度协程)
						continue
					}
					// 构建一个Event事件
					jobEvent = common.BuildJobEvent(common.JobEventSave, job)
					// 推送一个更新事件给schedule
					GScheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE:
					// TODO 反序列化Job 推送一个删除事件给schedule
					jobName = common.ExtractJobName(string(watchEvent.Kv.Key))
					job = &common.Job{
						Name: jobName,
					}
					// 构建一个Event事件
					jobEvent = common.BuildJobEvent(common.JobEventDelete, job)
					// 推送一个删除事件给schedule
					GScheduler.PushJobEvent(jobEvent)
				}
			}
		}
	}()


	return
}


// 创建任务执行锁
func (jobMgr *JobMgr) CreateJobLock(jobName string) (jobLock *JobLock){
	jobLock = InitJobLock(jobName, jobMgr.kv, jobMgr.lease)
	return
}