package worker

import (
	"context"
	"cron-worker/common"
	"go.etcd.io/etcd/clientv3"
)

// 分布式锁
type JobLock struct {
	kv clientv3.KV
	lease clientv3.Lease
	jobName string
	cancelFunc context.CancelFunc  // 用于取消自动续租
	leaseID clientv3.LeaseID
	isLocked bool
}

func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease)(jobLock *JobLock){
	jobLock = &JobLock{
		kv:      kv,
		lease:   lease,
		jobName: jobName,
	}
	return
}

func (jobLock *JobLock) TryLock() (err error) {
	var (
		leaseGrantResp *clientv3.LeaseGrantResponse
		cancelCtx context.Context
		cancelFunc context.CancelFunc
		leaseID clientv3.LeaseID
		keepRespChan <- chan *clientv3.LeaseKeepAliveResponse
		txn clientv3.Txn
		lockKey string
		txnResp *clientv3.TxnResponse
	)

	// 创建租约
	if leaseGrantResp, err = jobLock.lease.Grant(context.TODO(), 5);err != nil{
		return
	}
	cancelCtx, cancelFunc = context.WithCancel(context.TODO())
	leaseID = leaseGrantResp.ID
	// 自动续租
	if keepRespChan, err = jobLock.lease.KeepAlive(cancelCtx, leaseID); err != nil{
		goto FAIL
	}
	go func() {
		var (
			keepResp *clientv3.LeaseKeepAliveResponse
		)
		for {
			select {
			case keepResp = <- keepRespChan:  // 自动续租的应答
				if keepResp == nil {}
				goto END
			}
		}
		END:
	}()
	// 创建事务
	txn = jobLock.kv.Txn(context.TODO())
	lockKey = common.JobKeyDIR + jobLock.jobName
	// 事务抢锁
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=",0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseID))).
		Else(clientv3.OpGet(lockKey))
	if txnResp, err = txn.Commit(); err != nil{
		goto FAIL
	}
	// 成功返回 失败释放租约
	if !txnResp.Succeeded { // 锁未占用
		err = common.ErrLockAlreadyRequired
		goto FAIL
	}
	// 上锁成功
	jobLock.leaseID = leaseID
	jobLock.cancelFunc = cancelFunc
	jobLock.isLocked = true
	return
FAIL:
	cancelFunc()
	jobLock.lease.Revoke(context.TODO(), leaseID)  // 释放租约
	return
}

func (jobLock *JobLock) UnLock() {
	if jobLock.isLocked{
		jobLock.cancelFunc()
		jobLock.lease.Revoke(context.TODO(), jobLock.leaseID)
	}
}