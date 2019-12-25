package worker

import (
	"crontab/worker/common"
	"math/rand"
	"os/exec"
	"time"
)

type Executor struct {

}

var (
	GExecutor *Executor
)

func (executor *Executor) ExecuteJob(jobExecuteStatus *common.JobExecuteStatus) {
	go func() {
		var (
			cmd *exec.Cmd
			output []byte
			err error
			result *common.JobExecuteResult
			jobLock *JobLock
		)
		// 初始化锁
		jobLock = GJobMgr.CreateJobLock(jobExecuteStatus.JOB.Name)

		// 初始化任务结果
		result = &common.JobExecuteResult{
			JOBExecuteStatus: jobExecuteStatus,
			Output:           make([]byte, 0),
		}

		err = jobLock.TryLock()
		time.Sleep(time.Duration(rand.Intn(1000))* time.Millisecond)
		defer jobLock.UnLock()
		if err != nil{  // 上锁失败
			result.StartTime = time.Now()
			result.Err = err
			result.EndTime = time.Now()
		}else {
			// 执行shell 并获取输出
			result.StartTime = time.Now()
			cmd = exec.CommandContext(jobExecuteStatus.CancelCtx,"/bin/bash", "-c",jobExecuteStatus.JOB.Command)
			output, err =  cmd.CombinedOutput()
			result.Output = output
			result.Err = err
			result.EndTime = time.Now()
		}

		// 任务执行完成后，把执行的结果返回给scheduler，sheduler会从exectingTable中删除执行记录
		GScheduler.PushJobResult(result)

	}()
}

func InitExecutor()(err error) {
	GExecutor = &Executor{}
	return
}
