package worker

import (
	"fmt"
	"time"

	"cron-worker/common"
)

// Scheduler 任务调度
type Scheduler struct {
	jobEventChan chan *common.JobEvent
	jobPlanTable map[string]*common.JobSchedulerPlan
	jobExecutingTable map[string]*common.JobExecuteStatus // 任务执行状态表
	jobResultChan chan *common.JobExecuteResult
}

// 处理任务
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	var (
		jobSchedulerPlan *common.JobSchedulerPlan
		err              error
		jobExecuteStatus *common.JobExecuteStatus
		jobExecuting bool
		jobExisted bool
	)

	switch jobEvent.EventType {
	case common.JobEventSave:
		if jobSchedulerPlan, err = common.BuildJobSchedulerPlan(jobEvent.JOB); err != nil {
			return
		}
		scheduler.jobPlanTable[jobEvent.JOB.Name] = jobSchedulerPlan

	case common.JobEventDelete:
		if jobSchedulerPlan, jobExisted = scheduler.jobPlanTable[jobEvent.JOB.Name]; jobExisted {
			delete(scheduler.jobPlanTable, jobEvent.JOB.Name)
		}
	case common.JobEventKill:
		if jobExecuteStatus, jobExecuting = scheduler.jobExecutingTable[jobEvent.JOB.Name]; jobExecuting{
			jobExecuteStatus.CancelFunc()
		}
	}
}

// 尝试执行任务
func (scheduler *Scheduler) TryStartJob(jobPlan *common.JobSchedulerPlan) {
	var (
		jobExecuteStatus *common.JobExecuteStatus
		jobExecuting bool
	)
	// 如果任务正在执行，跳过本次调度
	if jobExecuteStatus, jobExecuting = scheduler.jobExecutingTable[jobPlan.JOB.Name]; jobExecuting{
		fmt.Println("任务正在执行，跳过...", jobExecuteStatus.JOB.Name)
		return
	}
	// 构建执行状态信息
	jobExecuteStatus = common.BuildJobExecuteStatus(jobPlan)
	// 保存执行状态
	scheduler.jobExecutingTable[jobPlan.JOB.Name] = jobExecuteStatus
	// 执行任务
	fmt.Println("执行任务 ", jobExecuteStatus.JOB.Name, jobExecuteStatus.RealTime, jobExecuteStatus.PlanTime)
	GExecutor.ExecuteJob(jobExecuteStatus)
}

// TryScheduler 重新计算任务调度状态
func (scheduler *Scheduler) TryScheduler() (schedulerAfter time.Duration) {
	var (
		jobPlan  *common.JobSchedulerPlan
		now      time.Time
		nearTime *time.Time
	)
	// 如果任务表为空，随便睡眠多久
	if len(scheduler.jobPlanTable) == 0 {
		schedulerAfter = time.Second * 1
		return
	}
	now = time.Now()
	// 1 遍历所有任务
	for _, jobPlan = range scheduler.jobPlanTable {
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			jobPlan.NextTime = jobPlan.Expr.Next(now) // 更新下次执行时间
			// TODO: 尝试执行任务
			scheduler.TryStartJob(jobPlan)
		}
	}
	// 统计最近一个要过期的任务时间
	if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
		nearTime = &jobPlan.NextTime
	}
	// 下次调度时间
	schedulerAfter = (*nearTime).Sub(now)
	return
}

func (scheduler *Scheduler) schedulerLoop() {
	var (
		jobEvent       *common.JobEvent
		schedulerAfter  time.Duration
		schedulerTimer *time.Timer
		jobResult *common.JobExecuteResult
	)
	schedulerAfter = scheduler.TryScheduler()
	// 调度的延迟定时器
	schedulerTimer = time.NewTimer(schedulerAfter)
	for {
		select {
		case jobEvent = <-scheduler.jobEventChan: // 监听任务变化事件
			// 对任务做增删改查
			scheduler.handleJobEvent(jobEvent)
		case <-schedulerTimer.C:
		case jobResult = <- scheduler.jobResultChan:  // 监听任务执行结果
			scheduler.handleJobResult(jobResult)
		}
		schedulerAfter = scheduler.TryScheduler()

		// 重置调度间隔
		schedulerTimer.Reset(schedulerAfter)
	}
}

var (
	// GScheduler 单例对象
	GScheduler *Scheduler
)

// PushJobEvent 推送任务事件变化
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

// InitScheduler 初始化调度协程
func InitScheduler()(err error) {

	GScheduler = &Scheduler{
		jobEventChan: make(chan *common.JobEvent, 1000),
		jobPlanTable: make(map[string]*common.JobSchedulerPlan),
		jobExecutingTable:make(map[string]*common.JobExecuteStatus),
		jobResultChan: make(chan *common.JobExecuteResult, 1000),
	}
	go GScheduler.schedulerLoop()
	return
}

// 回传执行结果
func (scheduler *Scheduler) PushJobResult(jobResult *common.JobExecuteResult){
	scheduler.jobResultChan <- jobResult
}

// 处理执行结果
func (scheduler *Scheduler) handleJobResult(jobResult *common.JobExecuteResult){
	var (
		jobLog *common.JobLog
	)
	// 删除执行状态
	delete(scheduler.jobExecutingTable, jobResult.JOBExecuteStatus.JOB.Name)

	// 生成执行日志
	if jobResult.Err != common.ErrLockAlreadyRequired {
		jobLog = &common.JobLog{
			JobName:      jobResult.JOBExecuteStatus.JOB.Name,
			Command:      jobResult.JOBExecuteStatus.JOB.Command,
			Output:       string(jobResult.Output),
			PlanTime:     jobResult.JOBExecuteStatus.PlanTime.UnixNano()/1000/1000,
			ScheduleTime: jobResult.JOBExecuteStatus.RealTime.UnixNano()/1000/1000,
			StartTime:    jobResult.StartTime.UnixNano()/1000/1000,
			EndTime:      jobResult.EndTime.UnixNano()/1000/1000,
		}
		if jobResult.Err != nil{
			jobLog.Err = jobResult.Err.Error()
		}else {
			jobLog.Err = ""
		}
		GLogSink.Append(jobLog)
		fmt.Println("任务执行完成 ", jobResult.JOBExecuteStatus.JOB.Name, string(jobResult.Output), jobResult.Err)
	}

}