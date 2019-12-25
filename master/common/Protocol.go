package common

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/gorhill/cronexpr"
)

// Job 定时任务
type Job struct {
	Name     string `json:"name"`
	Command  string `json:"command"`
	CronExpr string `json:"cronExpr"`
}

// Response HTTP接口应答
type Response struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

// JobSchedulerPlan 任务调度计划
type JobSchedulerPlan struct {
	JOB      *Job
	Expr     *cronexpr.Expression
	NextTime time.Time // 下次调度时间
}

// JobExecuteStatus 任务执行状态
type JobExecuteStatus struct {
	JOB *Job
	PlanTime time.Time  // 理论调度时间
	RealTime time.Time  // 实际调度时间
	CancelCtx context.Context
	CancelFunc  context.CancelFunc
}

// JobEvent job事件
type JobEvent struct {
	 EventType int
	JOB       *Job
}

// JobExecuteResult 任务执行结果
type JobExecuteResult struct {
	JOBExecuteStatus *JobExecuteStatus
	Output []byte
	Err error
	StartTime time.Time
	EndTime time.Time
}

// BuildResponse 应答方法
func BuildResponse(code int, msg string, data interface{}) (resp []byte, err error) {
	var (
		response Response
	)
	response.Code = code
	response.Msg = msg
	response.Data = data
	resp, err = json.Marshal(response)
	return
}

// 反序列化job
func UnpackJob(value []byte) (job *Job, err error) {
	job = &Job{}
	if err = json.Unmarshal(value, &job); err != nil {
		return
	}
	return
}

// ExtractJobName 从etcd的key中提取任务名
func ExtractJobName(jobKey string) string {
	return strings.TrimPrefix(jobKey, JobSaveDIR)
}

// ExtractKillerName 从etcd的key中提取任务名
func ExtractKillerName(killerKey string) string {
	return strings.TrimPrefix(killerKey, JobKillerDIR)
}

//
func BuildJobEvent(eventType int, job *Job) (jobEvent *JobEvent) {
	return &JobEvent{
		EventType: eventType,
		JOB:       job,
	}
}

func BuildJobSchedulerPlan(job *Job) (jobSchedulerPlan *JobSchedulerPlan, err error) {
	var (
		expr *cronexpr.Expression
	)

	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		return
	}
	jobSchedulerPlan = &JobSchedulerPlan{
		JOB:      job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}


func BuildJobExecuteStatus(jobSchedulerPlan *JobSchedulerPlan)(jobExecuteStatus * JobExecuteStatus){
	jobExecuteStatus = &JobExecuteStatus{
		JOB:      jobSchedulerPlan.JOB,
		PlanTime: jobSchedulerPlan.NextTime,
		RealTime: time.Now(),

	}
	jobExecuteStatus.CancelCtx, jobExecuteStatus.CancelFunc = context.WithCancel(context.TODO())
	return
}

type JobLog struct {
	JobName string `json:"jobName" bson:"jobName"` // 任务名字
	Command string `json:"command" bson:"command"` // 脚本命令
	Err string `json:"err" bson:"err"` // 错误原因
	Output string `json:"output" bson:"output"`	// 脚本输出
	PlanTime int64 `json:"planTime" bson:"planTime"` // 计划开始时间
	ScheduleTime int64 `json:"scheduleTime" bson:"scheduleTime"` // 实际调度时间
	StartTime int64 `json:"startTime" bson:"startTime"` // 任务执行开始时间
	EndTime int64 `json:"endTime" bson:"endTime"` // 任务执行结束时间
}

type LogBatch struct {
	Logs []interface{}
}

// 任务过滤条件
type JobLogFilter struct {
	JobName string `bson:"jobName"`
}
// 任务排序规则
type SortLogByStartTime struct {
	SortOrder int `bson:"startTime"`
}

// ExtractWorkerIP 提取worker的IP
func ExtractWorkerIP(workerKey string) string {
	return strings.TrimPrefix(workerKey, JobWorkerDir)
}