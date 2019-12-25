package worker

import (
	"context"
	"crontab/worker/common"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type LogSink struct {
	client *mongo.Client
	logCollection *mongo.Collection
	logChan chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	GLogSink *LogSink
)

func InitLogSink()(err error){
	var (
		client *mongo.Client
		cancelCtx context.Context
		//cancelFunc context.CancelFunc
	)
	if client, err = mongo.NewClient(options.Client().ApplyURI(GConfig.MongodbUri)); err != nil {
		fmt.Println(err)
		return
	}
	cancelCtx, _ = context.WithTimeout(context.Background(), time.Duration(GConfig.MongodbConnectTimeout) * time.Millisecond)
	//defer cancelFunc()
	if err = client.Connect(cancelCtx); err != nil {
		return 
	}
	GLogSink =  &LogSink{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
		logChan:       make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}
	go GLogSink.writeLoop()
	return
}

func (logSink *LogSink) writeLoop(){
	var (
		jobLog *common.JobLog
		logBatch *common.LogBatch
		commitTimer *time.Timer
		timeoutBatch *common.LogBatch
	)
	for {
		select {
		case jobLog = <- logSink.logChan:
			// log写到MongoDB
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				commitTimer = time.AfterFunc(time.Duration(GConfig.JobLogCommitTimeout) * time.Millisecond, func(logBatch *common.LogBatch) func(){
					return func() {
						// 发出超时通知 不直接提交logBatch
						logSink.autoCommitChan <- logBatch
					}
				}(logBatch))

				commitTimer = time.NewTimer(time.Duration(GConfig.JobLogCommitTimeout) * time.Millisecond)
			}
			// 把日志追加到LogBatch中
			logBatch.Logs = append(logBatch.Logs, jobLog)
			// 如果批次满了 就立即发送
			if len(logBatch.Logs) >= GConfig.JobLogBatchSize{
				logSink.saveLogs(logBatch)
				// 清空logBatch
				logBatch = nil
				// 取消定时器
				commitTimer.Stop()
			}
		case timeoutBatch = <- logSink.autoCommitChan:  // 过期的批次
			if timeoutBatch != logBatch{  // 跳过已提交的批次
				continue
			}

			logSink.saveLogs(timeoutBatch)
			// 清空logBatch
			logBatch = nil

		}
	}
}

func (logSink *LogSink) saveLogs(logBatch *common.LogBatch){
	logSink.logCollection.InsertMany(context.TODO(),logBatch.Logs)
}

// 发送日志
func (logSink *LogSink) Append(jobLog *common.JobLog){
	select {
	case logSink.logChan <- jobLog:
	default:
		// 队列满了就丢弃
	}

}
