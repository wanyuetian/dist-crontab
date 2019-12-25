package master

import (
	"context"
	"cron-master/common"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type LogMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var (
	GLogMgr *LogMgr
)

func InitLogMgr() (err error) {
	var (
		client    *mongo.Client
		cancelCtx context.Context
		//cancelFunc context.CancelFunc
	)
	if client, err = mongo.NewClient(options.Client().ApplyURI(GConfig.MongodbUri)); err != nil {
		return
	}
	cancelCtx, _ = context.WithTimeout(context.Background(), time.Duration(GConfig.MongodbConnectTimeout)*time.Millisecond)
	//defer cancelFunc()
	if err = client.Connect(cancelCtx); err != nil {
		return
	}
	GLogMgr = &LogMgr{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}
	return
}

func (logMgr *LogMgr) ListLog(name string, skip int64, limit int64) (logArr []*common.JobLog, err error) {
	var (
		filter  *common.JobLogFilter
		logSort *common.SortLogByStartTime
		cursor  *mongo.Cursor
		jobLog  *common.JobLog
	)
	logArr = make([]*common.JobLog, 0)
	// 过滤条件
	filter = &common.JobLogFilter{JobName: name}
	// 按照任务开始时间倒排
	logSort = &common.SortLogByStartTime{SortOrder: -1}

	if cursor, err = logMgr.logCollection.Find(context.TODO(), filter, &options.FindOptions{
		Skip:  &skip,
		Limit: &limit,
		Sort:  logSort,
	}); err != nil {
		return
	}
	defer cursor.Close(context.TODO())
	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}
		if err = cursor.Decode(jobLog); err != nil {
			continue
		}
		logArr = append(logArr, jobLog)
	}
	return
}
