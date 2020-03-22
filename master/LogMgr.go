package master

import (
	"context"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/clientopt"
	"github.com/mongodb/mongo-go-driver/mongo/findopt"
	"github.com/zhuhj083/crontab/common"
	"time"
)

// mongodb日志管理
type LogMgr struct {
	client        *mongo.Client
	logConnection *mongo.Collection
}

var (
	// 单例
	G_logMgr *LogMgr
)

func InitLogMgr() (err error) {
	var (
		client *mongo.Client
	)

	if client, err = mongo.Connect(
		context.TODO(),
		G_config.MongodbUri,
		clientopt.ConnectTimeout(time.Duration(G_config.MongodbConnectionTimeout)*time.Millisecond)); err != nil {
		return
	}

	G_logMgr = &LogMgr{
		client:        client,
		logConnection: client.Database("cron").Collection("log"),
	}

	return
}

func (logMgr *LogMgr) ListLog(name string, skip int, limit int) (logArr []*common.JobLog, err error) {
	var (
		filter  *common.JobLogFilter
		logSort *common.SortLogByStartTime
		cursor  mongo.Cursor
		jobLog  *common.JobLog
	)

	logArr = make([]*common.JobLog, 0)

	// 过滤条件
	filter = &common.JobLogFilter{
		JobName: name,
	}

	// 按照任务开始时间倒排
	logSort = &common.SortLogByStartTime{
		SortOrder: -1,
	}

	// 查询
	if cursor, err = logMgr.logConnection.Find(context.TODO(), filter, findopt.Sort(logSort), findopt.Skip(int64(skip)), findopt.Limit(int64(limit))); err != nil {
		return
	}

	// 延迟释放游标
	defer cursor.Close(context.TODO())

	// 遍历游标
	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}

		// 反序列化BSON
		if err = cursor.Decode(jobLog); err != nil {
			// 有日志不合法
			continue
		}

		logArr = append(logArr, jobLog)
	}

	return
}
