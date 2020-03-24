package master

import (
	"context"
	"github.com/zhuhj083/crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
		ctx    context.Context
	)

	ctx, _ = context.WithTimeout(context.Background(), time.Duration(G_config.MongodbConnectionTimeout)*time.Second)
	client, err = mongo.Connect(ctx, options.Client().ApplyURI(G_config.MongodbUri))

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
		cursor  *mongo.Cursor
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

	opts := new(options.FindOptions)

	// 查询
	if cursor, err = logMgr.logConnection.Find(context.TODO(), filter, opts.SetSort(logSort), opts.SetSkip(int64(skip)), opts.SetLimit(int64(limit))); err != nil {
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
