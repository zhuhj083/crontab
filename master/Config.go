package master

import (
	"encoding/json"
	"io/ioutil"
)

// 程序配置
type Config struct {
	ApiPort                  int      `json:"apiPort"`
	ApiReadTimeOut           int      `json:apiReadTimeout`
	ApiWriteTimeOut          int      `json:apiWriteTimeout`
	EtcdEndPoints            []string `json:"etcdEndpoints"`
	EtcdDailTimeout          int      `json:"etcdDailTimeout"`
	WebRoot                  string   `json:"webroot"`
	MongodbUri               string   `json:"mongodbUri`
	MongodbConnectionTimeout int      `json:"mongodbConnectionTimeout"`
}

// 单例
var (
	G_config *Config
)

// 加载配置
func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf    Config
	)

	// 1、把配置文件读进来
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}

	// 2、做json反序列化
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}

	// 3、赋值单例
	G_config = &conf

	return
}
