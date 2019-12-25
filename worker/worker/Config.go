package worker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// Config 程序配置
type Config struct {
	EtcdEndpoints   []string `json:"etcdEndpoints"`
	EtcdDialTimeout int      `json:"etcdDialTimeout"`
	MongodbUri string `json:"mongodbUri"`
	MongodbConnectTimeout int `json:"mongodbConnectTimeout"`
	JobLogBatchSize int `json:"jobLogBatchSize"`
	JobLogCommitTimeout int `json:"jobLogCommitTimeout"`
}

var (
	// GConfig 单例对象
	GConfig *Config
)

// InitConfig 初始化配置
func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf    Config
	)
	// 1 把配置文件读进来
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}
	// 2 做json反序列化
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}
	GConfig = &conf
	fmt.Println(GConfig)
	return
}
