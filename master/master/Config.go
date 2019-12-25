package master

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// Config 程序配置
type Config struct {
	APIPort         int      `json:"apiPort"`
	APIReadTimeout  int      `json:"apiReadTimeout"`
	APIWriteTimeout int      `json:"apiWriteTimeout"`
	EtcdEndpoints   []string `json:"etcdEndpoints"`
	EtcdDialTimeout int      `json:"etcdDialTimeout"`
	WebRoot http.Dir	`json:"webRoot"`
	MongodbUri string `json:"mongodbUri"`
	MongodbConnectTimeout int `json:"mongodbConnectTimeout"`
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
