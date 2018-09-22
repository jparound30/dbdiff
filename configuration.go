package dbdiff

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"sync"
)

type Configuration struct {
	Db Db `yaml:"db"`
}

type Db struct {
	DbType   string `yaml:"type"`
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Name     string `yaml:"name"`
	Schema   string `yaml:"schema"`
}

func GetConfiguration() *Configuration {
	onceYaml.Do(func() {
		initializeYaml()
	})
	return instanceYaml
}

var instanceYaml *Configuration
var onceYaml sync.Once

// yamlファイルから構造体を生成
func initializeYaml() {
	// TODO 読み込むファイル名を可変にするか？
	buf, err := ioutil.ReadFile("configuration.yaml")
	if err != nil {
		log.Fatalln(err)
	}
	instanceYaml = &Configuration{}
	err = yaml.Unmarshal(buf, instanceYaml)
	if err != nil {
		log.Fatalln(err)
	}
}
