package dbdiff

import (
	"errors"
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

func LoadConfiguration(configFilePath string) (*Configuration, error) {
	var err error
	onceYaml.Do(func() {
		if instanceYaml, err = initializeYaml(configFilePath); err != nil {
			log.Printf("Can not load configuration %+v\n", err)
		}
	})
	return instanceYaml, err
}

func GetConfiguration() (*Configuration, error) {
	var err error
	if instanceYaml == nil {
		log.Printf("Need to be initialize with LoadConfiguration()\n")
		err = errors.New("need to be initialize with LoadConfiguration()")
	}
	return instanceYaml, err
}

var instanceYaml *Configuration
var onceYaml sync.Once

const DefaultConfigFilePath = "configuration.yaml"

// yamlファイルから構造体を生成
func initializeYaml(configFilePath string) (*Configuration, error) {
	var buf []byte
	var err error
	if len(configFilePath) == 0 {
		buf, err = ioutil.ReadFile(DefaultConfigFilePath)
	} else {
		buf, err = ioutil.ReadFile(configFilePath)
	}
	if err != nil {
		log.Println(err)
		return nil, err
	}
	var instance = &Configuration{}
	err = yaml.Unmarshal(buf, instance)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return instance, err
}
