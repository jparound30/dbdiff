package dbdiff

import (
	"reflect"
	"sync"
	"testing"
)

const TestConfigPrefix string = "./testdata/configuration/"

func Test_initializeYaml(t *testing.T) {
	type args struct {
		configFilePath string
	}
	tests := []struct {
		name string
		args args
	}{
		{"Normal", args{TestConfigPrefix + "test_config_normal.yaml"}},
		{"FileNotFound", args{TestConfigPrefix + "test_config_notfound.yaml"}},
		{"Invalid InputFile", args{TestConfigPrefix + "test_config_invalid.yaml"}},
		{"NotSpecified InputFile", args{""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initializeYaml(tt.args.configFilePath)
		})
	}
}

func TestGetConfiguration(t *testing.T) {
	var expectedConfig = Configuration{Db: Db{
		DbType:   "dbtype",
		Host:     "host",
		Port:     "123",
		User:     "user",
		Password: "pass",
		Name:     "dbname",
		Schema:   "schema",
	}}
	tests := []struct {
		name    string
		want    *Configuration
		wantErr bool
		setup   func()
	}{
		{
			name:    "Normal",
			want:    &expectedConfig,
			wantErr: false,
			setup: func() {
				instanceYaml = &expectedConfig
			},
		},
		{
			name:    "No loaded configuration",
			want:    nil,
			wantErr: true,
			setup: func() {
				instanceYaml = nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			got, err := GetConfiguration()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetConfiguration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetConfiguration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoadConfiguration(t *testing.T) {
	var expectedConfig = Configuration{Db: Db{
		DbType:   "postgresql",
		Host:     "localhost",
		Port:     "5432",
		User:     "user1",
		Password: "pswd2",
		Name:     "dbname",
		Schema:   "schema.",
	}}

	type args struct {
		configFilePath string
	}
	tests := []struct {
		name    string
		args    args
		want    *Configuration
		wantErr bool
		setup   func()
	}{
		{
			name:    "Normal",
			args:    args{configFilePath: TestConfigPrefix + "test_config_normal.yaml"},
			want:    &expectedConfig,
			wantErr: false,
			setup: func() {
				onceYaml = sync.Once{}
				instanceYaml = nil
			},
		},
		{
			name:    "TestConfig not found",
			args:    args{configFilePath: TestConfigPrefix + "test_config_notfound.yaml"},
			want:    nil,
			wantErr: true,
			setup: func() {
				onceYaml = sync.Once{}
				instanceYaml = nil
			},
		},
		{
			name:    "Already initialized",
			args:    args{configFilePath: TestConfigPrefix + "test_config_notfound.yaml"},
			want:    &Configuration{Db: Db{}},
			wantErr: false,
			setup: func() {
				onceYaml = sync.Once{}
				onceYaml.Do(func() {
					// nothing
				})
				instanceYaml = &Configuration{Db: Db{}}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			got, err := LoadConfiguration(tt.args.configFilePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadConfiguration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LoadConfiguration() = %v, want %v", got, tt.want)
			}
		})
	}
}
