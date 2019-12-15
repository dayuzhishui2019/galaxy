package context

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"sunset/data-hub/util"
)

func init() {
	configPath := util.GetAppPath() + "config.yml"
	log.Info("configPath:", configPath)
	viper.SetConfigFile(configPath)
	viper.SetConfigType("yaml")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal("Fail to read config file :", err)
	}
}


//配置
func Set(key string, value interface{}) {
	viper.Set(key, value)
}

func IsExsit(key string) bool {
	return viper.IsSet(key)
}

func GetString(key string) string {
	return viper.GetString(key)
}
func GetInt(key string) int {
	return viper.GetInt(key)
}
func GetInt32(key string) int32 {
	return viper.GetInt32(key)
}
func GetInt64(key string) int64 {
	return viper.GetInt64(key)
}
func GetBool(key string) bool {
	return viper.GetBool(key)
}
