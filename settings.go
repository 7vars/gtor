package gtor

import (
	"time"

	"github.com/spf13/viper"
)

type Config interface {
	Get(string) interface{}
	GetBool(string) bool
	GetFloat64(string) float64
	GetInt(string) int
	GetIntSlice(string) []int
	GetString(string) string
	GetStringMap(string) map[string]interface{}
	GetStringMapString(string) map[string]string
	GetStringSlice(string) []string
	GetTime(string) time.Time
	GetDuration(string) time.Duration

	IsSet(string) bool

	GetDefault(string, interface{}) interface{}
	GetBoolDefault(string, bool) bool
	GetFloat64Default(string, float64) float64
	GetIntDefault(string, int) int
	GetIntSliceDefault(string, []int) []int
	GetStringDefault(string, string) string
	GetStringMapDefault(string, map[string]interface{}) map[string]interface{}
	GetStringMapStringDefault(string, map[string]string) map[string]string
	GetStringSliceDefault(string, []string) []string
	GetTimeDefault(string, time.Time) time.Time
	GetDurationDefault(string, time.Duration) time.Duration

	GetConfig(string) (Config, bool)
}

func config() Config {
	return &viperWrapper{
		viper.GetViper(),
	}
}

type viperWrapper struct {
	*viper.Viper
}

func (w *viperWrapper) GetDefault(key string, v interface{}) interface{} {
	if w.IsSet(key) {
		return w.Get(key)
	}
	return v
}

func (w *viperWrapper) GetBoolDefault(key string, v bool) bool {
	if w.IsSet(key) {
		return w.GetBool(key)
	}
	return v
}

func (w *viperWrapper) GetFloat64Default(key string, v float64) float64 {
	if w.IsSet(key) {
		return w.GetFloat64(key)
	}
	return v
}

func (w *viperWrapper) GetIntDefault(key string, v int) int {
	if w.IsSet(key) {
		return w.GetInt(key)
	}
	return v
}

func (w *viperWrapper) GetIntSliceDefault(key string, v []int) []int {
	if w.IsSet(key) {
		return w.GetIntSlice(key)
	}
	return v
}

func (w *viperWrapper) GetStringDefault(key string, v string) string {
	if w.IsSet(key) {
		return w.GetString(key)
	}
	return v
}

func (w *viperWrapper) GetStringMapDefault(key string, v map[string]interface{}) map[string]interface{} {
	if w.IsSet(key) {
		return w.GetStringMap(key)
	}
	return v
}

func (w *viperWrapper) GetStringMapStringDefault(key string, v map[string]string) map[string]string {
	if w.IsSet(key) {
		return w.GetStringMapString(key)
	}
	return v
}

func (w *viperWrapper) GetStringSliceDefault(key string, v []string) []string {
	if w.IsSet(key) {
		return w.GetStringSlice(key)
	}
	return v
}

func (w *viperWrapper) GetTimeDefault(key string, v time.Time) time.Time {
	if w.IsSet(key) {
		return w.GetTime(key)
	}
	return v
}

func (w *viperWrapper) GetDurationDefault(key string, v time.Duration) time.Duration {
	if w.IsSet(key) {
		return w.GetDuration(key)
	}
	return v
}

func (w *viperWrapper) GetConfig(key string) (Config, bool) {
	if w.IsSet(key) {
		return &viperWrapper{
			w.Sub(key),
		}, true
	}
	return nil, false
}

type Option struct {
	Name  string
	Value interface{}
}
