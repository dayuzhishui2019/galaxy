package util

import "time"

/**
重试
*/
func Retry(f func() error, times int, space time.Duration) error {
	if times == 0 || times == 1 {
		return f()
	}
	var err error
	var i = 0
	for {
		err = f()
		if err == nil {
			return nil
		}
		i++
		if times > 1 && i >= times {
			break
		}
		time.Sleep(space)
	}
	return err
}
