package util

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
	"dyzs/galaxy/logger"
)

func ExecCmd(cmdStr string) (string, error) {
	var out bytes.Buffer
	var stderr bytes.Buffer

	logger.LOG_INFO("### CMD ###:", cmdStr)
	cmd := exec.Command("bash", "-c", cmdStr)
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		logger.LOG_ERROR("### CMD FAILED ###:", fmt.Sprint(err)+": "+stderr.String(), err)
		return "", err
	}
	return strings.Trim(out.String(), "\n"), nil
}
