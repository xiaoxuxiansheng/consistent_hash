package os

import (
	"fmt"
	"os"
	"runtime"
	"strings"
)

// 获取由进程id+协程id组成的二位标识字符串
func GetCurrentProcessAndGogroutineIDStr() string {
	pid := GetCurrentProcessID()
	goroutineID := GetCurrentGoroutineID()
	return fmt.Sprintf("%d_%s", pid, goroutineID)
}

// 获取当前的协程id
func GetCurrentGoroutineID() string {
	buf := make([]byte, 128)
	buf = buf[:runtime.Stack(buf, false)]
	stackInfo := string(buf)
	return strings.TrimSpace(strings.Split(strings.Split(stackInfo, "[running]")[0], "goroutine")[1])
}

// 获取当前的进程id
func GetCurrentProcessID() int {
	return os.Getpid()
}
