/*
 * @Author: hswsp hswsp@mail.ustc.edu.cn
 * @Date: 2022-08-02 22:05:56
 * @LastEditors: hswsp hswsp@mail.ustc.edu.cn
 * @LastEditTime: 2022-08-03 16:01:05
 * @FilePath: /src/raft/logger.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
package raft

import (
	"fmt"

	"github.com/hashicorp/go-hclog"
)

// Logger is abstract type for debug log messages
type Logger interface {
	Log(v ...interface{})
	Logf(s string, v ...interface{})
}

// LoggerAdapter allows a log.Logger to be used with the local Logger interface
type LoggerAdapter struct {
	log hclog.Logger
}

// Log a message to the contained debug log
func (a *LoggerAdapter) Log(v ...interface{}) {
	a.log.Info(fmt.Sprint(v...))
}

// Logf will record a formatted message to the contained debug log
func (a *LoggerAdapter) Logf(s string, v ...interface{}) {
	a.log.Info(fmt.Sprintf(s, v...))
}
