// Package buildinfo 暴露构建期注入的版本信息。
//
// 通过 -ldflags 注入:
//
//	go build -ldflags "-X 'github.com/jrmarcco/synp/internal/pkg/buildinfo.Version=v1.0.0' ..."
package buildinfo

// Version / Commit / Date 由构建流程注入, 未注入时为开发默认值。
var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)
