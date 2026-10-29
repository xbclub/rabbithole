// Command rhctl 是 rabbithole 管理 REST API 的命令行控制端。
// main 只做一件事:把 cli.Execute 的退出码透传给 os.Exit(任务书 §2.1)。
package main

import (
	"os"

	"github.com/xbclub/rabbithole/cli/rhctl/internal/cli"
)

func main() {
	os.Exit(cli.Execute())
}
