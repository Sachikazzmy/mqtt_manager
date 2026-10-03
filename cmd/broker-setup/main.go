package main

import (
	"context"
	"fmt"
	"os"

	"Project/internal/receive"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Broker 初始化失败:", err)
		os.Exit(1)
	}
}

func run() error {
	config, err := receive.LoadConfigFromEnv()
	if err != nil {
		return err
	}
	manager := receive.NewManager(config)
	if err := manager.BootstrapReceiver(context.Background()); err != nil {
		return err
	}
	fmt.Println("Broker 已就绪；业务账户独立接收遥测/命令结果并发布设备命令，管理账户不发送业务命令，默认 ACL 为拒绝")
	return nil
}
