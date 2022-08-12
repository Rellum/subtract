package main

import (
	"context"
	"os"
	"os/signal"
	"subtract/cmd"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	cmd.Execute(ctx)
}
