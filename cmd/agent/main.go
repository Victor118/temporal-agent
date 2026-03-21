package main

import (
	"log"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

func newUUID() string {
	return uuid.New().String()
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "agent",
		Short: "Temporal Agent — AI assistant orchestrated by Temporal",
	}

	rootCmd.AddCommand(serverCmd, workerCmd, devCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
