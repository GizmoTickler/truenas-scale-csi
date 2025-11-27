package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/GizmoTickler/truenas-scale-csi/pkg/truenas"
)

func main() {
	apiKey := os.Getenv("TRUENAS_API_KEY")
	if apiKey == "" {
		fmt.Println("TRUENAS_API_KEY not set")
		os.Exit(1)
	}

	client, err := truenas.NewClient(&truenas.ClientConfig{
		Host:              "nas01.achva.casa",
		Port:              443,
		Protocol:          "https",
		APIKey:            apiKey,
		AllowInsecure:     true,
		Timeout:           60 * time.Second,
		MaxConcurrentReqs: 1,
		MaxConnections:    1,
	})
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	ctx := context.Background()

	// Check for snapshots with clones
	fmt.Println("=== Checking for snapshots with orphaned clones ===")
	snapshots, err := client.SnapshotListAll(ctx, "flashstor/k8s-csi", 100, 0)
	if err != nil {
		fmt.Printf("Error listing snapshots: %v\n", err)
		return
	}

	fmt.Printf("Found %d snapshots\n", len(snapshots))
	orphanedCount := 0
	for _, snap := range snapshots {
		clones := snap.GetClones()
		if len(clones) > 0 {
			orphanedCount++
			fmt.Printf("\nSnapshot: %s\n", snap.ID)
			fmt.Printf("  Clones: %v\n", clones)
		}
	}

	if orphanedCount == 0 {
		fmt.Println("\nNo snapshots with orphaned clones found!")
	} else {
		fmt.Printf("\nFound %d snapshots with clones\n", orphanedCount)
	}
}
