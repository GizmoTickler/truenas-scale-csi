package driver

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/truenas-scale-csi/pkg/truenas"
)

// DriverConfig holds the driver initialization configuration.
type DriverConfig struct {
	Name          string
	Version       string
	NodeID        string
	Endpoint      string
	RunController bool
	RunNode       bool
	Config        *Config
}

// Driver is the TrueNAS Scale CSI driver.
type Driver struct {
	// Embed unimplemented servers for forward compatibility with CSI spec
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer

	name          string
	version       string
	nodeID        string
	endpoint      string
	runController bool
	runNode       bool
	config        *Config

	// TrueNAS API client
	truenasClient *truenas.Client

	// gRPC server
	server *grpc.Server

	// Operation lock to prevent concurrent operations on same volume
	operationLock sync.Map

	// Ready flag
	ready bool
}

// NewDriver creates a new TrueNAS CSI driver instance.
func NewDriver(cfg *DriverConfig) (*Driver, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("driver name is required")
	}
	if cfg.Version == "" {
		cfg.Version = "unknown"
	}
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required")
	}
	if cfg.Config == nil {
		return nil, fmt.Errorf("config is required")
	}

	// Create TrueNAS API client
	truenasClient, err := truenas.NewClient(&truenas.ClientConfig{
		Host:          cfg.Config.TrueNAS.Host,
		Port:          cfg.Config.TrueNAS.Port,
		Protocol:      cfg.Config.TrueNAS.Protocol,
		APIKey:        cfg.Config.TrueNAS.APIKey,
		AllowInsecure: cfg.Config.TrueNAS.AllowInsecure,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create TrueNAS client: %w", err)
	}

	return &Driver{
		name:          cfg.Name,
		version:       cfg.Version,
		nodeID:        cfg.NodeID,
		endpoint:      cfg.Endpoint,
		runController: cfg.RunController,
		runNode:       cfg.RunNode,
		config:        cfg.Config,
		truenasClient: truenasClient,
	}, nil
}

// Run starts the CSI driver.
func (d *Driver) Run() error {
	// Parse endpoint
	u, err := url.Parse(d.endpoint)
	if err != nil {
		return fmt.Errorf("failed to parse endpoint: %w", err)
	}

	var addr string
	if u.Scheme == "unix" {
		addr = u.Path
		// Remove existing socket file
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove socket file: %w", err)
		}
	} else {
		addr = u.Host
	}

	// Create listener
	listener, err := net.Listen(u.Scheme, addr)
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}

	// Set socket permissions for unix sockets
	if u.Scheme == "unix" {
		if err := os.Chmod(addr, 0660); err != nil {
			return fmt.Errorf("failed to set socket permissions: %w", err)
		}
	}

	// Create gRPC server with interceptor for logging
	d.server = grpc.NewServer(
		grpc.UnaryInterceptor(d.logInterceptor),
	)

	// Register CSI services
	csi.RegisterIdentityServer(d.server, d)

	if d.runController {
		csi.RegisterControllerServer(d.server, d)
		klog.Info("Controller service registered")
	}

	if d.runNode {
		csi.RegisterNodeServer(d.server, d)
		klog.Info("Node service registered")
	}

	d.ready = true
	klog.Infof("CSI driver listening on %s", d.endpoint)

	return d.server.Serve(listener)
}

// Stop gracefully stops the driver.
func (d *Driver) Stop() {
	klog.Info("Stopping CSI driver")
	d.ready = false
	if d.server != nil {
		d.server.GracefulStop()
	}
	if d.truenasClient != nil {
		if err := d.truenasClient.Close(); err != nil {
			klog.Warningf("Failed to close TrueNAS client: %v", err)
		}
	}
}

// logInterceptor is a gRPC interceptor for logging requests.
func (d *Driver) logInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	klog.V(4).Infof("gRPC call: %s", info.FullMethod)
	klog.V(5).Infof("gRPC request: %+v", req)

	resp, err := handler(ctx, req)

	if err != nil {
		klog.Errorf("gRPC error: %s: %v", info.FullMethod, err)
	} else {
		klog.V(5).Infof("gRPC response: %+v", resp)
	}

	return resp, err
}

// acquireOperationLock acquires a lock for the given operation key.
// Returns false if the lock is already held.
func (d *Driver) acquireOperationLock(key string) bool {
	_, loaded := d.operationLock.LoadOrStore(key, struct{}{})
	return !loaded
}

// releaseOperationLock releases the lock for the given operation key.
func (d *Driver) releaseOperationLock(key string) {
	d.operationLock.Delete(key)
}

// GetTrueNASClient returns the TrueNAS API client.
func (d *Driver) GetTrueNASClient() *truenas.Client {
	return d.truenasClient
}

// GetConfig returns the driver configuration.
func (d *Driver) GetConfig() *Config {
	return d.config
}
