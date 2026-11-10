package main

//go:generate go run github.com/vektra/mockery/v3

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/elliot-gustafsson/kbootcd/internal/command"
	"github.com/elliot-gustafsson/kbootcd/internal/controller"
	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/clock"
)

func setupDefaultLogger(level, format string) {
	l := slog.LevelInfo

	if level != "" {
		err := l.UnmarshalText([]byte(level))
		if err != nil {
			slog.Error("failed to parse log level, defaulting to INFO",
				"error", err,
				"provided_level", level,
			)
			l = slog.LevelInfo
		}
	}
	opts := &slog.HandlerOptions{
		Level: l,
	}

	var handler slog.Handler
	switch strings.ToLower(format) {
	case "json":
		handler = slog.NewJSONHandler(os.Stdout, opts)
	case "text", "":
		handler = slog.NewTextHandler(os.Stdout, opts)
	default:
		slog.Error("unknown log format option, defaulting to text",
			"provided_format", format,
		)
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	slog.SetDefault(slog.New(handler))
}

var (
	logLevel       string
	logFormat      string
	configPath     string
	leaseName      string
	leaseNamespace string
	leaseDuration  time.Duration
	tickerInterval time.Duration
	rebootDelay    time.Duration
	cooldownDelay  time.Duration
	rebootDays     []string
	rebootStart    string
	rebootEnd      string
	timezone       string
)

var logLevels = []string{
	slog.LevelDebug.String(),
	slog.LevelInfo.String(),
	slog.LevelWarn.String(),
	slog.LevelError.String(),
}

func main() {

	var rootCmd = &cobra.Command{
		Use:   "kbootcd",
		Short: "Kubernetes Bootc Daemon",
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd.Context())
		},
	}

	rootCmd.Flags().StringVar(&logLevel, "log-level", slog.LevelInfo.String(), fmt.Sprintf("Log level. Valid values: %s", strings.Join(logLevels, ", ")))
	rootCmd.Flags().StringVar(&logFormat, "log-format", "text", "Log format. Valid values: text, json")

	rootCmd.Flags().StringVar(&configPath, "config-path", "/etc/bootc-config/target_image", "Path to the ConfigMap mount containing the desired digest")
	rootCmd.Flags().StringVar(&leaseName, "lease-name", "bootc-upgrade-lock", "Name of the cluster-wide coordination lease")
	rootCmd.Flags().StringVar(&leaseNamespace, "lease-namespace", "kube-system", "Namespace of the coordination lease")
	rootCmd.Flags().DurationVar(&leaseDuration, "lease-duration", 60*time.Minute, "Lease duration")
	rootCmd.Flags().DurationVar(&tickerInterval, "interval", 5*time.Minute, "How often to run the reconciliation loop")
	rootCmd.Flags().DurationVar(&rebootDelay, "reboot-delay", 0, "Delay reboot for this duration")
	rootCmd.Flags().DurationVar(&cooldownDelay, "cooldown-delay", 0, "Delay the release of the lease lock for this duration after a successful update")

	rootCmd.PersistentFlags().StringSliceVar(&rebootDays, "reboot-days", controller.EveryDay, "schedule reboot on these days")
	rootCmd.PersistentFlags().StringVar(&rebootStart, "start-time", "0:00", "schedule reboot only after this time of day")
	rootCmd.PersistentFlags().StringVar(&rebootEnd, "end-time", "23:59:59", "schedule reboot only before this time of day")
	rootCmd.PersistentFlags().StringVar(&timezone, "time-zone", "UTC", "use this timezone for schedule inputs")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)
	defer stop()

	err := rootCmd.ExecuteContext(ctx)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	setupDefaultLogger(logLevel, logFormat)

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		return fmt.Errorf("error NODE_NAME environment variable must be set")
	}

	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to get in-cluster kubernetes config, err: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes clientset, err: %w", err)
	}

	config := controller.Config{
		Clock:                 clock.RealClock{},
		NodeName:              nodeName,
		TargetImageConfigPath: configPath,
		LeaseName:             leaseName,
		LeaseNamespace:        leaseNamespace,
		LeaseDuration:         leaseDuration,
		RebootDelay:           rebootDelay,
		CooldownDelay:         cooldownDelay,
		SlackToken:            os.Getenv("SLACK_TOKEN"),
		SlackChannel:          os.Getenv("SLACK_CHANNEL"),
	}

	config.Window, err = controller.BuildTimeWindow(rebootStart, rebootEnd, rebootDays, timezone)
	if err != nil {
		return fmt.Errorf("error building time window, err: %w", err)
	}

	slog.Info("Starting kbootcd", "node", nodeName, "interval", tickerInterval.String())
	if config.SlackToken != "" && config.SlackChannel == "" {
		return fmt.Errorf("SLACK_CHANNEL must be set when SLACK_TOKEN is set")
	}
	if config.SlackToken != "" {
		slog.Info("slack notifications enabled", "channel", config.SlackChannel)
	}

	cmder := command.NewCommander()
	// Run immediate baseline check on startup
	err = controller.Reconcile(ctx, cmder, clientset, config)
	if err != nil {
		slog.Error("error during initial reconciliation", "error", err.Error())
	}

	ticker := time.NewTicker(tickerInterval)
	for {

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := controller.Reconcile(ctx, cmder, clientset, config)
			if err != nil {
				if errors.Is(err, controller.ErrRebootInitiated) {
					slog.Info("reboot issued successfully, waiting for node shutdown...", "node", nodeName)
					return nil
				}
				slog.Error("error during reconciliation", "error", err.Error())
			}
		}
	}
}
