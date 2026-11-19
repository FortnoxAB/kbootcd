# kbootcd

The Kubernetes Bootc Daemon is a node-level agent that automates OS image updates for Kubernetes nodes using `bootc`.

## Operation

The daemon runs a reconciliation loop that continuously aligns the node's OS image with a desired target by performing the following steps:

1. **Read Desired State:** It reads the target OS image digest or reference from a local file, which is typically mounted via a Kubernetes ConfigMap.
2. **Evaluate Current State:** It queries the host using `bootc status` to determine the currently booted and staged image layers.
3. **Stage Update:** If the desired image differs from the booted image and is not yet staged, the agent executes `bootc switch` to stage the new OS image layer on the host.
4. **Maintenance Window Validation:** If an update is staged and requires a reboot, the daemon waits for the host time to enter the configured maintenance window.
5. **Cluster Coordination:** To prevent simultaneous node restarts, it attempts to acquire a distributed lock via a Kubernetes `Lease` object in the cluster.
6. **Health Check:** It verifies that all other nodes in the cluster are in a `Ready` state before proceeding with node disruption.
7. **Drain and Reboot:** After acquiring the lock and confirming cluster health, it safely cordons the node, evicts running workloads, and issues a host reboot.
8. **Restore:** After the node reboots with the updated OS image, it automatically uncordons the node to resume scheduling and releases the distributed lock.

## Configuration

```text
Kubernetes Bootc Daemon

Usage:
  kbootcd [flags]

Flags:
      --config-path string       Path to the ConfigMap mount containing the desired digest (default "/etc/bootc-config/target_image")
      --end-time string          schedule reboot only before this time of day (default "23:59:59")
  -h, --help                     help for kbootcd
      --interval duration        How often to run the reconciliation loop (default 5m0s)
      --lease-name string        Name of the cluster-wide coordination lease (default "bootc-upgrade-lock")
      --lease-namespace string   Namespace of the coordination lease (default "kube-system")
      --log-format string        Log format. Valid values: text, json (default "text")
      --log-level string         Log level. Valid values: DEBUG, INFO, WARN, ERROR (default "INFO")
      --reboot-days strings      schedule reboot on these days (default [Sunday,Monday,Tuesday,Wednesday,Thursday,Friday,Saturday])
      --start-time string        schedule reboot only after this time of day (default "0:00")
      --time-zone string         use this timezone for schedule inputs (default "UTC")
```

## Environment Variables

- `NODE_NAME`: The name of the Kubernetes node the daemon is running on. Must be set to identify the node for API operations and the node drain process.
- `SLACK_TOKEN`: Optional Slack bot token with the `chat:write` and `chat:write.customize` scopes (the latter lets messages post as `kbootcd`). When set, a message is posted before draining, before rebooting and after uncordoning a node.
- `SLACK_CHANNEL`: Channel ID or name to post to. Required when `SLACK_TOKEN` is set.
