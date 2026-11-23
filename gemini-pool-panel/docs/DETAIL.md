# Gemini pool panel

## What it shows

- Configured accounts and their availability.
- Currently active calls, including the execution step.
- Recent calls with duration, latency, and completion status.

## Data source

The panel reads from the `agy-pool` MCP server. When started with:

```
AGY_POOL_DASHBOARD_PORT=8791
```

the server exposes a local JSON endpoint at:

```
http://127.0.0.1:8791/activity
```

## Offline behaviour

If the MCP server is not running or the port is unreachable, the panel displays an "offline" status indicator.

## Limitations

- Read-only: does not trigger, cancel, or reorder calls.
- Polling interval is fixed at 2 seconds.
- Only monitors processes launched with the `AGY_POOL_DASHBOARD_PORT` environment variable set.

No data leaves the local machine.
