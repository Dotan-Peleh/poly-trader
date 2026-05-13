#!/bin/bash
# Run poly-trader on the laptop. Polymarket geo-blocks GCP/AWS/Azure
# datacenter IPs for POST /order, so the bot has to run on a residential
# IP — i.e. your home network — to actually fire live trades.
#
# Usage:
#   ./run_local.sh paper          # paper mode (safe default)
#   ./run_local.sh live           # real money
#   ./run_local.sh live screen    # run inside `screen` so it survives terminal close
#
# Once running in screen: detach with Ctrl+a then d. Reattach with `screen -r poly`.
# Stop with: `screen -X -S poly quit` (or just close laptop).
set -euo pipefail

cd "$(dirname "$0")"

MODE="${1:-paper}"
USE_SCREEN="${2:-}"

if [[ "$MODE" != "paper" && "$MODE" != "live" ]]; then
  echo "usage: $0 [paper|live] [screen]" >&2
  exit 1
fi

# Prevent macOS App Nap from throttling background python: caffeinate keeps
# the laptop awake AND prevents the python process from being suspended.
# Combined with screen, the bot runs reliably even with the lid closed if
# you set "Prevent computer from sleeping" in Energy Saver. Otherwise the
# laptop sleeping = bot stops.
# config/settings.py only loads the 5 Polymarket secrets when
# settings.trading_mode == "live" — and that field reads from the
# TRADING_MODE env var, NOT from the --mode CLI flag. Without this
# export, `--mode live` would bail with "missing secrets" because the
# pydantic settings instance was already constructed in paper mode at
# module import. Match the env var to the CLI flag so they agree.
export TRADING_MODE="$MODE"

# Stream stdout+stderr to a rolling log file so we can inspect what the
# bot is doing without having to attach to screen. screen's hardcopy is
# flaky; a tee to disk just works.
LOGFILE="/tmp/poly_bot.log"
RUNNER=(/bin/bash -c "/usr/bin/caffeinate -is /usr/bin/env python3 main.py --mode \"$MODE\" --yes 2>&1 | tee \"$LOGFILE\"")

if [[ "$USE_SCREEN" == "screen" ]]; then
  # Kill any prior screen session so we don't double-run
  screen -X -S poly quit 2>/dev/null || true
  screen -dmS poly "${RUNNER[@]}"
  echo "Started in screen session 'poly'."
  echo "  Attach to view logs:   screen -r poly"
  echo "  Detach without stop:   Ctrl+a then d"
  echo "  Stop the bot:          screen -X -S poly quit"
else
  exec "${RUNNER[@]}"
fi
