#!/usr/bin/env zsh
# ─────────────────────────────────────────────────────────────────────────────
# run.sh – One-command startup for the Data Reconciliation Engine
#
# Usage:
#   ./run.sh          → run via fat JAR (production mode, rebuilds if needed)
#   ./run.sh dev      → run via Spring Boot Maven plugin (hot-reload friendly)
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# ── 1. Java 21 ────────────────────────────────────────────────────────────────
JAVA21="/opt/homebrew/Cellar/openjdk@21/21.0.10/libexec/openjdk.jdk/Contents/Home"
if [[ -d "$JAVA21" ]]; then
  export JAVA_HOME="$JAVA21"
  export PATH="$JAVA_HOME/bin:$PATH"
fi

JAVA_VER=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d. -f1)
if [[ "$JAVA_VER" -lt 21 ]]; then
  echo "ERROR: Java 21+ required (found Java $JAVA_VER)"
  exit 1
fi

# ── 2. Load credentials & config from .env ───────────────────────────────────
if [[ -f ".env" ]]; then
  echo "Loading config from .env"
  set -a; source .env; set +a
else
  echo "WARNING: .env file not found. Expecting env vars or application.yml"
fi

# ── 3. Kill any process still listening on 7070 ───────────────────────────────
if lsof -tiTCP:7070 -sTCP:LISTEN &>/dev/null; then
  echo "Stopping existing process on port 7070..."
  kill "$(lsof -tiTCP:7070 -sTCP:LISTEN)" 2>/dev/null || true
  sleep 1
fi

# ── 4. Start ──────────────────────────────────────────────────────────────────
MODE="${1:-jar}"

if [[ "$MODE" == "dev" ]]; then
  echo "Starting in DEV mode (Spring Boot Maven plugin)..."
  mvn spring-boot:run
else
  echo "Building fat JAR..."
  mvn -q clean package -DskipTests

  JAR="target/compare-engine-1.0.0.jar"
  echo "Starting fat JAR: $JAR"
  echo "→ Open http://localhost:7070"
  exec java -jar "$JAR"
fi

