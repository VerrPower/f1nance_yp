#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  ./monitor_hadoop_job.sh [--input GLOB] [--output HDFS_DIR] [--jar PATH]
                          [--interval SEC] [--top N]

Defaults (project-specific):
  --input    /user/pogi/HD_INPUT_REPO/FINANCE_for_YP/*/*/snapshot.csv
  --output   /user/pogi/HD_OUTPUT_REPO/FINANCE_for_YP/run_monitor_<timestamp>
  --jar      POGI-ONE-RELEASE/target/POGI-ONE-RELEASE-0.1.0-SNAPSHOT.jar
  --interval 0.2
  --top      25

What it does:
  - Starts `hadoop jar ...` in background
  - Attempts to identify the actual JVM PID (sometimes the wrapper execs; sometimes it forks)
  - Prints CPU/cpuset/affinity info and periodically prints top CPU threads via `ps -T`

Example:
  ./monitor_hadoop_job.sh
USAGE
}

INPUT="/user/pogi/HD_INPUT_REPO/FINANCE_for_YP/*/*/snapshot.csv"
OUTPUT="/user/pogi/HD_OUTPUT_REPO/FINANCE_for_YP/run_monitor_$(date +%Y%m%d_%H%M%S)"
JAR="POGI-ONE-RELEASE/target/POGI-ONE-RELEASE-0.1.0-SNAPSHOT.jar"
MAIN="pogi_one.Driver"
INTERVAL="0.2"
TOP_N="25"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --input) INPUT="${2:?}"; shift 2 ;;
    --output) OUTPUT="${2:?}"; shift 2 ;;
    --jar) JAR="${2:?}"; shift 2 ;;
    --interval) INTERVAL="${2:?}"; shift 2 ;;
    --top) TOP_N="${2:?}"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ ! -f "$JAR" ]]; then
  echo "Jar not found: $JAR" >&2
  echo "Build first: python lifecycle/build.py" >&2
  exit 1
fi

echo "cpuset.effective=$(cat /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null || echo '?')"
echo "nproc=$(nproc 2>/dev/null || echo '?')"
echo "INPUT=$INPUT"
echo "OUTPUT=$OUTPUT"
echo "JAR=$JAR"
echo

echo "Starting: hadoop jar \"$JAR\" \"$MAIN\" \"$INPUT\" \"$OUTPUT\""
hadoop jar "$JAR" "$MAIN" "$INPUT" "$OUTPUT" &
launcher_pid=$!
echo "launcher_pid=$launcher_pid"

# Give the wrapper/JVM a moment to exec/fork.
sleep 0.5

target_pid="$launcher_pid"
comm="$(ps -p "$launcher_pid" -o comm= 2>/dev/null | awk '{print $1}' || true)"
if [[ "$comm" != "java" ]]; then
  child_java_pid="$(pgrep -P "$launcher_pid" -n java 2>/dev/null || true)"
  if [[ -n "${child_java_pid:-}" ]]; then
    target_pid="$child_java_pid"
  fi
fi

echo "target_pid=$target_pid"
echo

echo "Affinity:"
taskset -pc "$target_pid" 2>/dev/null || echo "(taskset unavailable or not permitted)"
echo

echo "Threads/CPU snapshot (updates every ${INTERVAL}s, showing top ${TOP_N}):"
echo

while kill -0 "$target_pid" 2>/dev/null; do
  date
  ps -p "$target_pid" -o pid,ppid,psr,pcpu,pmem,etime,comm 2>/dev/null || true
  ps -T -p "$target_pid" -o pid,spid,psr,pcpu,comm --sort=-pcpu 2>/dev/null | head -n "$TOP_N" || true
  echo "----"
  sleep "$INTERVAL"
done

wait "$launcher_pid"
rc=$?
echo "hadoop jar exit_code=$rc"
exit "$rc"
