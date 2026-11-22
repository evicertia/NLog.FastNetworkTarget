#!/usr/bin/env bash
# Build each target revision against the same frameworks and run today's harness.
set -eu -o pipefail
root=$(git rev-parse --show-toplevel)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
bench=NLog.FastNetworkTarget.Benchmarks
lib=NLog.FastNetworkTarget
for rev in "$@"; do
  sha=$(git rev-parse --short --verify "$rev^{commit}")
  dir="$tmp/$sha"
  mkdir -p "$dir/$bench"
  git archive "$sha" "$lib" | tar -x -C "$dir"
  cp "$root/$lib/$lib.csproj" "$dir/$lib/"
  cp "$root/$bench/"*.cs "$root/$bench/$bench.csproj" "$dir/$bench/"
  dotnet build "$dir/$bench/$bench.csproj" -c Release -p:NuGetAudit=false
  for tfm in net8.0 net462; do
    exe="$dir/$bench/bin/Release/$tfm/$bench"
    if [[ $tfm == net462 ]]; then cmd=(mono "$exe.exe"); else cmd=(dotnet "$exe.dll"); fi
    "${cmd[@]}" --iterations 3 --markdown "$root/benchmarks/results/$sha-$tfm.md"
  done
done
