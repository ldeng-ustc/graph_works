#!/usr/bin/env python3
"""Locked, bounded E12 LiveGraph correctness checks; never runs a real dataset."""
import argparse
import datetime as dt
import fcntl
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from zoneinfo import ZoneInfo


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rows(path):
    return [json.loads(line.removeprefix("E12_JSON "))
            for line in path.read_text().splitlines() if line.startswith("E12_JSON ")]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scatter-root", type=Path,
                        default=Path("/home/denglong.1997/scatter_graph"))
    parser.add_argument("--cpu-reference", type=Path, help="Existing shared offline-reference binary")
    parser.add_argument("--wait-lock", action="store_true", help="Queue once after a coordinated preceding window")
    args = parser.parse_args()
    scatter = args.scatter_root.resolve()
    source = Path(__file__).resolve().parent
    baseline = source.parent.parent
    cpu_reference = (args.cpu_reference or scatter / "build/mixed-workloads/app/sg/mixed_workload/sg_mixed_workload_reference").resolve()
    validator_path = scatter / "script/sg/E12-mixed_workloads/validate.py"
    if not cpu_reference.is_file() or not validator_path.is_file():
        raise RuntimeError("Shared CPU reference and validator must be built/provided before cross validation")
    temp = scatter / "output/tmp"
    temp.mkdir(parents=True, exist_ok=True)
    with (temp / "nvme-benchmark.lock").open("a") as lock:
        try:
            if args.wait_lock:
                print("Queued once for the coordinated NVMe lock handoff.", flush=True)
            fcntl.flock(lock, fcntl.LOCK_EX | (0 if args.wait_lock else fcntl.LOCK_NB))
        except BlockingIOError:
            print("Device lock is busy; no build or test started.")
            return 75
        stamp = dt.datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d-%H%M%S")
        root = temp / f"{stamp}-livegraph-mixed-smoke"
        root.mkdir()
        print(root, flush=True)
        started = time.monotonic()
        environment = os.environ.copy()
        environment["OMP_PROC_BIND"] = "FALSE"
        environment.pop("OMP_PLACES", None)
        environment.pop("GOMP_CPU_AFFINITY", None)
        commands = []
        provenance = {"purpose": "correctness_only_no_performance_conclusion", "files": {}}
        paths = sorted(source.glob("*.cpp")) + sorted(source.glob("*.hpp"))
        paths += [source / "CMakeLists.txt", Path(__file__).resolve()]
        paths += sorted((baseline / "LiveGraph/core").glob("*.hpp"))
        paths += sorted((baseline / "LiveGraph/src").glob("*.cpp"))
        paths += [scatter / f"include/{name}" for name in
                  ["datatype.h", "sg/mixed_workload/workload.h", "sg/mixed_workload/analytics.h",
                   "sg/oneway_batched_benchmarks.hpp"]]
        paths += [baseline / "oneway_basic_benchmarks/oneway_graph_benchmarks.hpp"]
        paths += [validator_path, cpu_reference]
        for path in paths:
            key = str(path)
            provenance["files"][key] = sha256(path)
            relative = path.relative_to(scatter) if path.is_relative_to(scatter) else path.relative_to(baseline)
            family = "scatter" if path.is_relative_to(scatter) else "baselines"
            target = root / "source" / family / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
        (root / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")

        def run(name, command, budget=120, expected=0):
            full = ["timeout", "--kill-after=5s", f"{budget}s", *map(str, command)]
            commands.append({"name": name, "command": full, "expected": expected})
            (root / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
            with (root / f"{name}.log").open("x") as output:
                result = subprocess.run(full, stdout=output, stderr=subprocess.STDOUT,
                                        cwd=baseline, env=environment, timeout=budget + 15)
            commands[-1]["returncode"] = result.returncode
            if result.returncode != expected:
                raise RuntimeError(f"{name} exited {result.returncode}, expected {expected}; see {name}.log")
            return root / f"{name}.log"

        try:
            build = root / "build"
            run("configure", ["cmake", "-S", source, "-B", build,
                              "-DCMAKE_BUILD_TYPE=Release", f"-DSCATTER_GRAPH_ROOT={scatter}"], 60)
            run("build", ["cmake", "--build", build, "-j", "4"], 180)
            test_log = run("native-semantics", [build / "livegraph_mixed_test", root / "suite"])
            expected = next(row for row in rows(test_log) if row["kind"] == "fixture_expected")
            cpu_log = run("cpu-reference", [cpu_reference, "--mode", "reference", "--base", root / "suite/base.bin",
                          "--trace", root / "suite/updates.bin", "--bfs-source", "6", "--pr-iterations", "10"])
            spec = importlib.util.spec_from_file_location("e12_shared_validator", validator_path)
            validator = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(validator)
            validation = []
            binary = build / "livegraph_mixed_bench"
            common = [binary, "--input", root / "suite/base.bin", "--trace", root / "suite/updates.bin",
                      "--mapping-bytes", str(1 << 32), "--pr-iterations", "10", "--bfs-source", "6"]
            for workers, chunk in [(1, 3), (4, 12)]:
                log = run(f"cli-{workers}", [*common, "--threads", workers, "--chunk-edges", chunk,
                          "--transaction-records", "5", "--data", root / f"cli-{workers}"])
                output = rows(log)
                actual = next(row for row in output if row["kind"] == "mixed_summary")
                assert any(row["kind"] == "mixed_process_end" for row in output)
                for key, value in expected.items():
                    if key in {"kind", "schema", "system"}:
                        continue
                    if isinstance(value, float):
                        assert math.isclose(actual[key], value, rel_tol=2e-6, abs_tol=2e-6), key
                    else:
                        assert actual[key] == value, key
                assert math.isclose(actual["load_s"], actual["update_apply_s"] + actual["final_drain_s"],
                                    rel_tol=1e-12)
                assert actual["load_s"] <= actual["updates_wall_s"]
                assert actual["update_records"] == 22
                assert [actual[f"{name}_attempts"] for name in
                        ["edge_insert", "edge_erase", "vertex_add", "vertex_erase"]] == [9, 6, 4, 3]
                failures = validator.validate_mixed(validator.read_records(log), validator.read_records(cpu_log),
                                                    require_direct=False)
                validation.append({"raw": str(log), "cpu": str(cpu_log), "errors": failures})
                (root / "shared-validation.json").write_text(json.dumps(validation, indent=2) + "\n")
                if failures:
                    raise RuntimeError("shared validator rejected output: " + "; ".join(failures))

            protected = root / "existing-runtime"
            protected.mkdir()
            marker = protected / "keep.txt"
            marker.write_text("existing data must remain intact\n")
            run("reject-existing-runtime", [*common, "--data", protected], 30, 1)
            assert marker.read_text() == "existing data must remain intact\n"
            assert sorted(p.name for p in protected.iterdir()) == ["keep.txt"]

            broken = bytearray((root / "suite/updates.bin").read_bytes())
            broken[88] ^= 1  # checksum field; records and operation counts remain well formed
            bad_trace = root / "bad-checksum.bin"
            bad_trace.write_bytes(broken)
            bad = run("reject-bad-trace", [binary, "--input", root / "suite/base.bin", "--trace", bad_trace,
                      "--data", root / "invalid-trace", "--threads", "4", "--mapping-bytes", str(1 << 32)], 30, 1)
            errors = rows(bad)
            assert any(row["kind"] == "error" and "checksum" in row["message"] for row in errors)
            assert all(row["kind"] != "mixed_summary" for row in errors)
            zero_scan = run("reject-zero-scan", [binary, "--input", root / "suite/base.bin",
                            "--trace", root / "suite/updates.bin", "--data", root / "zero-scan",
                            "--threads", "4", "--mapping-bytes", str(1 << 32), "--bfs-source", "0"], 30, 1)
            assert any(row["kind"] == "error" and "zero edges" in row["message"] for row in rows(zero_scan))
            for path, digest in provenance["files"].items():
                if sha256(Path(path)) != digest:
                    raise RuntimeError(f"Source changed during validation: {path}")
            status = {"status": "PASS", "checks": ["native_semantics_and_random_reference",
                       "cli_1_thread", "cli_4_threads", "refuse_existing_runtime", "reject_trace_checksum",
                       "reject_explicit_zero_scan_bfs", "shared_offline_reference_and_validator"],
                      "binary_sha256": sha256(binary), "wall_s": time.monotonic() - started}
        except Exception as error:
            status = {"status": "FAIL", "error": str(error), "wall_s": time.monotonic() - started}
        (root / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
        (root / "status.json").write_text(json.dumps(status, indent=2) + "\n")
        print(json.dumps(status), flush=True)
        return 0 if status["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
