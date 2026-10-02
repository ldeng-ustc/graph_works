#!/usr/bin/env python3
"""Build/run the bounded correctness probe while exclusively holding NVMe lock.

No upstream file/build is modified. Existing native libbach.so is linked and
its digest is recorded. Exit 75 means the lock is busy; no build/run occurred.
"""
import datetime
import fcntl
import hashlib
import json
import pathlib
import shlex
import subprocess
import sys
from zoneinfo import ZoneInfo


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main():
    private = pathlib.Path(__file__).resolve().parent
    related = private.parents[1]
    bach = related / "BACH"
    build = bach / "build"
    workspace = pathlib.Path("/home/denglong.1997/scatter_graph")
    with (workspace / "output/tmp/nvme-benchmark.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("NVMe lock busy; no build or test started", flush=True)
            return 75
        stamp = datetime.datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d-%H%M%S")
        output = workspace / "output/tmp" / (stamp + "-bach-mixed-native-probe")
        output.mkdir()
        print(output, flush=True)
        (output / "native_semantics_probe.cpp").write_bytes(
            (private / "native_semantics_probe.cpp").read_bytes())
        (output / "run_probe.py").write_bytes(pathlib.Path(__file__).read_bytes())
        binary = output / "bach_native_semantics_probe"
        flags = (build / "CMakeFiles/bach_oneway_basic_benchmarks.dir/flags.make").read_text()
        includes = next(line.split("=", 1)[1] for line in flags.splitlines()
                        if line.startswith("CXX_INCLUDES ="))
        defines = next(line.split("=", 1)[1] for line in flags.splitlines()
                       if line.startswith("CXX_DEFINES ="))
        link = shlex.split((build / "CMakeFiles/bach_oneway_basic_benchmarks.dir/link.txt").read_text())
        command = [link[0], "-O0", "-g", "-std=gnu++20", "-fopenmp"]
        command += shlex.split(defines) + shlex.split(includes)
        command += ["-I" + str(workspace / "include"),
                    "-I" + str(related / "oneway_basic_benchmarks")]
        for index, token in enumerate(link[1:], 1):
            if token.startswith("-O") or token == "-DNDEBUG":
                continue
            if token.endswith(".o"):
                command.append(str(private / "native_semantics_probe.cpp"))
            elif index > 0 and link[index - 1] == "-o":
                command.append(str(binary))
            elif not token.startswith("-") and not token.startswith("/") and "/" in token:
                command.append(str(build / token))
            elif token == "libbach.so":
                command.append(str(build / token))
            else:
                command.append(token)
        provenance = {
            "purpose": "correctness only; no performance timing claims",
            "native_library": str(build / "libbach.so"),
            "native_library_sha256": sha256(build / "libbach.so"),
            "source_sha256": sha256(private / "native_semantics_probe.cpp"),
            "shared_analytics_sha256": sha256(workspace / "include/sg/mixed_workload/analytics.h"),
            "native_sources": {str(path.relative_to(related)): sha256(path) for path in
                               [bach / "src/MemoryManager.cpp", bach / "src/Transaction.cpp",
                                bach / "src/SSTableParser.cpp", bach / "src/Version.cpp"]},
            "build_command": command,
            "expectations": [
                "same-memtable duplicate/delete should work",
                "cross-flush GetEdges may expose deleted/duplicate older versions",
                "native GetEdge should retain latest tombstone/value",
                "native DelVertex does not hide all adjacent edges or clear its delete time on PutVertex",
                "point-filter plus alive/birth should pass tiny E12 semantics, with expensive point scans",
            ],
            "build_timeout_s": 120,
            "run_timeout_s": 60,
        }
        (output / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
        with (output / "build.log").open("w") as log:
            build_result = subprocess.run(["timeout", "120", *command], stdout=log,
                                          stderr=subprocess.STDOUT)
        if build_result.returncode:
            print("build failed", build_result.returncode, output / "build.log", flush=True)
            return build_result.returncode
        with (output / "probe.log").open("w") as log:
            result = subprocess.run(["timeout", "60", str(binary), str(output / "runtime")],
                                    stdout=log, stderr=subprocess.STDOUT)
        records = []
        for line in (output / "probe.log").read_text().splitlines():
            if line.startswith("{"):
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        status = {
            "returncode": result.returncode,
            "returncode_meaning": "probe completion; native incompatibilities are recorded, not hidden",
            "binary_sha256": sha256(binary),
            "native_e12_compatibility_false": [row["check"] for row in records
                                               if row.get("e12_semantics") is False],
            "native_e12_compatibility_true": [row["check"] for row in records
                                              if row.get("e12_semantics") is True],
            "private_projection_pass": any(row.get("check") == "point_filtered_projection_all_operations"
                                            and row.get("pass") for row in records),
            "shared_algorithms_pass": any(row.get("check") == "shared_e12_algorithms"
                                           and row.get("pass") for row in records),
        }
        (output / "status.json").write_text(json.dumps(status, indent=2) + "\n")
        print("probe returncode", result.returncode, output / "probe.log", flush=True)
        return result.returncode


if __name__ == "__main__":
    sys.exit(main())
