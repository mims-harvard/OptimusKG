import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def unify_benchmark_files_command(benchmarks_dir: Path):
    """Unify all benchmark JSON files in the edges directory into a single file."""

    # Initialize the unified data structure
    unified_data = {
        "benchmarks": [],
        "metadata": {"total_files": 0, "source_directory": str(benchmarks_dir)},
    }

    # Get all benchmark JSON files and sort them numerically
    benchmark_files = sorted(
        [f for f in benchmarks_dir.glob("benchmark_*.json")],
        key=lambda x: int(x.stem.split("_")[1]),
    )

    logger.info(f"Found {len(benchmark_files)} benchmark files")

    # Process each benchmark file
    for file_path in benchmark_files:
        try:
            with open(file_path) as f:
                data = json.load(f)

            # Add the benchmark number to each result
            benchmark_num = int(file_path.stem.split("_")[1])

            for result in data.get("results", []):
                benchmark_entry = {
                    "benchmark_id": benchmark_num,
                    "source_file": file_path.name,
                    **result,
                }
                unified_data["benchmarks"].append(benchmark_entry)

            logger.info(f"Processed {file_path.name}")

        except Exception as e:
            logger.error(f"Error processing {file_path.name}: {e}")

    # Update metadata
    unified_data["metadata"]["total_files"] = len(benchmark_files)
    unified_data["metadata"]["total_benchmarks"] = len(unified_data["benchmarks"])

    # Write the unified file
    output_path = benchmarks_dir / "unified_benchmarks.json"
    with open(output_path, "w") as f:
        json.dump(unified_data, f, indent=2)

    logger.info(f"\nUnified data written to: {output_path}")
    logger.info(f"Total benchmarks: {len(unified_data['benchmarks'])}")
    logger.info(f"Total files processed: {len(benchmark_files)}")
