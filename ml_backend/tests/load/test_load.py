"""
Load Testing Framework
======================

Benchmarks the pipeline at different scales to measure actual performance.
Creates real benchmark data to replace theoretical estimates.
"""

import asyncio
import time
import psutil
import json
from typing import Dict, Any, List
from dataclasses import dataclass, asdict
from datetime import datetime
import tempfile
import os
import pandas as pd
import numpy as np

from src.orchestrator import SynthosOrchestrator


@dataclass
class BenchmarkResult:
    """Results from a single benchmark run"""
    dataset_size: int
    dataset_format: str
    total_time_seconds: float
    rows_per_second: float
    peak_memory_mb: float
    avg_cpu_percent: float
    avg_gpu_utilization: float
    stages: Dict[str, float]
    success: bool
    error_message: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


class LoadTester:
    """
    Performs load testing at various scales.
    """

    def __init__(self):
        self.results: List[BenchmarkResult] = []

    def generate_test_dataset(self, num_rows: int, num_features: int = 10) -> pd.DataFrame:
        """
        Generate synthetic dataset for testing.

        Args:
            num_rows: Number of rows
            num_features: Number of features

        Returns:
            Generated DataFrame
        """
        np.random.seed(42)
        data = {}

        for i in range(num_features):
            if i % 3 == 0:
                # Continuous numerical
                data[f'feature_{i}'] = np.random.randn(num_rows)
            elif i % 3 == 1:
                # Categorical
                data[f'feature_{i}'] = np.random.choice(['A', 'B', 'C', 'D'], num_rows)
            else:
                # Integer
                data[f'feature_{i}'] = np.random.randint(0, 1000, num_rows)

        return pd.DataFrame(data)

    async def benchmark_size(
        self,
        num_rows: int,
        dataset_format: str = 'parquet',
        num_features: int = 10
    ) -> BenchmarkResult:
        """
        Benchmark a specific dataset size.

        Args:
            num_rows: Number of rows to test
            dataset_format: Format to use (csv, parquet)
            num_features: Number of features

        Returns:
            BenchmarkResult with timing and resource usage
        """
        print(f"\n{'='*60}")
        print(f"BENCHMARKING: {num_rows:,} rows ({dataset_format})")
        print(f"{'='*60}")

        # Generate test data
        print(f"Generating test dataset...")
        dataset = self.generate_test_dataset(num_rows, num_features)

        # Save to temporary file
        with tempfile.NamedTemporaryFile(
            suffix=f'.{dataset_format}',
            delete=False
        ) as f:
            temp_file = f.name
            if dataset_format == 'csv':
                dataset.to_csv(temp_file, index=False)
            elif dataset_format == 'parquet':
                dataset.to_parquet(temp_file, index=False)
            else:
                raise ValueError(f"Unsupported format: {dataset_format}")

        try:
            # Create orchestrator
            orchestrator = SynthosOrchestrator(
                gpu_memory_fraction=0.9,
                enable_mixed_precision=False,
                collapse_threshold=65.0,
                diversity_threshold=50.0,
                use_cache=False,
                skip_cascade_training=True  # Skip for benchmarking
            )

            # Track resource usage
            process = psutil.Process()
            start_memory = process.memory_info().rss / (1024 * 1024)  # MB
            peak_memory = start_memory

            # Run validation
            start_time = time.time()

            try:
                result = await orchestrator.validate(
                    dataset_path=temp_file,
                    dataset_format=dataset_format,
                    stream_progress=False
                )

                end_time = time.time()
                total_time = end_time - start_time

                # Get final memory
                end_memory = process.memory_info().rss / (1024 * 1024)
                peak_memory = max(peak_memory, end_memory)

                # Extract stage timings
                stages = {
                    'data_loading': result.load_time_seconds,
                    'diversity_analysis': result.diversity_time_seconds,
                    'cascade_training': result.cascade_time_seconds,
                    'collapse_detection': result.collapse_time_seconds,
                    'localization': result.localization_time_seconds,
                    'recommendations': result.recommendation_time_seconds,
                }

                # Calculate throughput
                rows_per_second = num_rows / total_time if total_time > 0 else 0

                # Print summary
                print(f"\n✅ COMPLETED")
                print(f"Total Time: {total_time:.2f}s")
                print(f"Throughput: {rows_per_second:,.0f} rows/second")
                print(f"Memory: {peak_memory:.0f} MB")
                print(f"\nStage Breakdown:")
                for stage, duration in stages.items():
                    print(f"  {stage:20s}: {duration:6.2f}s ({duration/total_time*100:5.1f}%)")

                return BenchmarkResult(
                    dataset_size=num_rows,
                    dataset_format=dataset_format,
                    total_time_seconds=total_time,
                    rows_per_second=rows_per_second,
                    peak_memory_mb=peak_memory,
                    avg_cpu_percent=psutil.cpu_percent(),
                    avg_gpu_utilization=result.gpu_utilization_avg,
                    stages=stages,
                    success=True
                )

            except Exception as e:
                end_time = time.time()
                total_time = end_time - start_time

                print(f"\n❌ FAILED: {e}")

                return BenchmarkResult(
                    dataset_size=num_rows,
                    dataset_format=dataset_format,
                    total_time_seconds=total_time,
                    rows_per_second=0,
                    peak_memory_mb=peak_memory,
                    avg_cpu_percent=psutil.cpu_percent(),
                    avg_gpu_utilization=0,
                    stages={},
                    success=False,
                    error_message=str(e)
                )

        finally:
            # Cleanup
            if os.path.exists(temp_file):
                os.unlink(temp_file)

    async def run_load_tests(
        self,
        sizes: List[int] = None,
        formats: List[str] = None
    ) -> List[BenchmarkResult]:
        """
        Run comprehensive load tests.

        Args:
            sizes: List of dataset sizes to test
            formats: List of formats to test

        Returns:
            List of benchmark results
        """
        if sizes is None:
            sizes = [1_000, 10_000, 100_000]  # Start conservative

        if formats is None:
            formats = ['csv', 'parquet']

        print("\n" + "="*60)
        print("SYNTHOS ML BACKEND - LOAD TESTING")
        print("="*60)
        print(f"Testing {len(sizes)} sizes × {len(formats)} formats = {len(sizes) * len(formats)} benchmarks")
        print(f"Sizes: {', '.join(f'{s:,}' for s in sizes)}")
        print(f"Formats: {', '.join(formats)}")

        results = []

        for size in sizes:
            for fmt in formats:
                result = await self.benchmark_size(size, fmt)
                results.append(result)
                self.results.append(result)

                # Add delay between tests
                await asyncio.sleep(2)

        return results

    def save_results(self, output_path: str = "benchmark_results.json"):
        """Save benchmark results to JSON file"""
        with open(output_path, 'w') as f:
            json.dump(
                {
                    'timestamp': datetime.now().isoformat(),
                    'results': [r.to_dict() for r in self.results]
                },
                f,
                indent=2
            )
        print(f"\n📊 Results saved to: {output_path}")

    def print_summary(self):
        """Print summary of all benchmarks"""
        print("\n" + "="*60)
        print("BENCHMARK SUMMARY")
        print("="*60)

        successful = [r for r in self.results if r.success]
        failed = [r for r in self.results if not r.success]

        print(f"\nTotal Benchmarks: {len(self.results)}")
        print(f"Successful: {len(successful)}")
        print(f"Failed: {len(failed)}")

        if successful:
            print(f"\n{'Size':>12} | {'Format':>8} | {'Time':>10} | {'Throughput':>15} | {'Memory':>10}")
            print("-" * 75)
            for result in successful:
                print(
                    f"{result.dataset_size:>12,} | "
                    f"{result.dataset_format:>8} | "
                    f"{result.total_time_seconds:>9.2f}s | "
                    f"{result.rows_per_second:>13,.0f} r/s | "
                    f"{result.peak_memory_mb:>9.0f} MB"
                )

        if failed:
            print(f"\n❌ FAILED BENCHMARKS:")
            for result in failed:
                print(f"  - {result.dataset_size:,} rows ({result.dataset_format}): {result.error_message}")


async def main():
    """Run load tests"""
    tester = LoadTester()

    # Define test sizes (start small, scale up)
    # Conservative sizes for initial testing
    test_sizes = [
        1_000,      # 1K rows
        10_000,     # 10K rows
        100_000,    # 100K rows
        # 1_000_000,  # 1M rows (uncomment when ready)
        # 10_000_000, # 10M rows (uncomment when ready)
    ]

    # Run tests
    await tester.run_load_tests(
        sizes=test_sizes,
        formats=['csv', 'parquet']
    )

    # Print summary
    tester.print_summary()

    # Save results
    tester.save_results('benchmark_results.json')

    print("\n✅ Load testing complete!")


if __name__ == "__main__":
    asyncio.run(main())
