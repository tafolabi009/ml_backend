#!/usr/bin/env python3
"""
SynthOS Large-Scale Synthetic Dataset Generator
Generates realistic ML training data with mixed schemas, noise, and edge cases.
"""

import os
import sys
import json
import hashlib
import random
import string
import struct
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple
import csv
import io

# Configuration
CONFIG = {
    "target_size_mb": int(os.getenv("DATASET_SIZE_MB", "100")),  # Default 100MB for testing
    "output_dir": os.getenv("OUTPUT_DIR", "./synthetic_data"),
    "seed": int(os.getenv("RANDOM_SEED", "42")),
    "include_corrupted": True,
    "include_adversarial": True,
}

random.seed(CONFIG["seed"])


class DatasetGenerator:
    """Generates various types of synthetic ML datasets."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.stats = {
            "total_rows": 0,
            "total_bytes": 0,
            "schemas_generated": [],
            "corruption_rate": 0.0,
            "null_rate": 0.0,
        }
        
    def generate_tabular_data(self, num_rows: int, num_cols: int) -> Tuple[str, Dict]:
        """Generate tabular CSV data with realistic distributions."""
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Generate column names and types
        col_types = []
        headers = []
        for i in range(num_cols):
            col_type = random.choice(["numeric", "categorical", "text", "datetime", "embedding"])
            col_types.append(col_type)
            headers.append(f"col_{col_type}_{i}")
        
        writer.writerow(headers)
        
        null_count = 0
        corrupt_count = 0
        
        for row_idx in range(num_rows):
            row = []
            for col_idx, col_type in enumerate(col_types):
                # Inject nulls (5% rate)
                if random.random() < 0.05:
                    row.append("")
                    null_count += 1
                    continue
                    
                # Inject corrupted data (1% rate)
                if self.config["include_corrupted"] and random.random() < 0.01:
                    row.append(self._generate_corrupted_value(col_type))
                    corrupt_count += 1
                    continue
                
                # Generate valid data
                if col_type == "numeric":
                    # Mix of distributions
                    dist = random.choice(["normal", "uniform", "exponential", "bimodal"])
                    if dist == "normal":
                        row.append(round(random.gauss(100, 25), 4))
                    elif dist == "uniform":
                        row.append(round(random.uniform(0, 1000), 4))
                    elif dist == "exponential":
                        row.append(round(random.expovariate(0.1), 4))
                    else:  # bimodal
                        if random.random() < 0.5:
                            row.append(round(random.gauss(30, 5), 4))
                        else:
                            row.append(round(random.gauss(80, 5), 4))
                            
                elif col_type == "categorical":
                    categories = ["A", "B", "C", "D", "E", "rare_F", "rare_G"]
                    weights = [0.3, 0.25, 0.2, 0.15, 0.08, 0.015, 0.005]
                    row.append(random.choices(categories, weights=weights)[0])
                    
                elif col_type == "text":
                    word_count = random.randint(1, 20)
                    words = [''.join(random.choices(string.ascii_lowercase, k=random.randint(3, 10))) 
                            for _ in range(word_count)]
                    row.append(' '.join(words))
                    
                elif col_type == "datetime":
                    base_date = datetime(2020, 1, 1)
                    offset = timedelta(days=random.randint(0, 1500), 
                                      hours=random.randint(0, 23),
                                      minutes=random.randint(0, 59))
                    row.append((base_date + offset).isoformat())
                    
                elif col_type == "embedding":
                    # 32-dim embedding vector
                    embedding = [round(random.gauss(0, 1), 6) for _ in range(32)]
                    row.append(json.dumps(embedding))
            
            writer.writerow(row)
        
        content = output.getvalue()
        
        metadata = {
            "type": "tabular",
            "rows": num_rows,
            "columns": num_cols,
            "column_types": dict(zip(headers, col_types)),
            "null_rate": null_count / (num_rows * num_cols),
            "corruption_rate": corrupt_count / (num_rows * num_cols),
            "size_bytes": len(content.encode('utf-8')),
        }
        
        self.stats["total_rows"] += num_rows
        self.stats["total_bytes"] += metadata["size_bytes"]
        
        return content, metadata
    
    def generate_timeseries_data(self, num_series: int, points_per_series: int) -> Tuple[str, Dict]:
        """Generate time-series data with trends, seasonality, and anomalies."""
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["series_id", "timestamp", "value", "quality_flag", "metadata"])
        
        anomaly_count = 0
        
        for series_id in range(num_series):
            # Series parameters
            base_value = random.uniform(10, 1000)
            trend = random.uniform(-0.1, 0.1)
            seasonality_period = random.choice([24, 168, 720])  # hourly, weekly, monthly
            seasonality_amplitude = random.uniform(0.1, 0.3) * base_value
            noise_level = random.uniform(0.01, 0.1) * base_value
            
            base_time = datetime(2024, 1, 1)
            
            for point_idx in range(points_per_series):
                timestamp = base_time + timedelta(hours=point_idx)
                
                # Calculate value with trend + seasonality + noise
                trend_component = trend * point_idx
                seasonal_component = seasonality_amplitude * \
                    (0.5 + 0.5 * ((point_idx % seasonality_period) / seasonality_period))
                noise_component = random.gauss(0, noise_level)
                
                value = base_value + trend_component + seasonal_component + noise_component
                
                # Inject anomalies (2% rate)
                quality_flag = "normal"
                if random.random() < 0.02:
                    anomaly_type = random.choice(["spike", "drop", "shift", "missing"])
                    if anomaly_type == "spike":
                        value *= random.uniform(2, 5)
                        quality_flag = "anomaly_spike"
                    elif anomaly_type == "drop":
                        value *= random.uniform(0.1, 0.5)
                        quality_flag = "anomaly_drop"
                    elif anomaly_type == "shift":
                        value += base_value * random.choice([-1, 1]) * random.uniform(0.5, 1.5)
                        quality_flag = "anomaly_shift"
                    else:
                        value = ""
                        quality_flag = "missing"
                    anomaly_count += 1
                
                meta = json.dumps({
                    "sensor_type": random.choice(["temperature", "pressure", "flow", "voltage"]),
                    "unit": random.choice(["celsius", "psi", "m3/s", "V"]),
                })
                
                writer.writerow([f"series_{series_id}", timestamp.isoformat(), 
                               round(value, 4) if value else "", quality_flag, meta])
        
        content = output.getvalue()
        
        metadata = {
            "type": "timeseries",
            "num_series": num_series,
            "points_per_series": points_per_series,
            "total_points": num_series * points_per_series,
            "anomaly_rate": anomaly_count / (num_series * points_per_series),
            "size_bytes": len(content.encode('utf-8')),
        }
        
        self.stats["total_rows"] += num_series * points_per_series
        self.stats["total_bytes"] += metadata["size_bytes"]
        
        return content, metadata
    
    def generate_embedding_data(self, num_samples: int, embedding_dim: int) -> Tuple[bytes, Dict]:
        """Generate binary embedding vectors (simulating neural network outputs)."""
        embeddings = []
        labels = []
        
        # Create cluster centers for realistic distribution
        num_clusters = random.randint(5, 15)
        cluster_centers = [[random.gauss(0, 3) for _ in range(embedding_dim)] 
                          for _ in range(num_clusters)]
        
        for _ in range(num_samples):
            # Pick a cluster
            cluster_idx = random.randint(0, num_clusters - 1)
            center = cluster_centers[cluster_idx]
            
            # Generate embedding near cluster center
            embedding = [center[d] + random.gauss(0, 0.5) for d in range(embedding_dim)]
            
            # Normalize
            norm = sum(x**2 for x in embedding) ** 0.5
            embedding = [x / norm for x in embedding]
            
            embeddings.append(embedding)
            labels.append(cluster_idx)
        
        # Pack as binary (float32)
        binary_data = b''
        for emb in embeddings:
            binary_data += struct.pack(f'{embedding_dim}f', *emb)
        
        # Add labels
        binary_data += struct.pack(f'{num_samples}i', *labels)
        
        metadata = {
            "type": "embeddings",
            "num_samples": num_samples,
            "embedding_dim": embedding_dim,
            "num_clusters": num_clusters,
            "format": "float32_binary",
            "size_bytes": len(binary_data),
        }
        
        self.stats["total_rows"] += num_samples
        self.stats["total_bytes"] += metadata["size_bytes"]
        
        return binary_data, metadata
    
    def generate_adversarial_samples(self, num_samples: int) -> Tuple[str, Dict]:
        """Generate adversarial/edge-case samples to stress-test validation."""
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["sample_id", "adversarial_type", "data", "expected_behavior"])
        
        adversarial_types = [
            ("unicode_injection", lambda: ''.join(chr(random.randint(0x4e00, 0x9fff)) for _ in range(50))),
            ("sql_injection", lambda: "'; DROP TABLE users; --"),
            ("script_injection", lambda: "<script>alert('xss')</script>"),
            ("extremely_long", lambda: 'x' * 100000),
            ("special_chars", lambda: ''.join(random.choices('!@#$%^&*()[]{}|;:,.<>?`~', k=100))),
            ("null_bytes", lambda: '\x00\x00\x00data\x00with\x00nulls\x00'),
            ("negative_numbers", lambda: str(random.uniform(-1e308, -1e-308))),
            ("infinity", lambda: "Infinity"),
            ("nan", lambda: "NaN"),
            ("empty", lambda: ""),
            ("whitespace_only", lambda: "   \t\n\r   "),
            ("mixed_encoding", lambda: "UTF8: café, Latin1: caf\xe9"),
            ("deeply_nested_json", lambda: json.dumps(self._create_nested_dict(20))),
            ("huge_number", lambda: str(10 ** 1000)),
            ("date_edge", lambda: "9999-12-31T23:59:59.999999Z"),
        ]
        
        for i in range(num_samples):
            adv_type, generator = random.choice(adversarial_types)
            try:
                data = generator()
            except:
                data = "GENERATION_ERROR"
            
            expected = random.choice(["reject", "sanitize", "accept_with_warning", "truncate"])
            writer.writerow([f"adv_{i}", adv_type, data[:1000], expected])  # Truncate for CSV safety
        
        content = output.getvalue()
        
        metadata = {
            "type": "adversarial",
            "num_samples": num_samples,
            "adversarial_types": list(set(t[0] for t in adversarial_types)),
            "size_bytes": len(content.encode('utf-8', errors='replace')),
        }
        
        self.stats["total_rows"] += num_samples
        self.stats["total_bytes"] += metadata["size_bytes"]
        
        return content, metadata
    
    def _generate_corrupted_value(self, col_type: str) -> str:
        """Generate intentionally corrupted data for a given column type."""
        corruptions = {
            "numeric": lambda: random.choice(["NaN", "inf", "-inf", "1e999", "0x1F", "twelve"]),
            "categorical": lambda: random.choice(["UNKNOWN", "???", "N/A", "null", ""]),
            "text": lambda: random.choice(["\x00\x00", "\\u0000", chr(0xFFFD) * 10]),
            "datetime": lambda: random.choice(["not-a-date", "2024-13-45", "0000-00-00", "9999-99-99"]),
            "embedding": lambda: random.choice(["[]", "[NaN]", "not-an-array", '{"broken": true}']),
        }
        return corruptions.get(col_type, lambda: "CORRUPT")()
    
    def _create_nested_dict(self, depth: int) -> Dict:
        """Create deeply nested dictionary."""
        if depth <= 0:
            return {"value": random.random()}
        return {"nested": self._create_nested_dict(depth - 1), "level": depth}
    
    def generate_full_dataset(self) -> Dict[str, Any]:
        """Generate a complete multi-schema dataset."""
        target_bytes = self.config["target_size_mb"] * 1024 * 1024
        
        print(f"Generating synthetic dataset (target: {self.config['target_size_mb']}MB)")
        
        datasets = {}
        
        # Calculate proportions
        tabular_budget = int(target_bytes * 0.50)  # 50% tabular
        timeseries_budget = int(target_bytes * 0.30)  # 30% time-series
        embedding_budget = int(target_bytes * 0.15)  # 15% embeddings
        adversarial_budget = int(target_bytes * 0.05)  # 5% adversarial
        
        # Generate tabular data
        print("  Generating tabular data...")
        estimated_row_size = 500  # bytes per row estimate
        num_rows = max(1000, tabular_budget // estimated_row_size)
        num_cols = random.randint(10, 30)
        tabular_content, tabular_meta = self.generate_tabular_data(num_rows, num_cols)
        datasets["tabular_training.csv"] = {
            "content": tabular_content.encode('utf-8'),
            "metadata": tabular_meta,
        }
        self.stats["schemas_generated"].append("tabular")
        
        # Generate time-series data
        print("  Generating time-series data...")
        estimated_point_size = 100  # bytes per point
        total_points = max(10000, timeseries_budget // estimated_point_size)
        num_series = random.randint(10, 50)
        points_per_series = total_points // num_series
        ts_content, ts_meta = self.generate_timeseries_data(num_series, points_per_series)
        datasets["timeseries_sensors.csv"] = {
            "content": ts_content.encode('utf-8'),
            "metadata": ts_meta,
        }
        self.stats["schemas_generated"].append("timeseries")
        
        # Generate embedding data
        print("  Generating embedding data...")
        embedding_dim = 256
        bytes_per_sample = embedding_dim * 4 + 4  # float32 + int32 label
        num_samples = max(1000, embedding_budget // bytes_per_sample)
        emb_content, emb_meta = self.generate_embedding_data(num_samples, embedding_dim)
        datasets["embeddings.bin"] = {
            "content": emb_content,
            "metadata": emb_meta,
        }
        self.stats["schemas_generated"].append("embeddings")
        
        # Generate adversarial samples
        if self.config["include_adversarial"]:
            print("  Generating adversarial samples...")
            estimated_adv_size = 500
            num_adversarial = max(100, adversarial_budget // estimated_adv_size)
            adv_content, adv_meta = self.generate_adversarial_samples(num_adversarial)
            datasets["adversarial_samples.csv"] = {
                "content": adv_content.encode('utf-8', errors='replace'),
                "metadata": adv_meta,
            }
            self.stats["schemas_generated"].append("adversarial")
        
        # Calculate final stats
        self.stats["null_rate"] = sum(
            d["metadata"].get("null_rate", 0) for d in datasets.values()
        ) / len(datasets)
        self.stats["corruption_rate"] = sum(
            d["metadata"].get("corruption_rate", 0) + d["metadata"].get("anomaly_rate", 0) 
            for d in datasets.values()
        ) / len(datasets)
        
        # Generate manifest
        manifest = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "generator_version": "1.0.0",
            "seed": self.config["seed"],
            "target_size_mb": self.config["target_size_mb"],
            "actual_size_bytes": self.stats["total_bytes"],
            "actual_size_mb": round(self.stats["total_bytes"] / (1024 * 1024), 2),
            "files": {
                name: {
                    "size_bytes": data["metadata"]["size_bytes"],
                    "type": data["metadata"]["type"],
                    "checksum_sha256": hashlib.sha256(data["content"]).hexdigest(),
                }
                for name, data in datasets.items()
            },
            "statistics": self.stats,
        }
        
        datasets["manifest.json"] = {
            "content": json.dumps(manifest, indent=2).encode('utf-8'),
            "metadata": {"type": "manifest"},
        }
        
        print(f"  Generated {len(datasets)} files, total {self.stats['total_bytes'] / (1024*1024):.2f}MB")
        
        return {
            "datasets": datasets,
            "manifest": manifest,
            "stats": self.stats,
        }


def save_datasets(output_dir: str, datasets: Dict) -> List[str]:
    """Save generated datasets to disk."""
    os.makedirs(output_dir, exist_ok=True)
    
    saved_files = []
    for filename, data in datasets.items():
        filepath = os.path.join(output_dir, filename)
        mode = 'wb' if isinstance(data["content"], bytes) else 'w'
        
        with open(filepath, mode) as f:
            if mode == 'wb':
                f.write(data["content"])
            else:
                f.write(data["content"])
        
        saved_files.append(filepath)
        print(f"  Saved: {filepath} ({len(data['content'])} bytes)")
    
    return saved_files


if __name__ == "__main__":
    print("=" * 60)
    print("SynthOS Synthetic Dataset Generator")
    print("=" * 60)
    
    generator = DatasetGenerator(CONFIG)
    result = generator.generate_full_dataset()
    
    saved = save_datasets(CONFIG["output_dir"], result["datasets"])
    
    print("\n" + "=" * 60)
    print("Generation Complete!")
    print("=" * 60)
    print(f"Total files: {len(saved)}")
    print(f"Total size: {result['stats']['total_bytes'] / (1024*1024):.2f} MB")
    print(f"Total rows: {result['stats']['total_rows']:,}")
    print(f"Schemas: {', '.join(result['stats']['schemas_generated'])}")
    print(f"Output: {CONFIG['output_dir']}")
