# 🚀 GCP H100 Deployment Guide

## Instance Configuration

**Type**: `a3-highgpu-4g`  
**Location**: `us-central1-b`  
**GPUs**: 4x NVIDIA H100 (80GB each)  
**Cost**: $44.36/hour ($32,383.85/month)

---

## 📋 Instance Specifications

```yaml
Compute:
  vCPUs: 104
  RAM: 936 GB
  GPUs: 4x NVIDIA H100 80GB (320GB total)

Storage:
  Boot Disk: 500 GB Hyperdisk Balanced
  - 6,000 provisioned IOPS
  - 890 MB/s throughput
  
  Local SSD: 8 disks x 375 GB = 3 TB NVMe
  - Ultra-fast local storage
  - Perfect for training data cache

OS:
  Image: Rocky Linux 8
  NVIDIA Driver: 580 (latest)
  CUDA: 12.4
  cuDNN: 9.0
```

---

## 🛠️ Setup Instructions

### 1. Create Instance (GCP Console or CLI)

```bash
gcloud compute instances create synthos-ml-validator \
    --zone=us-central1-b \
    --machine-type=a3-highgpu-4g \
    --accelerator=type=nvidia-h100-80gb,count=4 \
    --image=rocky-linux-8-nvidia-580 \
    --boot-disk-type=hyperdisk-balanced \
    --boot-disk-size=500GB \
    --boot-disk-provisioned-iops=6000 \
    --boot-disk-provisioned-throughput=890 \
    --local-ssd=interface=nvme,count=8 \
    --network-interface=network-tier=PREMIUM \
    --tags=ml-validator,allow-grpc \
    --metadata=enable-osconfig=TRUE
```

### 2. SSH into Instance

```bash
gcloud compute ssh synthos-ml-validator --zone=us-central1-b
```

### 3. Verify GPU Setup

```bash
# Check GPUs
nvidia-smi

# Expected output:
# +-----------------------------------------------------------------------------+
# | NVIDIA-SMI 580.XX       Driver Version: 580.XX       CUDA Version: 12.4     |
# |-------------------------------+----------------------+----------------------+
# | GPU  Name        Persistence-M| Bus-Id        Disp.A | Volatile Uncorr. ECC |
# | Fan  Temp  Perf  Pwr:Usage/Cap|         Memory-Usage | GPU-Util  Compute M. |
# |===============================+======================+======================|
# |   0  H100 80GB           On   | 00000000:00:04.0 Off |                    0 |
# | N/A   30C    P0    70W / 700W |      0MiB / 81559MiB |      0%      Default |
# |   1  H100 80GB           On   | 00000000:00:05.0 Off |                    0 |
# | N/A   29C    P0    69W / 700W |      0MiB / 81559MiB |      0%      Default |
# |   2  H100 80GB           On   | 00000000:00:06.0 Off |                    0 |
# | N/A   31C    P0    71W / 700W |      0MiB / 81559MiB |      0%      Default |
# |   3  H100 80GB           On   | 00000000:00:07.0 Off |                    0 |
# | N/A   28C    P0    68W / 700W |      0MiB / 81559MiB |      0%      Default |
# +-----------------------------------------------------------------------------+
```

### 4. Mount Local SSDs

```bash
# Find NVMe devices
lsblk | grep nvme

# Create RAID 0 array for maximum performance
sudo mdadm --create /dev/md0 --level=0 --raid-devices=8 \
    /dev/nvme0n1 /dev/nvme0n2 /dev/nvme0n3 /dev/nvme0n4 \
    /dev/nvme0n5 /dev/nvme0n6 /dev/nvme0n7 /dev/nvme0n8

# Format with XFS (best for ML workloads)
sudo mkfs.xfs /dev/md0

# Mount
sudo mkdir -p /mnt/localssd
sudo mount /dev/md0 /mnt/localssd
sudo chown $USER:$USER /mnt/localssd

# Auto-mount on reboot
echo '/dev/md0 /mnt/localssd xfs defaults,noatime 0 0' | sudo tee -a /etc/fstab
```

### 5. Install System Dependencies

```bash
# Update system
sudo dnf update -y

# Install development tools
sudo dnf groupinstall "Development Tools" -y
sudo dnf install -y git wget curl vim htop tmux

# Install Python 3.12
sudo dnf install -y python312 python312-devel python312-pip

# Verify CUDA
nvcc --version  # Should show CUDA 12.4
```

### 6. Clone and Setup Project

```bash
# Create workspace
mkdir -p ~/workspace
cd ~/workspace

# Clone repository (or upload via scp/gcloud)
git clone https://github.com/tafolabi009/ml_backend.git
cd ml_backend

# Create virtual environment
python3.12 -m venv venv
source venv/bin/activate

# Install PyTorch with CUDA 12.4 support
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu124

# Install project dependencies
pip install -r requirements.txt

# Install custom architectures
pip install packages/resonance_nn-0.1.0-py3-none-any.whl
pip install packages/temporal_eigenstate_networks-0.1.0-py3-none-any.whl

# Verify installation
python verify_installation.py
```

### 7. Configure Storage Paths

```bash
# Create data directories on local SSD
mkdir -p /mnt/localssd/{datasets,models,checkpoints,signatures,cache}

# Update config to use local SSD
export ML_DATA_DIR=/mnt/localssd
export ML_CACHE_DIR=/mnt/localssd/cache
export SIGNATURE_LIBRARY_PATH=/mnt/localssd/signatures
```

### 8. Generate mTLS Certificates

```bash
cd ~/workspace/ml_backend
bash scripts/generate_certs.sh

# Copy to standard location
sudo mkdir -p /etc/synthos/certs
sudo cp /tmp/synthos_certs/* /etc/synthos/certs/
sudo chown -R $USER:$USER /etc/synthos/certs
```

### 9. Test Installation

```bash
# Run example pipeline
python examples/complete_pipeline.py

# Expected output:
# ✅ All 6 steps completed successfully!
# 📊 Final Assessment:
#    - Current Score: 72.4/100
#    - Collapse Detected: False
```

### 10. Start gRPC Server

```bash
# Option 1: Foreground (for testing)
python src/grpc_services/validation_server.py

# Option 2: Background with nohup
nohup python src/grpc_services/validation_server.py > server.log 2>&1 &

# Option 3: Systemd service (recommended)
sudo cp deployment/systemd/synthos-validator.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable synthos-validator
sudo systemctl start synthos-validator
sudo systemctl status synthos-validator
```

---

## 🔥 Performance Optimization

### GPU Optimization Checklist

```bash
# Enable persistence mode (reduces startup latency)
sudo nvidia-smi -pm 1

# Set power limit to max (700W per H100)
sudo nvidia-smi -pl 700

# Set compute mode to exclusive process
sudo nvidia-smi -c 3

# Verify settings
nvidia-smi -q | grep -A 5 "Persistence Mode\|Power Limit\|Compute Mode"
```

### System Tuning

```bash
# Increase file descriptor limits
echo "* soft nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "* hard nofile 65536" | sudo tee -a /etc/security/limits.conf

# Disable transparent huge pages (can hurt performance)
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled

# Set CPU governor to performance
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Verify
cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor  # Should be 'performance'
```

---

## 📊 Monitoring

### Real-time GPU Monitoring

```bash
# Watch GPU utilization
watch -n 1 nvidia-smi

# Detailed GPU stats
nvidia-smi dmon -s pucvmet

# Log GPU metrics
nvidia-smi --query-gpu=timestamp,name,temperature.gpu,utilization.gpu,utilization.memory,memory.used,memory.total,power.draw --format=csv -l 10 > gpu_metrics.csv
```

### Application Monitoring

```bash
# Install Ops Agent (for Cloud Monitoring)
curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
sudo bash add-google-cloud-ops-agent-repo.sh --also-install

# Monitor logs
tail -f server.log

# Monitor Python process
htop -p $(pgrep -f validation_server)
```

---

## 🔒 Security Configuration

### Firewall Rules

```bash
# Allow gRPC traffic (port 50051)
gcloud compute firewall-rules create allow-grpc \
    --allow=tcp:50051 \
    --source-ranges=10.0.0.0/8 \  # Adjust for your VPC
    --target-tags=ml-validator

# Allow SSH
gcloud compute firewall-rules create allow-ssh \
    --allow=tcp:22 \
    --source-ranges=0.0.0.0/0 \  # Restrict in production!
    --target-tags=ml-validator

# Allow internal monitoring
gcloud compute firewall-rules create allow-monitoring \
    --allow=tcp:9090,tcp:9100 \
    --source-ranges=10.0.0.0/8 \
    --target-tags=ml-validator
```

### mTLS Configuration

Already configured! Certificates in `/etc/synthos/certs/`:
- `ca.crt` - Certificate Authority
- `server.crt` - ML service certificate
- `server.key` - ML service private key
- `client.crt` - Backend client certificate
- `client.key` - Backend client private key

---

## 💰 Cost Management

### Current Costs

| Item | Monthly Cost | Notes |
|------|--------------|-------|
| 4x H100 GPUs | $28,605.93 | 89% of total cost |
| Compute (104 vCPU + 936GB) | $3,452.92 | |
| Local SSD (3TB) | $240.00 | |
| Hyperdisk | $85.00 | Including IOPS/throughput |
| **TOTAL** | **$32,383.85** | **$44.36/hour** |

### Cost Optimization Strategies

1. **Use Preemptible/Spot Instances** (70% discount)
   ```bash
   --preemptible  # Add to gcloud command
   # Risk: Can be terminated anytime
   # Good for: Non-critical validations
   ```

2. **Committed Use Discounts** (37-55% discount)
   - 1-year: 37% discount
   - 3-year: 55% discount

3. **Auto-shutdown During Idle**
   ```bash
   # Add to crontab
   0 2 * * * /usr/local/bin/auto-shutdown-if-idle.sh
   ```

4. **Right-sizing**
   - Use `a3-highgpu-2g` (2x H100) if validations fit in 160GB: **50% cost savings**
   - Use `a3-highgpu-1g` (1x H100) for smaller workloads: **75% cost savings**

---

## 🧪 Testing

### Run Test Suite

```bash
# Unit tests
pytest tests/unit/ -v --cov=src

# Integration tests
pytest tests/integration/ -v

# Load test (synthetic 100M rows)
python tests/load/test_large_dataset.py
```

### Benchmark Performance

```bash
# Test cascade training
python scripts/benchmark_cascade.py

# Profile GPU utilization
python scripts/profile_gpu.py --duration=300  # 5 minutes
```

---

## 📈 Expected Performance

| Dataset Size | Processing Time | Cost | GPU Util |
|--------------|----------------|------|----------|
| 10K rows | <1 min | $0.74 | 45% |
| 1M rows | 5 min | $3.70 | 75% |
| 100M rows | 45 min | $33.27 | 85% |
| 1B rows | 6 hours | $266.16 | 90% |

*Based on 4x H100 at $44.36/hour*

---

## 🆘 Troubleshooting

### Common Issues

**Issue**: CUDA out of memory
```bash
# Solution: Reduce batch size in config/hardware_config.yaml
batch_size: 64  # Try 32 or 16
```

**Issue**: Slow data loading
```bash
# Solution: Verify data is on local SSD
df -h /mnt/localssd
# Move data if needed:
cp /path/to/dataset.parquet /mnt/localssd/datasets/
```

**Issue**: Low GPU utilization
```bash
# Solution: Increase num_workers in DataLoader
num_workers: 16  # Try 32 with 104 vCPUs
```

**Issue**: gRPC connection refused
```bash
# Check if server is running
ps aux | grep validation_server

# Check firewall
sudo firewall-cmd --list-all

# Check logs
tail -f server.log
```

---

## 📞 Support

- **Documentation**: `/workspace/ml_backend/docs/`
- **Examples**: `/workspace/ml_backend/examples/`
- **Logs**: `~/workspace/ml_backend/server.log`
- **GCP Console**: https://console.cloud.google.com/compute/instances

---

**Status**: Ready for production deployment! 🚀

**Last Updated**: October 31, 2025
