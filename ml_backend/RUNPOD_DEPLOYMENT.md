# RunPod Deployment for SynthOS ML Backend

## Quick Start (5 minutes)

### Step 1: Get Your API Key
1. Go to [RunPod Settings](https://www.runpod.io/console/user/settings)
2. Click **API Keys** → **Create API Key**
3. Copy the key

### Step 2: Deploy via RunPod Console (Easiest)

1. Go to [RunPod Pods](https://www.runpod.io/console/pods)
2. Click **+ Deploy**
3. Configure:

| Setting | Value |
|---------|-------|
| **GPU Type** | NVIDIA A10G |
| **GPU Count** | 4 (or start with 1 for testing) |
| **Template** | RunPod Pytorch 2.1 |
| **Container Disk** | 50 GB |
| **Volume Disk** | 100 GB |
| **Volume Path** | /workspace |

4. Click **Deploy**

### Step 3: Connect and Setup

Once the pod is running:

```bash
# SSH into the pod (get command from RunPod console)
ssh root@<pod-ip> -i ~/.ssh/id_rsa

# Inside the pod:
cd /workspace

# Clone the repo
git clone https://github.com/tafolabi009/ml_backend.git
cd ml_backend/ml_backend

# Install dependencies
pip install -r requirements.txt

# Install TEN (Temporal Eigenstate Networks)
pip install git+https://github.com/tafolabi009/NEURON_NEW.git

# Set environment variables
export GPU_TIER=auto
export MAX_GPU_MEMORY_FRACTION=0.9
export ENABLE_MIXED_PRECISION=true
export DATABASE_URL="postgresql://synthos:YOUR_PASSWORD@synthos-db.csdea42am9u5.us-east-1.rds.amazonaws.com:5432/synthos"
export REDIS_URL="redis://synthos-redis.2zdx8r.0001.use1.cache.amazonaws.com:6379"

# Start the ML backend
python server.py
```

### Step 4: Expose gRPC Ports

RunPod automatically exposes HTTP ports. For gRPC, use their TCP proxy:

1. In RunPod console, click on your pod
2. Go to **Connect** tab
3. Note the **TCP Proxy** endpoints for ports 50051, 50052, 50054

---

## Cost Breakdown

| Configuration | GPUs | Cost/Hour | Cost/Month (24/7) |
|--------------|------|-----------|-------------------|
| 1x A10G | 1 | $0.80 | $576 |
| **4x A10G** | 4 | **$3.20** | **$2,304** |
| 1x A100 40GB | 1 | $1.89 | $1,360 |
| 4x A100 40GB | 4 | $7.56 | $5,443 |

**Recommended: Start with 1x A10G ($0.80/hr) for testing, scale to 4x for production**

---

## Alternative: RunPod Serverless (Pay-per-request)

For intermittent workloads, RunPod Serverless is more cost-effective:

1. Go to [RunPod Serverless](https://www.runpod.io/console/serverless)
2. Create an **Endpoint**
3. Use our Docker image as the handler

```python
# handler.py for RunPod Serverless
import runpod

def handler(event):
    """
    RunPod serverless handler for ML validation
    """
    input_data = event.get("input", {})
    dataset_path = input_data.get("dataset_path")
    
    # Import and run validation
    from src.orchestrator import SynthosOrchestrator
    
    orchestrator = SynthosOrchestrator()
    result = await orchestrator.validate(dataset_path, "parquet")
    
    return result.to_dict()

runpod.serverless.start({"handler": handler})
```

---

## Connect RunPod to AWS API

Once your RunPod ML backend is running, update the Go backend to connect:

```bash
# In AWS Secrets Manager, update:
ML_BACKEND_HOST=<runpod-tcp-proxy-host>
ML_BACKEND_PORT=<runpod-tcp-proxy-port>
```

Or directly in ECS task definition environment variables.
