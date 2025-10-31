# 🚀 Resonance NN - Quick Reference Card

**Version:** 0.1.0 | **O(n log n) Complexity** | **200K Context Length**

---

## 📦 Installation (One Command)

```bash
pip install resonance_nn-0.1.0-py3-none-any.whl
```

---

## 🎯 Most Common Use Cases

### 1️⃣ Text Generation
```python
from resonance_nn import create_spectral_lm
import torch

model = create_spectral_lm('base', vocab_size=50257)
input_ids = torch.randint(0, 50257, (1, 1024))
output = model.generate(input_ids, max_length=100)
```

### 2️⃣ Classification
```python
from resonance_nn import SpectralClassifier, SpectralConfig

config = SpectralConfig(
    vocab_size=30522,
    embed_dim=768,
    hidden_dim=3072,
    num_layers=12,
    num_heads=12,
    max_seq_len=512
)
model = SpectralClassifier(config, num_classes=2)
logits = model(input_ids)
```

### 3️⃣ Embeddings
```python
from resonance_nn import SpectralEncoder

encoder = SpectralEncoder(config)
embeddings = encoder(input_ids)
sentence_emb = embeddings.mean(dim=1)
```

---

## 🎚️ Model Sizes (Choose One)

| Size | Params | Context | Recommendation |
|------|--------|---------|----------------|
| `tiny` | 77M | 16K | Prototyping |
| `small` | 454M | 65K | Development |
| **`base`** | **983M** | **131K** | **MVP (Start Here)** |
| `medium` | 3.3B | 200K | High Performance |
| `large` | 9.8B | 200K | Production |

```python
model = create_spectral_lm('base', vocab_size=50257)
```

---

## ⚡ Performance

| Sequence | Spectral | Transformer | Speedup |
|----------|----------|-------------|---------|
| 512 | 37ms | 54ms | 1.5x |
| 2048 | 134ms | 283ms | 2.1x |
| 8192 | 568ms | 2555ms | **4.5x** |

---

## 🔧 Essential Code Patterns

### Training Loop
```python
model.train()
for batch in dataloader:
    optimizer.zero_grad()
    logits = model(input_ids)
    loss = criterion(logits.view(-1, vocab_size), labels.view(-1))
    loss.backward()
    torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
    optimizer.step()
```

### Inference
```python
model.eval()
with torch.no_grad():
    output = model(input_ids)
```

### Save/Load
```python
# Save
torch.save(model.state_dict(), 'model.pth')

# Load
model.load_state_dict(torch.load('model.pth'))
```

### GPU Usage
```python
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
model = model.to(device)
input_ids = input_ids.to(device)
```

---

## 🎛️ Configuration Options

```python
from resonance_nn import SpectralConfig, LayerType

config = SpectralConfig(
    vocab_size=50257,              # Your vocabulary size
    embed_dim=768,                 # Embedding dimension
    hidden_dim=3072,               # FFT processing dimension
    num_layers=12,                 # Number of layers
    num_heads=12,                  # Number of frequency heads
    max_seq_len=2048,              # Maximum sequence length
    dropout=0.1,                   # Dropout rate
    layer_type=LayerType.DENSE,    # DENSE, SPARSE, MOE
    use_gradient_checkpointing=False,  # Memory optimization
)
```

---

## 🐛 Quick Fixes

### Out of Memory?
```python
# Use smaller batch size
batch_size = 4

# OR use smaller model
model = create_spectral_lm('small', vocab_size=50257)

# OR enable checkpointing
config.use_gradient_checkpointing = True
```

### Slow Inference?
```python
# Ensure eval mode
model.eval()

# Use no_grad
with torch.no_grad():
    output = model(input_ids)

# Move to GPU
model = model.cuda()
```

### Import Error?
```bash
pip install --force-reinstall resonance_nn-0.1.0-py3-none-any.whl
```

---

## 📊 Typical Memory Usage

| Model Size | GPU Memory | CPU Memory | Batch Size |
|------------|------------|------------|------------|
| Small | ~2GB | ~4GB | 16-32 |
| **Base** | **~4GB** | **~8GB** | **8-16** |
| Medium | ~13GB | ~26GB | 4-8 |
| Large | ~40GB | ~80GB | 1-4 |

---

## 🎯 MVP Checklist

- [ ] Install: `pip install resonance_nn-0.1.0-py3-none-any.whl`
- [ ] Verify: `python verify_installation.py`
- [ ] Import: `from resonance_nn import create_spectral_lm`
- [ ] Create: `model = create_spectral_lm('base', vocab_size=50257)`
- [ ] Test: Run forward pass with dummy data
- [ ] Train: Fine-tune on your dataset
- [ ] Deploy: Save and load model in production

---

## 📚 Documentation Files

1. **`DISTRIBUTION_README.md`** - Full distribution info
2. **`INSTALLATION_GUIDE.md`** - Detailed usage guide
3. **`README.md`** - Complete project documentation
4. **`verify_installation.py`** - Test script
5. **This file** - Quick reference

---

## 📞 Need Help?

- **Email:** afolabi@genovotech.com
- **GitHub:** https://github.com/tafolabi009/RNN
- **Issues:** https://github.com/tafolabi009/RNN/issues

---

## 💡 One-Liner Examples

```python
# Quick import
from resonance_nn import create_spectral_lm, SpectralClassifier

# Create model
model = create_spectral_lm('base', vocab_size=50257)

# List models
from resonance_nn import list_available_models
list_available_models()

# Get model info
from resonance_nn import get_model_info
info = get_model_info('base')
print(f"Parameters: {info['params_m']:.0f}M")
```

---

## 🚀 Start Coding

```python
# Copy this to get started
from resonance_nn import create_spectral_lm
import torch

# Create model
model = create_spectral_lm('base', vocab_size=50257)
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
model.to(device)

# Your MVP code here...
print("✅ Ready to build!")
```

---

**Built with ❤️ and FFTs | Faster than transformers on long sequences**

*October 31, 2025*
