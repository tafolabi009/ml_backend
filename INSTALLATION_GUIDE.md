# Resonance NN - Installation Guide for ML Engineers

## 📦 Distribution Package

**Package Name:** `resonance-nn`  
**Version:** 0.1.0  
**Architecture:** Spectral Neural Networks with O(n log n) complexity

---

## 🚀 Quick Installation

### Option 1: Install from Wheel File (Recommended - Fastest)

```bash
pip install resonance_nn-0.1.0-py3-none-any.whl
```

### Option 2: Install from Source Distribution

```bash
pip install resonance_nn-0.1.0.tar.gz
```

### Option 3: Install from GitHub

```bash
pip install git+https://github.com/tafolabi009/RNN.git
```

---

## 📋 System Requirements

- **Python:** 3.8 or higher
- **PyTorch:** 2.0.0 or higher
- **NumPy:** 1.21.0 or higher
- **SciPy:** 1.7.0 or higher

Dependencies will be installed automatically.

---

## 🎯 Basic Usage Examples

### 1. Language Model (Text Generation)

```python
from resonance_nn import create_spectral_lm
import torch

# Create a base model (983M parameters, 131K context)
model = create_spectral_lm('base', vocab_size=50257)
print(f"Parameters: {model.get_num_params()/1e6:.1f}M")

# Forward pass
input_ids = torch.randint(0, 50257, (2, 1024))
logits = model(input_ids)  # Shape: (2, 1024, 50257)

# Generate text
from transformers import GPT2TokenizerFast
tokenizer = GPT2TokenizerFast.from_pretrained('gpt2')

prompt = tokenizer.encode("The future of AI is", return_tensors='pt')
generated = model.generate(prompt, max_length=100, temperature=0.8)
print(tokenizer.decode(generated[0]))
```

### 2. Classification Model

```python
from resonance_nn import SpectralClassifier, SpectralConfig, ModalityType
import torch

# Configure classifier
config = SpectralConfig(
    vocab_size=30522,        # BERT vocabulary
    embed_dim=768,
    hidden_dim=3072,
    num_layers=12,
    max_seq_len=512,
    modality=ModalityType.TEXT
)

# Create classifier
model = SpectralClassifier(config, num_classes=2)

# Classify
input_ids = torch.randint(0, 30522, (4, 128))
logits = model(input_ids)  # Shape: (4, 2)
predictions = logits.argmax(dim=-1)
```

### 3. Custom Configuration

```python
from resonance_nn import SpectralLanguageModel, SpectralConfig, LayerType

# Create custom config
config = SpectralConfig(
    vocab_size=50257,
    hidden_dim=1536,
    num_layers=16,
    layer_type=LayerType.SPARSE,  # Use sparse processing
    sparsity=0.10,                # Keep 10% of frequencies
    use_moe=True,                 # Enable Mixture of Experts
    num_experts=16,
    max_seq_len=8192
)

# Create model
model = SpectralLanguageModel(config)
```

### 4. Multi-Modal (Vision + Text)

```python
from resonance_nn import SpectralVisionEncoder, SpectralCrossModalFusion, SpectralConfig

# Configure for vision
config = SpectralConfig(
    hidden_dim=768,
    num_layers=12,
    max_seq_len=1024,
    modality=ModalityType.VISION
)

# Vision encoder (no attention!)
vision_encoder = SpectralVisionEncoder(config)
image_features = vision_encoder(images)  # (batch, patches, 768)

# Cross-modal fusion (no cross-attention!)
fusion = SpectralCrossModalFusion(config)
fused = fusion(text_features=text_emb, vision_features=image_features)
```

---

## 🏗️ Available Model Sizes

Use `create_spectral_lm(size, vocab_size)` with these sizes:

| Size | Parameters | Context Length | Hidden Dim | Use Case |
|------|------------|----------------|------------|----------|
| `tiny` | 77M | 16K | 1024 | Fast prototyping, edge devices |
| `small` | 454M | 65K | 2048 | Development, fine-tuning |
| `base` | 983M | 131K | 3072 | **Production (Recommended)** |
| `medium` | 3.3B | 200K | 4096 | High performance |
| `large` | 9.8B | 200K | 6144 | State-of-the-art |
| `xlarge` | 21.7B | 200K | 8192 | Research, largest scale |

```python
# List all available models
from resonance_nn import list_available_models
list_available_models()
```

---

## 🎓 Training Your Own Models

```python
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from resonance_nn import create_spectral_lm

# Setup
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
model = create_spectral_lm('base', vocab_size=50257).to(device)
optimizer = torch.optim.AdamW(model.parameters(), lr=1e-4)
criterion = nn.CrossEntropyLoss()

# Training loop
model.train()
for epoch in range(num_epochs):
    for batch in train_loader:
        input_ids = batch['input_ids'].to(device)
        labels = batch['labels'].to(device)
        
        # Forward pass
        logits = model(input_ids)
        loss = criterion(logits.view(-1, vocab_size), labels.view(-1))
        
        # Backward pass
        optimizer.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()
        
        print(f"Epoch {epoch}, Loss: {loss.item():.4f}")

# Save model
torch.save(model.state_dict(), 'spectral_model.pth')
```

---

## 💾 Loading Pretrained Models

```python
from resonance_nn import create_spectral_lm
import torch

# Create model
model = create_spectral_lm('base', vocab_size=50257)

# Load weights
checkpoint = torch.load('spectral_model.pth', map_location='cpu')
model.load_state_dict(checkpoint)
model.eval()

# Use for inference
with torch.no_grad():
    output = model(input_ids)
```

---

## 🔧 Advanced Features

### GPU/TPU Optimization

```python
from torch.cuda.amp import autocast, GradScaler

# Mixed precision training
scaler = GradScaler()

with autocast():
    logits = model(input_ids)
    loss = criterion(logits.view(-1, vocab_size), labels.view(-1))

scaler.scale(loss).backward()
scaler.step(optimizer)
scaler.update()
```

### Gradient Checkpointing (Large Models)

```python
config = SpectralConfig(
    vocab_size=50257,
    hidden_dim=4096,
    num_layers=24,
    use_gradient_checkpointing=True  # Save memory
)
model = SpectralLanguageModel(config)
```

### XLA/TPU Support

```python
import torch_xla.core.xla_model as xm

device = xm.xla_device()
model = model.to(device)

# Enable XLA compilation
config = SpectralConfig(use_xla=True)
```

---

## 📊 Performance Benchmarks

### Speed Comparison (vs Standard Transformer)

| Sequence Length | Spectral NN | Transformer | Speedup |
|-----------------|-------------|-------------|---------|
| 512 tokens | 36.6 ms | 53.6 ms | 1.5x |
| 2,048 tokens | 134 ms | 283 ms | 2.1x |
| 4,096 tokens | 332 ms | 840 ms | 2.5x |
| 8,192 tokens | 568 ms | 2,555 ms | **4.5x** 🚀 |
| 16,384 tokens | 1,576 ms | 10,074 ms | **6.4x** 🔥 |

**Context Length:** Up to 200K tokens (6x longer than GPT-4)

---

## 🐛 Troubleshooting

### Import Error

```bash
# If you see "No module named 'resonance_nn'"
pip install --upgrade resonance_nn-0.1.0-py3-none-any.whl
```

### CUDA Out of Memory

```python
# Use smaller batch size or sequence length
# Enable gradient checkpointing
config.use_gradient_checkpointing = True

# Use mixed precision
from torch.cuda.amp import autocast
with autocast():
    output = model(input_ids)
```

### Slow Performance

```python
# Enable fused operations
config.use_fused_ops = True

# Use smaller model for prototyping
model = create_spectral_lm('small', vocab_size=50257)
```

---

## 📚 API Reference

### Main Classes

- **`SpectralLanguageModel`** - Language model for text generation
- **`SpectralClassifier`** - Classification model (sentiment, topic, etc.)
- **`SpectralEncoder`** - Encoder for embeddings
- **`SpectralSeq2Seq`** - Sequence-to-sequence model (translation, summarization)
- **`SpectralVisionEncoder`** - Vision encoder (images)
- **`SpectralAudioEncoder`** - Audio encoder (speech, music)

### Configuration

```python
from resonance_nn import SpectralConfig, LayerType, ModalityType

config = SpectralConfig(
    vocab_size=50257,           # Vocabulary size
    embed_dim=768,              # Embedding dimension
    hidden_dim=3072,            # Hidden dimension (FFT processing)
    num_layers=12,              # Number of layers
    num_heads=12,               # Number of frequency heads
    max_seq_len=2048,           # Maximum sequence length
    dropout=0.1,                # Dropout rate
    layer_type=LayerType.DENSE, # DENSE, SPARSE, MOE, MULTISCALE
    modality=ModalityType.TEXT, # TEXT, VISION, AUDIO
    use_gradient_checkpointing=False,  # Memory optimization
    use_fused_ops=False,        # Speed optimization
)
```

---

## 🤝 Support

- **GitHub Issues:** https://github.com/tafolabi009/RNN/issues
- **Email:** afolabi@genovotech.com
- **Documentation:** See `README.md` for detailed architecture information

---

## 📄 License

MIT License - See `LICENSE` file

---

## 🎯 Quick Start Checklist

- [ ] Install package: `pip install resonance_nn-0.1.0-py3-none-any.whl`
- [ ] Verify installation: `python -c "import resonance_nn; print(resonance_nn.__version__)"`
- [ ] Run example: Copy one of the usage examples above
- [ ] Train on your data: Adapt training script to your dataset
- [ ] Build MVP: Integrate into your application

---

**Built with ❤️ and FFTs | O(n log n) > O(n²)**

*Version: 0.1.0 | Last Updated: October 31, 2025*
