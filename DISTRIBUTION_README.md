# 📦 Resonance NN Distribution Package

**Version:** 0.1.0  
**Package Date:** October 31, 2025  
**License:** MIT

---

## 📋 What's Inside

This distribution contains:

```
dist/
├── resonance_nn-0.1.0-py3-none-any.whl    # Wheel package (RECOMMENDED)
└── resonance_nn-0.1.0.tar.gz              # Source distribution
```

---

## 🚀 Quick Start for ML Engineers

### Step 1: Install the Package

**Option A: Using Wheel (Fastest)**
```bash
pip install resonance_nn-0.1.0-py3-none-any.whl
```

**Option B: Using Source Distribution**
```bash
pip install resonance_nn-0.1.0.tar.gz
```

**Option C: From GitHub**
```bash
pip install git+https://github.com/tafolabi009/RNN.git
```

### Step 2: Verify Installation

```bash
python verify_installation.py
```

Or quickly test:
```bash
python -c "import resonance_nn; print(f'✅ v{resonance_nn.__version__}')"
```

### Step 3: Start Building Your MVP

```python
from resonance_nn import create_spectral_lm

# Create model
model = create_spectral_lm('base', vocab_size=50257)

# Use in your application
import torch
input_ids = torch.randint(0, 50257, (2, 1024))
output = model(input_ids)
```

---

## 📁 Additional Files

- **`INSTALLATION_GUIDE.md`** - Complete installation and usage guide
- **`verify_installation.py`** - Test script to verify installation
- **`README.md`** - Full project documentation
- **`LICENSE`** - MIT License
- **`examples/`** - Usage examples

---

## 🎯 Key Features for Your MVP

### 1. **Fast Inference (4-6x faster on long sequences)**
```python
# Perfect for processing long documents
model = create_spectral_lm('base', vocab_size=50257)
long_input = torch.randint(0, 50257, (1, 8192))  # 8K tokens
output = model(long_input)  # 568ms vs 2555ms (transformer)
```

### 2. **Classification Tasks**
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
classifier = SpectralClassifier(config, num_classes=2)
```

### 3. **Text Generation**
```python
from transformers import GPT2TokenizerFast

tokenizer = GPT2TokenizerFast.from_pretrained('gpt2')
prompt = tokenizer.encode("The future of AI is", return_tensors='pt')

# Generate with temperature control
output = model.generate(
    prompt, 
    max_length=100, 
    temperature=0.8,
    top_p=0.9
)
print(tokenizer.decode(output[0]))
```

### 4. **Easy Model Scaling**
```python
# Start small for prototyping
prototype = create_spectral_lm('small', vocab_size=50257)  # 454M params

# Scale up for production
production = create_spectral_lm('base', vocab_size=50257)   # 983M params

# Go large for performance
large = create_spectral_lm('large', vocab_size=50257)       # 9.8B params
```

---

## 💡 MVP Integration Examples

### Example 1: Document Analysis Service

```python
import torch
from resonance_nn import create_spectral_lm
from transformers import GPT2TokenizerFast

class DocumentAnalyzer:
    def __init__(self):
        self.model = create_spectral_lm('base', vocab_size=50257)
        self.tokenizer = GPT2TokenizerFast.from_pretrained('gpt2')
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.model.to(self.device)
        self.model.eval()
    
    def analyze(self, document_text):
        # Tokenize (supports up to 131K tokens!)
        input_ids = self.tokenizer.encode(
            document_text, 
            return_tensors='pt',
            max_length=131072,
            truncation=True
        ).to(self.device)
        
        # Get embeddings
        with torch.no_grad():
            output = self.model(input_ids)
        
        return output

# Usage
analyzer = DocumentAnalyzer()
result = analyzer.analyze("Your long document here...")
```

### Example 2: Real-time Classification API

```python
from fastapi import FastAPI
from pydantic import BaseModel
from resonance_nn import SpectralClassifier, SpectralConfig
import torch

app = FastAPI()

# Initialize model once
config = SpectralConfig(
    vocab_size=30522,
    embed_dim=768,
    hidden_dim=3072,
    num_layers=12,
    num_heads=12,
    max_seq_len=512
)
model = SpectralClassifier(config, num_classes=2)
model.eval()

class TextInput(BaseModel):
    text: str

@app.post("/classify")
def classify(input: TextInput):
    # Tokenize and classify
    # ... your tokenization logic ...
    
    with torch.no_grad():
        logits = model(input_ids)
        prediction = logits.argmax(dim=-1).item()
    
    return {"prediction": prediction, "confidence": logits.softmax(dim=-1).max().item()}
```

### Example 3: Batch Processing Pipeline

```python
import torch
from torch.utils.data import DataLoader
from resonance_nn import create_spectral_lm

class BatchProcessor:
    def __init__(self, model_size='base', batch_size=8):
        self.model = create_spectral_lm(model_size, vocab_size=50257)
        self.batch_size = batch_size
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.model.to(self.device)
        self.model.eval()
    
    def process_batch(self, dataloader):
        results = []
        
        for batch in dataloader:
            input_ids = batch['input_ids'].to(self.device)
            
            with torch.no_grad():
                output = self.model(input_ids)
            
            results.append(output.cpu())
        
        return torch.cat(results, dim=0)

# Usage
processor = BatchProcessor(model_size='base', batch_size=16)
results = processor.process_batch(your_dataloader)
```

---

## 🔧 Production Deployment Tips

### 1. Memory Optimization

```python
# Enable gradient checkpointing for large models
config = SpectralConfig(
    vocab_size=50257,
    hidden_dim=4096,
    num_layers=24,
    use_gradient_checkpointing=True  # Reduces memory usage
)
```

### 2. Speed Optimization

```python
# Use mixed precision for faster inference
from torch.cuda.amp import autocast

with autocast():
    output = model(input_ids)
```

### 3. Model Serialization

```python
# Save for deployment
torch.save({
    'model_state_dict': model.state_dict(),
    'config': config,
}, 'production_model.pth')

# Load in production
checkpoint = torch.load('production_model.pth')
model.load_state_dict(checkpoint['model_state_dict'])
model.eval()
```

---

## 📊 Performance Characteristics

### Sequence Length vs Speed

| Seq Length | Processing Time | Memory Usage | Best Use Case |
|------------|-----------------|--------------|---------------|
| 512 | 37ms | ~2GB | Chat, Q&A |
| 2,048 | 134ms | ~4GB | Articles, Code |
| 8,192 | 568ms | ~8GB | Long docs, Books |
| 16,384 | 1,576ms | ~16GB | Research papers |
| 131,072 | ~8s | ~32GB | Full documents |

### Model Size vs Parameters

| Size | Parameters | Memory | Inference Speed | Training Speed |
|------|------------|--------|-----------------|----------------|
| Small | 454M | ~2GB | Fast | Very Fast |
| **Base** | **983M** | **~4GB** | **Balanced** | **Fast** |
| Medium | 3.3B | ~13GB | Good | Medium |
| Large | 9.8B | ~40GB | Slower | Slow |

**Recommendation for MVP:** Start with `base` (983M parameters)

---

## 🐛 Common Issues & Solutions

### Issue: Import Error
```bash
# Solution: Reinstall
pip install --force-reinstall resonance_nn-0.1.0-py3-none-any.whl
```

### Issue: CUDA Out of Memory
```python
# Solution 1: Use smaller batch size
batch_size = 4  # instead of 32

# Solution 2: Use smaller model
model = create_spectral_lm('small', vocab_size=50257)

# Solution 3: Enable gradient checkpointing
config.use_gradient_checkpointing = True
```

### Issue: Slow Inference
```python
# Solution 1: Ensure model is in eval mode
model.eval()

# Solution 2: Use torch.no_grad()
with torch.no_grad():
    output = model(input_ids)

# Solution 3: Move to GPU
model = model.cuda()
input_ids = input_ids.cuda()
```

---

## 📞 Support & Resources

- **Installation Guide:** See `INSTALLATION_GUIDE.md`
- **Full Documentation:** See `README.md`
- **Code Examples:** Check `examples/` directory
- **GitHub Issues:** https://github.com/tafolabi009/RNN/issues
- **Email Support:** afolabi@genovotech.com

---

## ✅ Pre-Flight Checklist

Before starting your MVP development:

- [ ] Install package: `pip install resonance_nn-0.1.0-py3-none-any.whl`
- [ ] Run verification: `python verify_installation.py`
- [ ] Test basic import: `import resonance_nn`
- [ ] Create test model: `model = create_spectral_lm('small', vocab_size=1000)`
- [ ] Run forward pass with dummy data
- [ ] Check GPU availability: `torch.cuda.is_available()`
- [ ] Review `INSTALLATION_GUIDE.md` for usage examples
- [ ] Plan model size based on your hardware
- [ ] Set up monitoring/logging for your application

---

## 🎯 MVP Development Workflow

1. **Start Small**: Prototype with `small` model (454M params)
2. **Test Fast**: Use small datasets to validate logic
3. **Scale Up**: Move to `base` (983M) when ready
4. **Optimize**: Profile and optimize bottlenecks
5. **Deploy**: Package model with your application
6. **Monitor**: Track inference times and memory usage

---

## 📄 License

MIT License - Free for commercial use

---

## 🚀 Ready to Build?

```python
# Your MVP starts here
from resonance_nn import create_spectral_lm

model = create_spectral_lm('base', vocab_size=50257)
print("🚀 Ready to revolutionize your application!")
```

---

**Built with ❤️ and FFTs | O(n log n) > O(n²)**

*For questions or support, contact: afolabi@genovotech.com*
