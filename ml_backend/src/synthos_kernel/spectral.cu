/**
 * @file spectral.cu
 * @brief SynthOS Fused Spectral Entropy Kernel - Main Implementation
 * 
 * This file implements the C ABI interface for the SynthOS spectral
 * entropy computation library. It handles:
 *   - Runtime GPU architecture detection
 *   - cuFFT plan management
 *   - Kernel dispatch based on compute capability
 *   - Error handling and resource management
 * 
 * Build Command (example):
 *   nvcc -arch=sm_60 -gencode arch=compute_60,code=sm_60 \
 *        -gencode arch=compute_70,code=sm_70 \
 *        -gencode arch=compute_75,code=sm_75 \
 *        -gencode arch=compute_80,code=sm_80 \
 *        -gencode arch=compute_86,code=sm_86 \
 *        -gencode arch=compute_89,code=sm_89 \
 *        -gencode arch=compute_90,code=sm_90 \
 *        -gencode arch=compute_90,code=compute_90 \
 *        -shared -Xcompiler -fPIC -o libsynthos.so spectral.cu -lcufft
 * 
 * @author SynthOS ML Backend Team
 * @version 1.0.0
 */

#include "spectral.h"
#include "arch_dispatch.cuh"
#include "kernels/spectral_sm60.cuh"
#include "kernels/spectral_sm70.cuh"
#include "kernels/spectral_sm80.cuh"
#include "kernels/spectral_sm90.cuh"

#include <cuda_runtime.h>
#include <cufft.h>
#include <cstdio>
#include <cstring>
#include <mutex>

/* ============================================================================
 * Global State (Thread-Safe)
 * ============================================================================ */

namespace {

/**
 * @brief Library state
 */
struct SynthosState {
    bool initialized;
    int device_id;
    int compute_capability;
    SynthosArch arch;
    char arch_name[64];
    SynthosDeviceInfo device_info;
    
    /* cuFFT plan cache */
    /* Key: (n_samples << 16) | n_channels */
    /* We cache plans for common FFT sizes */
    static constexpr int MAX_CACHED_PLANS = 16;
    struct CachedPlan {
        cufftHandle plan;
        int n_samples;
        int n_channels;
        bool valid;
    };
    CachedPlan plan_cache[MAX_CACHED_PLANS];
    int plan_cache_count;
    
    /* Workspace memory (pre-allocated for common sizes) */
    void* d_fft_workspace;
    size_t fft_workspace_size;
    void* d_psd_buffer;
    size_t psd_buffer_size;
    
    std::mutex mutex;  /* Protect plan cache and initialization */
};

static SynthosState g_state = {false, -1, 0, SYNTHOS_ARCH_UNKNOWN, "", {}, {}, 0, nullptr, 0, nullptr, 0, {}};

/**
 * @brief Version string with built architectures
 */
static const char* g_version_string = 
    SYNTHOS_VERSION_STRING " (built for sm_60,sm_70,sm_75,sm_80,sm_86,sm_89,sm_90+PTX)";

/**
 * @brief Error strings
 */
static const char* error_strings[] = {
    "Success",
    "Invalid device ID",
    "Unsupported architecture (minimum sm_60 required)",
    "Invalid dimensions",
    "FFT size must be power of 2",
    "FFT size too large (maximum 8192)",
    "Null pointer argument",
    "cuFFT operation failed",
    "Library not initialized",
    "Library already initialized",
    "Workspace too small",
    "CUDA runtime error"
};

}  /* anonymous namespace */

/* ============================================================================
 * cuFFT Plan Management
 * ============================================================================ */

namespace {

/**
 * @brief Find or create a cuFFT plan for given dimensions
 */
cufftResult get_or_create_plan(
    cufftHandle* plan,
    int n_samples,
    int n_channels
) {
    /* Search cache */
    for (int i = 0; i < g_state.plan_cache_count; i++) {
        if (g_state.plan_cache[i].valid &&
            g_state.plan_cache[i].n_samples == n_samples &&
            g_state.plan_cache[i].n_channels == n_channels) {
            *plan = g_state.plan_cache[i].plan;
            return CUFFT_SUCCESS;
        }
    }
    
    /* Create new plan */
    cufftHandle new_plan;
    cufftResult result;
    
    /* Use batched 1D real-to-complex FFT */
    int rank = 1;
    int n[] = {n_samples};
    int inembed[] = {n_samples};
    int onembed[] = {n_samples / 2 + 1};
    int istride = 1;
    int ostride = 1;
    int idist = n_samples;
    int odist = n_samples / 2 + 1;
    
    result = cufftPlanMany(
        &new_plan,
        rank,
        n,
        inembed, istride, idist,  /* Input layout */
        onembed, ostride, odist,  /* Output layout */
        CUFFT_R2C,
        n_channels
    );
    
    if (result != CUFFT_SUCCESS) {
        return result;
    }
    
    /* Cache the plan */
    if (g_state.plan_cache_count < SynthosState::MAX_CACHED_PLANS) {
        g_state.plan_cache[g_state.plan_cache_count].plan = new_plan;
        g_state.plan_cache[g_state.plan_cache_count].n_samples = n_samples;
        g_state.plan_cache[g_state.plan_cache_count].n_channels = n_channels;
        g_state.plan_cache[g_state.plan_cache_count].valid = true;
        g_state.plan_cache_count++;
    }
    
    *plan = new_plan;
    return CUFFT_SUCCESS;
}

}  /* anonymous namespace */

/* ============================================================================
 * Fused PSD Computation Kernel
 * ============================================================================ */

/**
 * @brief Compute |FFT|² (power spectral density) in-place
 * 
 * Takes complex FFT output and computes magnitude squared.
 * This is a simple element-wise operation, architecture-agnostic.
 */
__global__ void compute_psd_kernel(
    const cufftComplex* __restrict__ d_fft,
    float* __restrict__ d_psd,
    int n_freq,
    int n_channels
) {
    const int idx = blockIdx.x * blockDim.x + threadIdx.x;
    const int total = n_freq * n_channels;
    
    if (idx < total) {
        cufftComplex c = d_fft[idx];
        d_psd[idx] = c.x * c.x + c.y * c.y;
    }
}

/**
 * @brief Launch PSD computation kernel
 */
inline cudaError_t launch_psd_kernel(
    const cufftComplex* d_fft,
    float* d_psd,
    int n_freq,
    int n_channels,
    cudaStream_t stream
) {
    int total = n_freq * n_channels;
    int block_size = 256;
    int grid_size = (total + block_size - 1) / block_size;
    
    compute_psd_kernel<<<grid_size, block_size, 0, stream>>>(
        d_fft, d_psd, n_freq, n_channels
    );
    
    return cudaGetLastError();
}

/* ============================================================================
 * Architecture-Specific Kernel Dispatch
 * ============================================================================ */

namespace {

/**
 * @brief Dispatch entropy kernel based on architecture
 */
cudaError_t dispatch_entropy_kernel(
    const float* d_psd,
    float* d_entropy,
    int n_freq,
    int n_channels,
    cudaStream_t stream
) {
    switch (g_state.arch) {
        case SYNTHOS_ARCH_HOPPER:
            return synthos::sm90::launch_spectral_entropy_sm90(
                d_psd, d_entropy, n_freq, n_channels, stream
            );
        
        case SYNTHOS_ARCH_ADA:
        case SYNTHOS_ARCH_AMPERE_RTX:
        case SYNTHOS_ARCH_AMPERE:
            return synthos::sm80::launch_spectral_entropy_sm80(
                d_psd, d_entropy, n_freq, n_channels, stream
            );
        
        case SYNTHOS_ARCH_TURING:
        case SYNTHOS_ARCH_VOLTA:
            return synthos::sm70::launch_spectral_entropy_sm70(
                d_psd, d_entropy, n_freq, n_channels, stream
            );
        
        case SYNTHOS_ARCH_PASCAL:
        default:
            return synthos::sm60::launch_spectral_entropy_sm60(
                d_psd, d_entropy, n_freq, n_channels, stream
            );
    }
}

}  /* anonymous namespace */

/* ============================================================================
 * Public API Implementation
 * ============================================================================ */

extern "C" {

cudaError_t synthos_init(int device_id) {
    std::lock_guard<std::mutex> lock(g_state.mutex);
    
    if (g_state.initialized) {
        /* Allow re-initialization on same device */
        if (g_state.device_id == device_id) {
            return cudaSuccess;
        }
        /* Different device requires cleanup first */
        return (cudaError_t)SYNTHOS_ERROR_ALREADY_INITIALIZED;
    }
    
    /* Set device */
    cudaError_t err = cudaSetDevice(device_id);
    if (err != cudaSuccess) {
        return err;
    }
    
    /* Get device properties */
    cudaDeviceProp props;
    err = cudaGetDeviceProperties(&props, device_id);
    if (err != cudaSuccess) {
        return err;
    }
    
    /* Check compute capability */
    int cc = props.major * 10 + props.minor;
    if (cc < SYNTHOS_MIN_COMPUTE_CAPABILITY) {
        fprintf(stderr, "SynthOS Error: GPU compute capability %d.%d is below minimum %d.0\n",
                props.major, props.minor, SYNTHOS_MIN_COMPUTE_CAPABILITY / 10);
        return (cudaError_t)SYNTHOS_ERROR_UNSUPPORTED_ARCH;
    }
    
    /* Initialize state */
    g_state.device_id = device_id;
    g_state.compute_capability = cc;
    g_state.arch = compute_capability_to_arch(cc);
    
    /* Create architecture name string */
    snprintf(g_state.arch_name, sizeof(g_state.arch_name),
             "%s", arch_to_string(g_state.arch, cc));
    
    /* Populate device info */
    g_state.device_info.device_id = device_id;
    g_state.device_info.compute_capability_major = props.major;
    g_state.device_info.compute_capability_minor = props.minor;
    g_state.device_info.arch = g_state.arch;
    g_state.device_info.arch_name = g_state.arch_name;
    g_state.device_info.global_memory_bytes = props.totalGlobalMem;
    g_state.device_info.multiprocessor_count = props.multiProcessorCount;
    g_state.device_info.max_threads_per_block = props.maxThreadsPerBlock;
    g_state.device_info.warp_size = props.warpSize;
    g_state.device_info.shared_memory_per_block = props.sharedMemPerBlock;
    g_state.device_info.max_grid_dim_x = props.maxGridSize[0];
    g_state.device_info.expected_speedup_vs_pytorch = get_expected_speedup(g_state.arch);
    
    /* Pre-allocate workspace for common sizes */
    /* PSD buffer: max 8192 samples × 256 channels × sizeof(float) */
    g_state.psd_buffer_size = SYNTHOS_MAX_FFT_SIZE * 256 * sizeof(float);
    err = cudaMalloc(&g_state.d_psd_buffer, g_state.psd_buffer_size);
    if (err != cudaSuccess) {
        /* Non-fatal: will allocate on-demand */
        g_state.d_psd_buffer = nullptr;
        g_state.psd_buffer_size = 0;
    }
    
    /* Initialize plan cache */
    g_state.plan_cache_count = 0;
    for (int i = 0; i < SynthosState::MAX_CACHED_PLANS; i++) {
        g_state.plan_cache[i].valid = false;
    }
    
    g_state.initialized = true;
    
    fprintf(stderr, "SynthOS: Initialized for %s (expected speedup: %.1fx)\n",
            g_state.arch_name, g_state.device_info.expected_speedup_vs_pytorch);
    
    return cudaSuccess;
}

cudaError_t synthos_fused_spectral_entropy(
    const float* d_input,
    float* d_entropy,
    int n_samples,
    int n_channels,
    cudaStream_t stream
) {
    return synthos_fused_spectral_entropy_ex(
        d_input,
        d_entropy,
        nullptr,
        0,
        n_samples,
        n_channels,
        stream
    );
}

cudaError_t synthos_fused_spectral_entropy_ex(
    const float* d_input,
    float* d_entropy,
    void* d_workspace,
    size_t workspace_bytes,
    int n_samples,
    int n_channels,
    cudaStream_t stream
) {
    /* Validation */
    if (!g_state.initialized) {
        return (cudaError_t)SYNTHOS_ERROR_NOT_INITIALIZED;
    }
    
    if (d_input == nullptr || d_entropy == nullptr) {
        return (cudaError_t)SYNTHOS_ERROR_NULL_POINTER;
    }
    
    if (n_samples <= 0 || n_channels <= 0) {
        return (cudaError_t)SYNTHOS_ERROR_INVALID_DIMENSIONS;
    }
    
    if (!is_power_of_2(n_samples)) {
        return (cudaError_t)SYNTHOS_ERROR_FFT_SIZE_NOT_POWER_OF_2;
    }
    
    if (n_samples > SYNTHOS_MAX_FFT_SIZE) {
        return (cudaError_t)SYNTHOS_ERROR_FFT_SIZE_TOO_LARGE;
    }

    int n_freq = n_samples / 2 + 1;
    size_t fft_workspace_bytes = sizeof(cufftComplex) * static_cast<size_t>(n_freq) * static_cast<size_t>(n_channels);
    size_t reduction_workspace_bytes = sizeof(float) * static_cast<size_t>(n_freq) * static_cast<size_t>(n_channels);
    size_t required_workspace_bytes = fft_workspace_bytes + reduction_workspace_bytes;

    bool using_external_workspace = d_workspace != nullptr;
    cufftComplex* d_fft = nullptr;
    float* d_psd = nullptr;

    if (using_external_workspace) {
        if (workspace_bytes < required_workspace_bytes) {
            return (cudaError_t)SYNTHOS_ERROR_WORKSPACE_TOO_SMALL;
        }

        char* workspace_bytes_ptr = static_cast<char*>(d_workspace);
        d_fft = reinterpret_cast<cufftComplex*>(workspace_bytes_ptr);
        d_psd = reinterpret_cast<float*>(workspace_bytes_ptr + fft_workspace_bytes);
    }
    
    /* Set stream for cuFFT if provided */
    cudaStream_t work_stream = stream ? stream : 0;
    
    /* Get or create cuFFT plan */
    cufftHandle plan;
    cufftResult cufft_result;
    
    {
        std::lock_guard<std::mutex> lock(g_state.mutex);
        cufft_result = get_or_create_plan(&plan, n_samples, n_channels);
    }
    
    if (cufft_result != CUFFT_SUCCESS) {
        fprintf(stderr, "SynthOS Error: cuFFT plan creation failed (error %d)\n", cufft_result);
        return (cudaError_t)SYNTHOS_ERROR_CUFFT_FAILED;
    }
    
    /* Set stream for cuFFT */
    cufftSetStream(plan, work_stream);
    
    cudaError_t err;

    if (!using_external_workspace) {
        /* Allocate temporary buffers */
        err = cudaMalloc(&d_fft, fft_workspace_bytes);
        if (err != cudaSuccess) {
            return err;
        }

        err = cudaMalloc(&d_psd, reduction_workspace_bytes);
        if (err != cudaSuccess) {
            cudaFree(d_fft);
            return err;
        }
    }
    
    /* ==================== Step 1: Batch FFT ==================== */
    /* Real-to-Complex FFT */
    cufft_result = cufftExecR2C(plan, (cufftReal*)d_input, d_fft);
    if (cufft_result != CUFFT_SUCCESS) {
        cudaFree(d_fft);
        cudaFree(d_psd);
        return (cudaError_t)SYNTHOS_ERROR_CUFFT_FAILED;
    }
    
    /* ==================== Step 2: Compute PSD ==================== */
    err = launch_psd_kernel(d_fft, d_psd, n_freq, n_channels, work_stream);
    if (err != cudaSuccess) {
        cudaFree(d_fft);
        cudaFree(d_psd);
        return err;
    }
    
    /* ==================== Step 3: Compute Entropy ==================== */
    /* Architecture-specific dispatch */
    err = dispatch_entropy_kernel(d_psd, d_entropy, n_freq, n_channels, work_stream);
    if (err != cudaSuccess) {
        if (!using_external_workspace) {
            cudaFree(d_fft);
            cudaFree(d_psd);
        }
        return err;
    }
    
    /* Cleanup temporary buffers */
    if (!using_external_workspace) {
        cudaFree(d_fft);
        cudaFree(d_psd);
    }
    
    return cudaSuccess;
}

cudaError_t synthos_get_workspace_size(
    int n_samples,
    int n_channels,
    SynthosWorkspaceInfo* info
) {
    if (info == nullptr) {
        return (cudaError_t)SYNTHOS_ERROR_NULL_POINTER;
    }
    
    int n_freq = n_samples / 2 + 1;
    
    /* FFT workspace: complex output */
    info->fft_workspace_bytes = sizeof(cufftComplex) * n_freq * n_channels;
    
    /* Reduction workspace: PSD buffer */
    info->reduction_workspace_bytes = sizeof(float) * n_freq * n_channels;
    
    /* Total with alignment */
    info->total_bytes = info->fft_workspace_bytes + info->reduction_workspace_bytes;
    info->total_bytes = (info->total_bytes + 255) & ~255;  /* 256-byte aligned */
    
    return cudaSuccess;
}

const char* synthos_get_active_arch(void) {
    if (!g_state.initialized) {
        return "Not initialized";
    }
    return g_state.arch_name;
}

cudaError_t synthos_get_device_info(SynthosDeviceInfo* info) {
    if (!g_state.initialized) {
        return (cudaError_t)SYNTHOS_ERROR_NOT_INITIALIZED;
    }
    
    if (info == nullptr) {
        return (cudaError_t)SYNTHOS_ERROR_NULL_POINTER;
    }
    
    memcpy(info, &g_state.device_info, sizeof(SynthosDeviceInfo));
    return cudaSuccess;
}

const char* synthos_get_error_string(SynthosError error) {
    if (error >= 0 && error < sizeof(error_strings) / sizeof(error_strings[0])) {
        return error_strings[error];
    }
    if (error >= 1001 && error <= 1011) {
        return error_strings[error - 1000];
    }
    return "Unknown error";
}

int synthos_is_device_supported(int device_id) {
    int cc = get_compute_capability(device_id);
    return cc >= SYNTHOS_MIN_COMPUTE_CAPABILITY ? 1 : 0;
}

cudaError_t synthos_cleanup(void) {
    std::lock_guard<std::mutex> lock(g_state.mutex);
    
    if (!g_state.initialized) {
        return cudaSuccess;  /* Already clean */
    }
    
    /* Destroy cached cuFFT plans */
    for (int i = 0; i < g_state.plan_cache_count; i++) {
        if (g_state.plan_cache[i].valid) {
            cufftDestroy(g_state.plan_cache[i].plan);
            g_state.plan_cache[i].valid = false;
        }
    }
    g_state.plan_cache_count = 0;
    
    /* Free pre-allocated buffers */
    if (g_state.d_psd_buffer) {
        cudaFree(g_state.d_psd_buffer);
        g_state.d_psd_buffer = nullptr;
    }
    
    if (g_state.d_fft_workspace) {
        cudaFree(g_state.d_fft_workspace);
        g_state.d_fft_workspace = nullptr;
    }
    
    g_state.initialized = false;
    g_state.device_id = -1;
    
    fprintf(stderr, "SynthOS: Cleanup complete\n");
    
    return cudaSuccess;
}

cudaError_t synthos_synchronize(void) {
    if (!g_state.initialized) {
        return (cudaError_t)SYNTHOS_ERROR_NOT_INITIALIZED;
    }
    return cudaDeviceSynchronize();
}

const char* synthos_get_version(void) {
    return g_version_string;
}

}  /* extern "C" */
