# ==============================================================================
# SynthOS Spectral Entropy Kernel - CMake Build System
# ==============================================================================
#
# Multi-architecture CUDA build with fat binary support for:
#   sm_60 (Pascal), sm_70 (Volta), sm_75 (Turing), sm_80/86 (Ampere),
#   sm_89 (Ada), sm_90 (Hopper) + PTX for future JIT
#
# Usage:
#   mkdir build && cd build
#   cmake .. -DCMAKE_BUILD_TYPE=Release
#   cmake --build . --parallel
#
# Options:
#   -DSYNTHOS_CUDA_ARCH=auto        Auto-detect GPU + common architectures
#   -DSYNTHOS_CUDA_ARCH=native      Build only for detected GPU
#   -DSYNTHOS_CUDA_ARCH=all         Build for all supported architectures
#   -DSYNTHOS_INSTALL_PYTHON=ON     Install to Python site-packages
#   -DSYNTHOS_ENABLE_PROFILING=ON   Enable nvprof/nsys annotations
#
# ==============================================================================

cmake_minimum_required(VERSION 3.18 FATAL_ERROR)

# Project definition
project(synthos_kernel
    VERSION 1.0.0
    DESCRIPTION "SynthOS Fused Spectral Entropy CUDA Kernel"
    LANGUAGES CXX CUDA
)

include(CTest)

# ==============================================================================
# Build Options
# ==============================================================================

option(SYNTHOS_INSTALL_PYTHON "Install to Python site-packages" OFF)
option(SYNTHOS_ENABLE_PROFILING "Enable profiling annotations (nvtx)" OFF)
option(SYNTHOS_BUILD_TESTS "Build test executables" ON)
option(SYNTHOS_VERBOSE_BUILD "Verbose compilation output" OFF)

set(SYNTHOS_CUDA_ARCH "all" CACHE STRING 
    "CUDA architectures: auto, native, all, or comma-separated list (60,70,80)")

# ==============================================================================
# CUDA Configuration
# ==============================================================================

# Require CUDA 11.0+
find_package(CUDAToolkit 11.0 REQUIRED)

# Verbose output
if(SYNTHOS_VERBOSE_BUILD)
    set(CMAKE_VERBOSE_MAKEFILE ON)
endif()

# ==============================================================================
# Architecture Detection and Configuration
# ==============================================================================

# Full list of supported architectures
set(SYNTHOS_ALL_ARCHS 
    60   # Pascal (GTX 10-series)
    70   # Volta (V100)
    75   # Turing (RTX 20-series)
    80   # Ampere (A100)
    86   # Ampere (RTX 30-series)
    89   # Ada Lovelace (RTX 40-series)
    90   # Hopper (H100)
)

# Determine which architectures to build
if(SYNTHOS_CUDA_ARCH STREQUAL "all")
    set(CUDA_TARGET_ARCHS ${SYNTHOS_ALL_ARCHS})
    message(STATUS "SynthOS: Building for ALL architectures (fat binary)")
    
elseif(SYNTHOS_CUDA_ARCH STREQUAL "auto" OR SYNTHOS_CUDA_ARCH STREQUAL "native")
    # Detect local GPU
    include(FindCUDA/select_compute_arch)
    CUDA_DETECT_INSTALLED_GPUS(DETECTED_GPUS)
    
    if(DETECTED_GPUS)
        # Extract compute capability
        string(REGEX MATCH "[0-9]+" DETECTED_CC "${DETECTED_GPUS}")
        message(STATUS "SynthOS: Detected GPU with compute capability: ${DETECTED_CC}")
        
        if(SYNTHOS_CUDA_ARCH STREQUAL "native")
            set(CUDA_TARGET_ARCHS ${DETECTED_CC})
        else()
            # Auto: detected + common data center GPUs
            set(CUDA_TARGET_ARCHS ${DETECTED_CC} 70 80 90)
            list(REMOVE_DUPLICATES CUDA_TARGET_ARCHS)
        endif()
    else()
        message(WARNING "SynthOS: No GPU detected, building for all architectures")
        set(CUDA_TARGET_ARCHS ${SYNTHOS_ALL_ARCHS})
    endif()
    
else()
    # Parse user-provided architecture list
    string(REPLACE "," ";" CUDA_TARGET_ARCHS "${SYNTHOS_CUDA_ARCH}")
    message(STATUS "SynthOS: Building for user-specified architectures: ${CUDA_TARGET_ARCHS}")
endif()

# Validate architectures
foreach(ARCH ${CUDA_TARGET_ARCHS})
    if(ARCH LESS 60)
        message(FATAL_ERROR "SynthOS requires compute capability >= 6.0, got ${ARCH}")
    endif()
endforeach()

# Configure CMAKE_CUDA_ARCHITECTURES
# Each entry creates both SASS and embedded PTX
set(CMAKE_CUDA_ARCHITECTURES "")
foreach(ARCH ${CUDA_TARGET_ARCHS})
    list(APPEND CMAKE_CUDA_ARCHITECTURES ${ARCH})
endforeach()

# Add PTX for highest architecture (for JIT on future GPUs)
list(GET CUDA_TARGET_ARCHS -1 HIGHEST_ARCH)
list(APPEND CMAKE_CUDA_ARCHITECTURES ${HIGHEST_ARCH}-virtual)

message(STATUS "SynthOS: CMAKE_CUDA_ARCHITECTURES = ${CMAKE_CUDA_ARCHITECTURES}")

# ==============================================================================
# Compiler Flags
# ==============================================================================

# C++ Standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CUDA_STANDARD 17)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)

# Position Independent Code (required for shared library)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# CUDA flags
set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} --expt-extended-lambda")
set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} --expt-relaxed-constexpr")
set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Xcompiler -Wall")

# Release optimizations
set(CMAKE_CUDA_FLAGS_RELEASE "-O3 -DNDEBUG -use_fast_math")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

# Debug with device info
set(CMAKE_CUDA_FLAGS_DEBUG "-G -g -O0 -DDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DDEBUG")

# Profiling annotations
if(SYNTHOS_ENABLE_PROFILING)
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -lineinfo")
    add_compile_definitions(SYNTHOS_ENABLE_NVTX)
endif()

# ==============================================================================
# Library Target: libsynthos.so
# ==============================================================================

add_library(synthos SHARED
    spectral.cu
)

target_include_directories(synthos PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(synthos PRIVATE
    CUDA::cudart
    CUDA::cufft
)

# Set output name and properties
set_target_properties(synthos PROPERTIES
    OUTPUT_NAME "synthos"
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
    CUDA_SEPARABLE_COMPILATION ON
    CUDA_RESOLVE_DEVICE_SYMBOLS ON
)

# ==============================================================================
# Static Library Target (optional)
# ==============================================================================

add_library(synthos_static STATIC
    spectral.cu
)

target_include_directories(synthos_static PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(synthos_static PRIVATE
    CUDA::cudart_static
    CUDA::cufft_static
)

set_target_properties(synthos_static PROPERTIES
    OUTPUT_NAME "synthos_static"
    CUDA_SEPARABLE_COMPILATION ON
    CUDA_RESOLVE_DEVICE_SYMBOLS ON
)

# ==============================================================================
# Test Executable
# ==============================================================================

if(SYNTHOS_BUILD_TESTS)
    find_package(Python3 COMPONENTS Interpreter REQUIRED)

    add_test(
        NAME synthos_spectral_python_tests
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_spectral.py --lib $<TARGET_FILE:synthos>
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()

# ==============================================================================
# Installation
# ==============================================================================

include(GNUInstallDirs)

# Install library
install(TARGETS synthos synthos_static
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

# Install headers
install(FILES
    spectral.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/synthos
)

# Python site-packages installation
if(SYNTHOS_INSTALL_PYTHON)
    # Detect Python and site-packages location
    find_package(Python3 COMPONENTS Interpreter REQUIRED)
    
    execute_process(
        COMMAND ${Python3_EXECUTABLE} -c 
            "import sysconfig; print(sysconfig.get_paths()['purelib'])"
        OUTPUT_VARIABLE PYTHON_SITE_PACKAGES
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    
    message(STATUS "SynthOS: Python site-packages: ${PYTHON_SITE_PACKAGES}")
    
    # Install to site-packages/synthos_kernel/
    install(TARGETS synthos
        LIBRARY DESTINATION ${PYTHON_SITE_PACKAGES}/synthos_kernel
    )
    
    install(FILES
        python_wrapper.py
        DESTINATION ${PYTHON_SITE_PACKAGES}/synthos_kernel
        RENAME __init__.py
    )
endif()

# ==============================================================================
# Build Summary
# ==============================================================================

message(STATUS "")
message(STATUS "=============== SynthOS Build Configuration ===============")
message(STATUS "  Version:              ${PROJECT_VERSION}")
message(STATUS "  Build type:           ${CMAKE_BUILD_TYPE}")
message(STATUS "  CUDA Toolkit:         ${CUDAToolkit_VERSION}")
message(STATUS "  Target architectures: ${CMAKE_CUDA_ARCHITECTURES}")
message(STATUS "  Install prefix:       ${CMAKE_INSTALL_PREFIX}")
message(STATUS "  Python install:       ${SYNTHOS_INSTALL_PYTHON}")
message(STATUS "  Profiling enabled:    ${SYNTHOS_ENABLE_PROFILING}")
message(STATUS "============================================================")
message(STATUS "")

# Architecture coverage report
message(STATUS "Architecture Coverage:")
foreach(ARCH ${CUDA_TARGET_ARCHS})
    if(ARCH EQUAL 60)
        message(STATUS "  sm_${ARCH}: Pascal (GTX 10-series) - BASELINE")
    elseif(ARCH EQUAL 70)
        message(STATUS "  sm_${ARCH}: Volta (V100) - Cooperative Groups")
    elseif(ARCH EQUAL 75)
        message(STATUS "  sm_${ARCH}: Turing (RTX 20-series)")
    elseif(ARCH EQUAL 80)
        message(STATUS "  sm_${ARCH}: Ampere (A100) - Async Copy")
    elseif(ARCH EQUAL 86)
        message(STATUS "  sm_${ARCH}: Ampere (RTX 30-series)")
    elseif(ARCH EQUAL 89)
        message(STATUS "  sm_${ARCH}: Ada Lovelace (RTX 40-series)")
    elseif(ARCH EQUAL 90)
        message(STATUS "  sm_${ARCH}: Hopper (H100) - Thread Block Clusters")
    endif()
endforeach()
message(STATUS "  PTX:    Embedded for JIT on future GPUs")
message(STATUS "")
