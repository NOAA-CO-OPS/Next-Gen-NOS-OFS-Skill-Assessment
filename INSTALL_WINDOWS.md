# Installing ofs-skill on Windows

This guide provides OS-agnostic installation instructions with special focus on Windows.

## ⚠️ Important: pyinterp is Required for 2D Functionality

**pyinterp is REQUIRED for:**
- ❗ `create_2dplot` command
- ❗ Any 2D plotting or visualization
- ❗ 2D field data processing
- ❗ Spatial grid interpolation

**If you need 2D functionality, you MUST install pyinterp.** Conda is the recommended approach.

---

## Quick Start (Recommended - Full Installation with 2D Support)

**Using Conda with environment.yml (Easiest & Most Reliable):**

```bash
# Windows (using Anaconda/Miniconda)
conda env create -n ofs_skill -f environment.yml
conda activate ofs_skill
pip install -e .
```

This installs ALL dependencies including pyinterp, optimized for Python 3.11. ✅

**If this works, you're done!** Skip to [Verifying Installation](#verifying-installation).

---

## Option 1: Use Conda (Recommended - Includes 2D)

**This is the most reliable approach for Windows users needing full functionality.**

Conda provides pre-compiled binaries with all C++ dependencies, avoiding compilation issues.

### Method A: Using environment.yml (Recommended)

```bash
# Windows (using Anaconda/Miniconda)
conda env create -n ofs_skill -f environment.yml
conda activate ofs_skill
pip install -e .
```

**Advantages:**
- ✅ Installs all dependencies at once
- ✅ Versions tested and known to work together
- ✅ Optimized for Python 3.11
- ✅ Includes pyinterp with all C++ dependencies
- ✅ **Enables full 2D functionality including `create_2dplot`**

### Method B: Manual conda installation

```bash
# Windows (using Anaconda/Miniconda)
conda create -n ofs_skill python=3.11
conda activate ofs_skill
conda install -c conda-forge pyinterp
pip install -e .
```

**Why conda works:**
- ✅ Conda handles C++ compilation (Boost, Eigen, CMake)
- ✅ Works reliably on Windows, Linux, macOS
- ✅ Pre-compiled binaries (fast installation - no 25-minute build!)

---

## Option 2: Use pip + venv (Limited - 1D Only on Windows)

⚠️ **WARNING: On Windows, this typically CANNOT install pyinterp, so 2D functionality will NOT work.**

This option is only suitable if you ONLY need 1D station-based analysis.

### Try with pyinterp (may fail on Windows):
```bash
# Windows
python -m venv venv
venv\Scripts\activate
pip install --upgrade pip
pip install -e .[spatial]  # Includes pyinterp - likely to fail on Windows
```

### Without pyinterp (1D only):
```bash
# Windows
python -m venv venv
venv\Scripts\activate
pip install --upgrade pip
pip install -e .  # No [spatial] - skips pyinterp
```

**What works without pyinterp:**
- ✅ Model data processing
- ✅ Observation retrieval (stations)
- ✅ Skill assessment (1D metrics)
- ✅ 1D station-based plots (`create_1dplot`)
- ✅ Database operations

**What DOES NOT work without pyinterp:**
- ❌ **2D visualizations** (`create_2dplot`)
- ❌ **2D field data plotting**
- ❌ **Spatial grid interpolation**
- ❌ **Field-based skill assessment**

---

## Option 3: Compile pyinterp on Windows (Advanced)

⚠️ **Not Recommended** - Only attempt this if you need 2D functionality but cannot use conda.

This requires significant C++ build tooling and can take 25+ minutes:

### Prerequisites
1. **Install Visual Studio Build Tools** (C++ compiler)
   - Download: https://visualstudio.microsoft.com/downloads/
   - Select "Desktop development with C++"

2. **Install CMake**
   ```bash
   pip install cmake
   ```

3. **Install Boost** (hardest part)
   - Download from: https://sourceforge.net/projects/boost/files/boost-binaries/
   - Install to: `C:\local\boost_1_XX_X`
   - Set environment variable:
     ```bash
     setx BOOST_ROOT "C:\local\boost_1_XX_X"
     ```

4. **Install Eigen3**
   - Download: https://gitlab.com/libeigen/eigen/-/releases
   - Extract to: `C:\local\eigen-3.X.X`
   - Set environment variable:
     ```bash
     setx EIGEN3_ROOT "C:\local\eigen-3.X.X"
     ```

5. **Try installing**
   ```bash
   pip install pyinterp
   ```

**Warning:** This is complex. Use Option 2 (conda) instead if possible.

---

## Recommended Approach by Use Case

### For Full Functionality (2D + 1D) - Recommended
```bash
# All Platforms - Use conda with environment.yml
conda env create -n ofs_skill -f environment.yml
conda activate ofs_skill
pip install -e .
```
**Best for:** Complete installation with all features including `create_2dplot`

### For Development (Full Features)
```bash
# All Platforms - Use conda with environment.yml
conda env create -n ofs_skill -f environment.yml
conda activate ofs_skill
pip install -e .[dev]  # Includes testing tools
```

### For 1D Station Analysis Only (If You Don't Need 2D)
```bash
# Windows/Linux/macOS
python -m venv venv
venv\Scripts\activate  # Linux/macOS: source venv/bin/activate
pip install -e .
```
**Works for:** Station observations, 1D plots, basic skill assessment (no `create_2dplot`)

### For Production Deployment
```bash
# All Platforms - Recommended
conda env create -n ofs_skill -f environment.yml
conda activate ofs_skill
pip install ofs-skill

# Alternative: pip only (Linux/macOS, 2D may work)
pip install ofs-skill[spatial]
```

---

## Verifying Installation

```python
import ofs_skill
print(f"ofs_skill version: {ofs_skill.__version__}")

# Test basic functionality
from ofs_skill.model_processing import ModelProperties
props = ModelProperties('cbofs')
print(f"✓ Model processing works - Model: {props.model}")

# Test if pyinterp is available (REQUIRED for 2D functionality)
try:
    import pyinterp
    print("✓ pyinterp available - 2D functionality enabled")
    print("  Can use: create_2dplot, 2D visualizations")
except ImportError:
    print("✗ pyinterp NOT available")
    print("  WARNING: Cannot use create_2dplot or 2D visualizations")
    print("  Only 1D functionality (create_1dplot) will work")
```

---

## Troubleshooting

### "Microsoft Visual C++ 14.0 or greater is required"
Install Visual Studio Build Tools (see Option 3 above)

### "Could NOT find Boost"
Use conda (Option 2) or manually install Boost (Option 3)

### "No module named 'pyinterp'" when running create_2dplot
**This means you cannot use 2D functionality.** You must install pyinterp:
- **Recommended:** Use conda (Option 2 above)
- **Alternative:** Try `pip install pyinterp` (may fail on Windows)
- **Last resort:** Compile from source (Option 3 above)

### Import errors for other packages
```bash
pip install --upgrade pip setuptools wheel
pip install -e . --force-reinstall
```

---

## Platform-Specific Notes

### Linux
All dependencies install easily:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -e .[spatial]
```

### macOS
Similar to Linux, but may need Homebrew for system libraries:
```bash
brew install boost eigen cmake
python3 -m venv venv
source venv/bin/activate
pip install -e .[spatial]
```

### Windows
Use one of the three options above. **Option 1 (no pyinterp)** or **Option 2 (conda)** recommended.

---

## Summary

### Installation Options by Functionality Needed

| Need 2D? | Platform | Command | Time |
|----------|----------|---------|------|
| **Yes** (2D + 1D) | **All** (Recommended) | `conda env create -f environment.yml` | 1-2 min |
| **Yes** (2D + 1D) | Linux/macOS | `pip install -e .[spatial]` | 1-3 min |
| **No** (1D only) | Any | `pip install -e .` | 30 sec |

### Quick Decision Guide

**✅ RECOMMENDED: Use conda with environment.yml for all platforms**
```bash
conda env create -n ofs_skill -f environment.yml
conda activate ofs_skill
pip install -e .
```
This gives you:
- ✅ Full 2D + 1D functionality
- ✅ Python 3.11 (latest stable)
- ✅ All dependencies pre-configured
- ✅ Works on Windows, Linux, macOS

**Alternative: If you only need `create_1dplot` and station analysis:**
- Use `pip install -e .` - fast and simple (no 2D support)

### Key Takeaway

⚠️ **pyinterp is REQUIRED for all 2D functionality including `create_2dplot`**

**Conda with environment.yml is the recommended installation method for all platforms.**
