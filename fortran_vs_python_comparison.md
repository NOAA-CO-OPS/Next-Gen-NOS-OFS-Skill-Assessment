# Fortran vs Python Constituent Table Comparison

Comparison of the legacy Fortran `table_Harmonic_C.f` (STEP 10) against the
Python replacement `constituent_table.py`, run on identical harmonic analysis
results from a synthetic M2+S2+N2+K1+O1 tidal signal (30 days, 6-min interval,
latitude 41.5N).

## Constituent Set

| | Count | Unique Members |
|---|---|---|
| Fortran ALIST | 37 | S1, M3 (not in Python) |
| Python NOS\_37 | 37 | MSM, MO3 (not in Fortran) |
| Common | 35 | |

## Constituent Ordering

The first three positions (M2, S2, N2) are shared. After that every position
differs. The Fortran uses a custom order while Python follows NOS CS 24
Appendix C (semidiurnal, diurnal, long-period, shallow-water).

| N | Fortran | Python (NOS CS 24) |
|--:|---------|---------------------|
| 1 | M2 | M2 |
| 2 | S2 | S2 |
| 3 | N2 | N2 |
| 4 | K1 | K2 |
| 5 | M4 | 2N2 |
| 6 | O1 | MU2 |
| 7 | M6 | NU2 |
| 8 | MK3 | L2 |
| 9 | S4 | T2 |
| 10 | MN4 | R2 |
| 11 | NU2 | LDA2 |
| 12 | S6 | K1 |
| 13 | MU2 | O1 |
| 14 | 2N2 | P1 |
| 15 | OO1 | Q1 |
| 16 | LDA2 | J1 |
| 17 | **S1** | **M1** |
| 18 | M1 | OO1 |
| 19 | J1 | 2Q1 |
| 20 | MM | RHO1 |
| 21 | SSA | MF |
| 22 | SA | MM |
| 23 | MSF | SSA |
| 24 | MF | SA |
| 25 | RHO1 | **MSM** |
| 26 | Q1 | MSF |
| 27 | T2 | M4 |
| 28 | R2 | M6 |
| 29 | 2Q1 | M8 |
| 30 | P1 | MS4 |
| 31 | 2SM2 | MN4 |
| 32 | **M3** | MK3 |
| 33 | L2 | S4 |
| 34 | 2MK3 | S6 |
| 35 | K2 | 2MK3 |
| 36 | M8 | 2SM2 |
| 37 | MS4 | **MO3** |

This is purely a display difference; the underlying analysis is identical.

## Phase Wrapping Logic

### Fortran implementation

```fortran
DIFF = EPOC_M(J) - EPOC_O(J)
IF(DIFF .LT. -180.0) DIFF = DIFF + 360.0
IF(DIFF .GT.  180.0) DIFF = 360.0 - DIFF
```

### Python implementation

```python
phase_diff = (model_phase - accepted_phase + 180.0) % 360.0 - 180.0
```

### Test cases

| Model | Obs | Fortran | Python | Match | Description |
|------:|----:|--------:|-------:|:-----:|-------------|
| 50.0 | 45.0 | 5.0 | 5.0 | Yes | small positive |
| 45.0 | 50.0 | -5.0 | -5.0 | Yes | small negative |
| 10.0 | 350.0 | 20.0 | 20.0 | Yes | across 360 boundary (+) |
| 350.0 | 10.0 | **20.0** | **-20.0** | **No** | across 360 boundary (-) |
| 200.0 | 10.0 | **170.0** | **-170.0** | **No** | large positive (190) |
| 10.0 | 200.0 | 170.0 | 170.0 | Yes | large negative (-190) |
| 270.0 | 90.0 | **180.0** | **-180.0** | **No** | exactly 180 |
| 90.0 | 270.0 | -180.0 | -180.0 | Yes | exactly -180 |

The Fortran logic has an asymmetry: when `diff > 180`, it computes `360 - diff`
(always positive), losing the sign. The Python modular arithmetic preserves the
correct signed direction. In practice this only affects constituents with
near-zero amplitudes where the phase is noise.

## Numerical Comparison

Both pipelines were fed the same UTide harmonic analysis output from a synthetic
currents test case (obs signal vs model signal with known offsets).

| Constituent | Amp Diff (Fortran) | Amp Diff (Python) | Match | Phase Diff (Fortran) | Phase Diff (Python) | Match | Vector Diff (Python only) |
|---|---:|---:|:---:|---:|---:|:---:|---:|
| **M2** | +0.0208 | +0.0208 | OK | +3.0 | +3.0 | OK | 0.0347 |
| **S2** | -0.0096 | -0.0096 | OK | -2.8 | -2.8 | OK | 0.0104 |
| **N2** | -0.0104 | -0.0104 | OK | +3.0 | +3.0 | OK | 0.0121 |
| **K1** | -0.0090 | -0.0090 | OK | -5.0 | -5.0 | OK | 0.0165 |
| **O1** | -0.0086 | -0.0086 | OK | -3.9 | -3.9 | OK | 0.0106 |
| K2 | -0.0001 | -0.0001 | OK | +154.9 | -154.9 | DIFF | 0.0005 |
| M4 | -0.0000 | -0.0000 | OK | -23.0 | -23.0 | OK | 0.0000 |
| MS4 | +0.0000 | +0.0000 | OK | +103.5 | +103.5 | OK | 0.0001 |

All amplitude differences match exactly. Phase differences match for all
physically meaningful constituents. The sole disagreement (K2, amplitude
0.0003 m/s) is a noise constituent where the Fortran phase wrapping bug
flips the sign.

## Summary of Differences

### 1. Constituent set

Fortran includes S1 and M3; Python includes MSM and MO3. The remaining 35
constituents are identical. The Python set follows NOS CS 24 Appendix C.

### 2. Constituent ordering

Fortran uses a custom order; Python groups by type (semidiurnal, diurnal,
long-period, shallow-water) per NOS CS 24. Display-only difference.

### 3. Phase wrapping (bug fix)

Fortran `360 - diff` loses the sign for large positive phase differences.
Python modular arithmetic `(diff + 180) % 360 - 180` preserves the correct
signed direction. Only affects near-zero-amplitude noise constituents in
practice.

### 4. Near-zero amplitude handling

Fortran sets amplitude and phase diffs to 0.0 when the observed amplitude is
below 0.00001. Python uses NaN for missing constituents, which is more explicit
and avoids conflating "zero difference" with "no data".

### 5. Vector difference (new in Python)

Python adds the NOS-convention vector difference metric:

```
Vd = sqrt(Am^2 + Aa^2 - 2 * Am * Aa * cos(delta_g))
```

This combines amplitude and phase errors into a single physical-units value.
The Fortran program does not compute this.

### 6. Output format

Fortran writes fixed-width text (`F7.3`, `F6.1` format codes). Python writes
CSV with `#`-prefixed metadata header lines (station ID, data type, timestamp,
optional extras).

## Conclusion

The Python implementation is numerically equivalent to the Fortran for all
scientifically meaningful constituents. It improves on the legacy code with
correct signed phase wrapping, explicit NaN handling for missing data, the
addition of the vector difference metric, and structured CSV output with
metadata.
