# OpenCLIF

**Open ICU Dataset Mappings for the Common Longitudinal ICU Data Format (CLIF)**

OpenCLIF bridges the gap between CLIF's standardized clinical categories and publicly available ICU datasets. It provides dataset-specific identifiers that enable researchers to extract CLIF-compatible data from multiple open ICU databases using a single set of mappings.

## Purpose

The [Common Longitudinal ICU Data Format (CLIF)](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF) consortium has defined standard clinical categories (via the mCIDE specification) for ICU data. However, mapping these categories to specific datasets requires knowing the exact item IDs, variable names, or regex patterns for each database.

OpenCLIF solves this by:
1. Taking CLIF's mCIDE category definitions
2. Adding mappings from the [ricu](https://github.com/eth-mds/ricu) package's concept dictionary
3. Providing ready-to-use identifiers for 5 major open ICU datasets

## Supported Datasets

| Dataset | Source | Access |
|---------|--------|--------|
| **MIMIC-III** | Beth Israel Deaconess Medical Center | [PhysioNet](https://physionet.org/content/mimiciii/) |
| **MIMIC-IV** | Beth Israel Deaconess Medical Center | [PhysioNet](https://physionet.org/content/mimiciv/) |
| **eICU-CRD** | Philips eICU Research Institute | [PhysioNet](https://physionet.org/content/eicu-crd/) |
| **HiRID** | Bern University Hospital | [PhysioNet](https://physionet.org/content/hirid/) |
| **AmsterdamUMCdb** | Amsterdam UMC | [AmsterdamMedicalDataScience](https://amsterdammedicaldatascience.nl/amsterdamumcdb/) |

## Mapping Structure

Each CSV file in the `mappings/` directory contains:

| Column | Description |
|--------|-------------|
| `*_category` | CLIF category name (e.g., `heart_rate`, `albumin`, `norepinephrine`) |
| `description` | Clinical description from CLIF mCIDE |
| `*_examples` | Example names/strings from source systems |
| `ricu_concept` | Corresponding concept name in ricu's concept-dict.json |
| `mimic_iii_itemid` | Item ID(s) for MIMIC-III (chartevents, labevents, etc.) |
| `mimic_iv_itemid` | Item ID(s) for MIMIC-IV |
| `eicu_ids` | Table/column or lab name for eICU-CRD |
| `hirid_ids` | Variable ID(s) for HiRID |
| `aumc_ids` | Item ID(s) for AmsterdamUMCdb |

### ID Format Notes

- **Numeric IDs**: Direct item IDs (e.g., `220045` for heart rate in MIMIC-IV)
- **Multiple IDs**: Separated by semicolons (e.g., `220050; 220179`)
- **Regex patterns**: Prefixed with `regex:` (e.g., `regex:^norepi`)
- **Column references**: Prefixed with `col:` (e.g., `col:heartrate`)
- **Empty values**: No mapping available in ricu for that dataset

## Directory Structure

```
OpenCLIF/
├── README.md
├── mappings/
│   ├── vitals/
│   │   └── clif_vitals_categories.csv
│   ├── labs/
│   │   └── clif_lab_categories.csv
│   ├── medications/
│   │   └── clif_medication_categories.csv
│   └── respiratory_support/
│       └── clif_respiratory_support_device_categories.csv
└── scripts/
    ├── build_openclif.py
    └── concept-dict.json
```

## Usage Examples

### Python - Extract heart rate from MIMIC-IV

```python
import pandas as pd

# Load mappings
vitals = pd.read_csv('mappings/vitals/clif_vitals_categories.csv')

# Get MIMIC-IV item IDs for heart rate
hr_row = vitals[vitals['vital_category'] == 'heart_rate']
mimic_iv_ids = hr_row['mimic_iv_itemid'].values[0].split('; ')
# Returns: ['220045']

# Use in query
query = f"""
SELECT *
FROM chartevents
WHERE itemid IN ({','.join(mimic_iv_ids)})
"""
```

### R - Use with ricu

```r
library(ricu)
library(readr)

# Load OpenCLIF mappings
vitals <- read_csv("mappings/vitals/clif_vitals_categories.csv")

# Cross-reference with ricu concept
hr_concept <- vitals$ricu_concept[vitals$vital_category == "heart_rate"]
# Returns: "hr"

# Load using ricu
hr_data <- load_concepts("hr", "miiv")
```

## Mapping Coverage

### Vitals (9 categories)
- ✅ All 9 categories mapped (100%)
- Full coverage: temp, heart_rate, sbp, dbp, spo2, respiratory_rate, map, height, weight

### Labs (55 categories)  
- ✅ 35 categories mapped (~64%)
- Well covered: basic metabolic panel, liver function tests, CBC, coagulation
- Gaps: Some differential counts (absolute values), specialized markers

### Medications (50 categories)
- ✅ 8 categories mapped (~16%)
- Well covered: vasopressors (norepinephrine, epinephrine, vasopressin, dopamine, dobutamine, phenylephrine), insulin
- Gaps: Most sedatives, anticoagulants, paralytics (not tracked by rate in ricu)

### Respiratory Support (9 categories)
- ✅ 2 categories mapped (~22%)
- Covered: IMV, NIPPV via mech_vent concept
- Gaps: CPAP, HFNC, other oxygen devices

## Sources & Attribution

- **CLIF mCIDE definitions**: [Common-Longitudinal-ICU-data-Format/skills](https://github.com/Common-Longitudinal-ICU-data-Format/skills/tree/main/skills/clif-icu/mCIDE)
- **ricu concept mappings**: [eth-mds/ricu](https://github.com/eth-mds/ricu) - Bennett et al., "ricu: R's interface to intensive care data"
- **MIMIC databases**: Johnson et al., PhysioNet
- **eICU-CRD**: Pollard et al., PhysioNet
- **HiRID**: Hyland et al., PhysioNet
- **AmsterdamUMCdb**: Thoral et al., Amsterdam UMC

## Contributing

Contributions are welcome! Areas that need work:

1. **Adding missing mappings** - Especially for sedatives, paralytics, and respiratory devices
2. **Validation** - Verifying mappings against actual dataset schemas
3. **Unit conversions** - Documenting unit differences between datasets
4. **Additional datasets** - SICdb, other open ICU databases

## License

This project is part of the CLIF consortium. Mappings derived from ricu are subject to its license terms.

## Related Projects

- [CLIF](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF) - Core CLIF specification
- [CLIFpy](https://github.com/Common-Longitudinal-ICU-data-Format/clif-consortia/CLIFpy) - Python tools for CLIF
- [ricu](https://github.com/eth-mds/ricu) - R interface for intensive care data
- [MIMIC Code Repository](https://github.com/MIT-LCP/mimic-code) - Official MIMIC analysis code

---

*Built by the CLIF Consortium to accelerate ICU research across open datasets.*
