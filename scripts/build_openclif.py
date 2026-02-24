#!/usr/bin/env python3
"""
Build OpenCLIF repository by combining CLIF mCIDE definitions with ricu concept mappings.
This script creates enhanced CSV files with dataset-specific identifiers for open ICU datasets.
"""

import json
import csv
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

# CLIF to ricu concept mappings
# Maps CLIF category names to ricu concept names
CLIF_TO_RICU_MAPPINGS = {
    # Vitals
    "temp_c": "temp",
    "heart_rate": "hr",
    "sbp": "sbp",
    "dbp": "dbp",
    "spo2": "spo2",
    "respiratory_rate": "resp",
    "map": "map",
    "height_cm": "height",
    "weight_kg": "weight",
    
    # Labs - chemistry
    "albumin": "alb",
    "alkaline_phosphatase": "alp",
    "alt": "alt",
    "ast": "ast",
    "bicarbonate": "bicar",
    "bilirubin_total": "bili",
    "bilirubin_conjugated": "bili_dir",
    "bun": "bun",
    "calcium_total": "ca",
    "calcium_ionized": "cai",
    "chloride": "cl",
    "creatinine": "crea",
    "crp": "crp",
    "glucose_serum": "glu",
    "glucose_fingerstick": "glu",
    "magnesium": "mg",
    "phosphate": "phos",
    "potassium": "k",
    "sodium": "na",
    "lactate": "lact",
    
    # Labs - hematology  
    "hemoglobin": "hgb",
    "platelet_count": "plt",
    "wbc": "wbc",
    "neutrophils_percent": "neut",
    "lymphocytes_percent": "lymph",
    "eosinophils_percent": "eos",
    "basophils_percent": "basos",
    "inr": "inr_pt",
    "pt": "pt",
    "ptt": "ptt",
    "troponin_i": "tri",
    "troponin_t": "tnt",
    "ferritin": None,  # Not in ricu
    "ldh": None,  # Not in ricu
    "esr": "esr",
    
    # Labs - blood gas
    "pco2_arterial": "pco2",
    "po2_arterial": "po2",
    "ph_arterial": "ph",
    "so2_arterial": "sao2",
    
    # Medications - vasopressors
    "norepinephrine": "norepi_rate",
    "epinephrine": "epi_rate", 
    "vasopressin": "adh_rate",
    "dopamine": "dopa_rate",
    "dobutamine": "dobu_rate",
    "phenylephrine": "phn_rate",
    
    # Other medications
    "insulin": "ins",
    "milrinone": None,  # Not in ricu
    "dexmedetomidine": None,  # Not in ricu (sedatives not tracked by rate)
    "propofol": None,  # Not in ricu
    "midazolam": None,  # Not in ricu
    "fentanyl": None,  # Not in ricu
    "heparin": None,  # Not tracked in ricu concept-dict
}


def extract_ids_from_source(source_entry: dict, dataset: str) -> str:
    """Extract item IDs from a ricu source entry."""
    ids = []
    if isinstance(source_entry, list):
        for item in source_entry:
            if 'ids' in item:
                item_ids = item['ids']
                if isinstance(item_ids, list):
                    ids.extend([str(i) for i in item_ids])
                else:
                    ids.append(str(item_ids))
            elif 'regex' in item:
                ids.append(f"regex:{item['regex']}")
            elif 'val_var' in item:
                ids.append(f"col:{item['val_var']}")
    return "; ".join(ids) if ids else ""


def parse_ricu_concepts(concept_dict: dict) -> Dict[str, Dict[str, Any]]:
    """Parse ricu concept dictionary and extract mappings."""
    mappings = {}
    
    for concept_name, concept_data in concept_dict.items():
        if 'sources' not in concept_data:
            continue
            
        sources = concept_data['sources']
        mapping = {
            'description': concept_data.get('description', ''),
            'category': concept_data.get('category', ''),
            'unit': concept_data.get('unit', ''),
            'mimic_iii_itemid': '',
            'mimic_iv_itemid': '',
            'eicu_ids': '',
            'hirid_ids': '',
            'aumc_ids': '',
        }
        
        # MIMIC-III (called 'mimic' in ricu)
        if 'mimic' in sources:
            mapping['mimic_iii_itemid'] = extract_ids_from_source(sources['mimic'], 'mimic')
        
        # MIMIC-IV (called 'miiv' in ricu)  
        if 'miiv' in sources:
            mapping['mimic_iv_itemid'] = extract_ids_from_source(sources['miiv'], 'miiv')
            
        # eICU
        if 'eicu' in sources:
            mapping['eicu_ids'] = extract_ids_from_source(sources['eicu'], 'eicu')
            
        # HiRID
        if 'hirid' in sources:
            mapping['hirid_ids'] = extract_ids_from_source(sources['hirid'], 'hirid')
            
        # AmsterdamUMCdb (AUMC)
        if 'aumc' in sources:
            mapping['aumc_ids'] = extract_ids_from_source(sources['aumc'], 'aumc')
            
        mappings[concept_name] = mapping
        
    return mappings


def enhance_csv_with_mappings(input_rows: List[dict], category_column: str, 
                               ricu_mappings: Dict[str, Dict[str, Any]],
                               clif_to_ricu: Dict[str, Optional[str]]) -> List[dict]:
    """Add ricu mapping columns to CSV rows."""
    enhanced_rows = []
    
    for row in input_rows:
        category = row.get(category_column, '').lower().strip()
        
        # Look up the ricu concept
        ricu_concept = clif_to_ricu.get(category)
        
        # Add new columns
        enhanced_row = dict(row)
        enhanced_row['ricu_concept'] = ricu_concept if ricu_concept else ''
        
        if ricu_concept and ricu_concept in ricu_mappings:
            mapping = ricu_mappings[ricu_concept]
            enhanced_row['mimic_iii_itemid'] = mapping['mimic_iii_itemid']
            enhanced_row['mimic_iv_itemid'] = mapping['mimic_iv_itemid']
            enhanced_row['eicu_ids'] = mapping['eicu_ids']
            enhanced_row['hirid_ids'] = mapping['hirid_ids']
            enhanced_row['aumc_ids'] = mapping['aumc_ids']
        else:
            enhanced_row['mimic_iii_itemid'] = ''
            enhanced_row['mimic_iv_itemid'] = ''
            enhanced_row['eicu_ids'] = ''
            enhanced_row['hirid_ids'] = ''
            enhanced_row['aumc_ids'] = ''
            
        enhanced_rows.append(enhanced_row)
        
    return enhanced_rows


def write_enhanced_csv(rows: List[dict], output_path: Path, extra_columns: List[str]):
    """Write enhanced CSV with new columns at the end."""
    if not rows:
        return
        
    # Get existing columns from first row
    existing_cols = list(rows[0].keys())
    
    # Remove extra columns from existing (in case they're already there)
    for col in extra_columns:
        if col in existing_cols:
            existing_cols.remove(col)
    
    # Final column order: existing + extra
    fieldnames = existing_cols + extra_columns
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# CSV data (embedded for simplicity)
VITALS_CSV = """vital_category,description,vital_name_examples
temp_c,"Body temperature in degrees Celsius, measured via any clinical method (oral, axillary, tympanic, rectal, or core). The measurement site or device type should be indicated in meas_site_name.","Temperature (C), Temp celsius"
heart_rate,"Heart rate in beats per minute (bpm), obtained from monitor, pulse oximeter, or manual palpation.","heart rate"
sbp,"Systolic blood pressure. Can include both invasive (arterial line) and non-invasive (cuff) measurements; specify in meas_site_name.","blood pressure, blood pressure SBP"
dbp,"Diastolic blood pressure. Can include both invasive (arterial line) and non-invasive (cuff) measurements; specify in meas_site_name.","blood pressure, blood pressure DBP"
spo2,"Peripheral oxygen saturation (%), typically measured by pulse oximetry.","spO2"
respiratory_rate,"Number of breaths per minute, either manually counted or obtained from a monitor.","respiratory rate"
map,"Can include both invasive (arterial line) and non-invasive (cuff) measurements. Source type should be indicated in meas_site_name.","MAP, Mean arterial pressure"
height_cm,"Patient's height in centimeters, measured or recorded at admission or encounter.","Height, Ht"
weight_kg,"Patient's weight in kilograms, measured or recorded at admission or encounter.","Weight, Wt"
"""

LABS_CSV = """lab_category,reference_unit,description,lab_name_type_examples,lab_order_category
albumin,g/dL,,"ALBUMIN, ALB, SERUM ALBUMIN",lft
alkaline_phosphatase,U/L,,"ALP, ALKALINE PHOS, ALK PHOS, ALKP",lft
alt,U/L,,"ALT, SGPT, ALANINE TRANSAMINASE",lft
ast,U/L,,"AST, SGOT, ASPARTATE TRANSAMINASE",lft
basophils_percent,%,,"BASO%, BASOPHILS %, BASO PERCENT",cbc
basophils_absolute,10^3/µL,,"BASO#, BASOPHILS ABS, BASO ABSOLUTE",cbc
bicarbonate,mmol/L,bicarbonate from BMP (sometimes reported out as CO2) not from ABG/VBG,"CO2, HCO3, BICARB, CARBON DIOXIDE",bmp
bilirubin_total,mg/dL,,"TBILI, TOTAL BILI, BILIRUBIN TOTAL",lft
bilirubin_conjugated,mg/dL,,"DBILI, DIRECT BILI, CONJUGATED BILI",lft
bilirubin_unconjugated,mg/dL,,"INDIRECT BILI, UNCONJUGATED BILI, IBILI",lft
bun,mg/dL,,"BUN, UREA NITROGEN, BLOOD UREA",bmp
calcium_total,mg/dL,includes lactate,"CA, CALCIUM, TOTAL CALCIUM, SERUM CA",misc
calcium_ionized,mg/dL,,"IONIZED CA, ICA, FREE CALCIUM",bmp
chloride,mmol/L,,"CL, CHLORIDE, SERUM CHLORIDE",bmp
creatinine,mg/dL,,"CR, CREAT, SERUM CREATININE",bmp
crp,mg/L,includes lactate,"CRP, C-REACTIVE PROTEIN, HS-CRP",misc
eosinophils_percent,%,,"EOS%, EOSINOPHILS %, EO PERCENT",cbc
eosinophils_absolute,10^3/µL,,"EOS#, EOSINOPHILS ABS, EO ABSOLUTE",cbc
esr,mm/hour,includes lactate,"ESR, SED RATE, SEDIMENTATION RATE",misc
ferritin,ng/mL,includes lactate,"FERRITIN, SERUM FERRITIN, FERR",misc
glucose_fingerstick,mg/dL,includes lactate,"POC GLUCOSE, FINGERSTICK GLUCOSE, ACCUCHECK, BEDSIDE GLUCOSE",misc
glucose_serum,mg/dL,,"GLUCOSE, SERUM GLUCOSE, BLOOD GLUCOSE, GLU",bmp
hemoglobin,g/dL,,"HGB, HB, HEMOGLOBIN",cbc
phosphate,mg/dL,sometimes called 'phosphorus' in the EHR,"PHOS, PHOSPHORUS, PHOSPHATE, SERUM PHOS",coags
inr,(no units),includes lactate,"INR, INTERNATIONAL NORMALIZED RATIO",misc
lactate,mmol/L,includes lactate,"LACTATE, LACTIC ACID, LAC",misc
ldh,U/L,,"LDH, LD, LACTATE DEHYDROGENASE",cbc
lymphocytes_percent,%,,"LYMPH%, LYMPHOCYTES %, LYMPH PERCENT",cbc
lymphocytes_absolute,10^3/µL,includes lactate,"LYMPH#, LYMPHOCYTES ABS, LYMPH ABSOLUTE",misc
magnesium,mg/dL,,"MG, MAGNESIUM, SERUM MG",cbc
monocytes_percent,%,,"MONO%, MONOCYTES %, MONO PERCENT",cbc
monocytes_absolute,10^3/µL,,"MONO#, MONOCYTES ABS, MONO ABSOLUTE",cbc
neutrophils_percent,%,,"NEUT%, NEUTROPHILS %, NEUT PERCENT, PMN%",cbc
neutrophils_absolute,10^3/µL,includes abg/vbg/mixed venous/etc,"NEUT#, ANC, NEUTROPHILS ABS, PMN#",blood_gas
pco2_arterial,mmHg,includes abg/vbg/mixed venous/etc,"PACO2, ARTERIAL PCO2, ABG PCO2",blood_gas
po2_arterial,mmHg,includes abg/vbg/mixed venous/etc,"PAO2, ARTERIAL PO2, ABG PO2",blood_gas
pco2_venous,mmHg,includes abg/vbg/mixed venous/etc,"PVCO2, VENOUS PCO2, VBG PCO2",blood_gas
ph_arterial,(no units),includes lactate,"ABG PH, ARTERIAL PH, BLOOD PH",misc
ph_venous,(no units),,"VBG PH, VENOUS PH",cbc
platelet_count,10^3/µL,includes abg/vbg/mixed venous/etc,"PLT, PLATELETS, PLATELET COUNT",blood_gas
potassium,mmol/L,,"K, POTASSIUM, SERUM K",bmp
procalcitonin,ng/mL,includes lactate,"PCT, PROCALCITONIN, PRO-CALCITONIN",misc
pt,sec,,"PT, PROTIME, PROTHROMBIN TIME",coags
ptt,sec,,"PTT, APTT, ACTIVATED PTT",coags
so2_arterial,%,includes abg/vbg/mixed venous/etc,"SAO2, ARTERIAL SAT, ABG O2 SAT",blood_gas
so2_mixed_venous,%,includes abg/vbg/mixed venous/etc,"SVO2, MIXED VENOUS SAT, SVMVO2",blood_gas
so2_central_venous,%,includes abg/vbg/mixed venous/etc,"SCVO2, CENTRAL VENOUS SAT",blood_gas
sodium,mmol/L,,"NA, SODIUM, SERUM NA",bmp
total_protein,g/dL,,"TP, TOTAL PROTEIN, SERUM PROTEIN",lft
troponin_i,ng/L,includes lactate,"TROP I, TROPONIN I, TNI",misc
troponin_t,ng/L,includes lactate,"TROP T, TROPONIN T, TNT",misc
wbc,10^3/µL,,"WBC, WHITE BLOOD CELL, LEUKOCYTES",cbc
"""

MEDICATIONS_CSV = """med_category,description,med_name_examples,med_group
norepinephrine,Catecholamine for hemodynamic support,"NOREPINEPHRINE 16 MG/250 ML IV INFUSION, NOREPINEPHRINE 0.01-3 MCG/KG/MIN IV",vasoactives
epinephrine,Catecholamine for hemodynamic support and/or anaphylaxis,"EPINEPHRINE 4 MG/250 ML IV INFUSION, EPINEPHRINE 0.01-0.5 MCG/KG/MIN IV",vasoactives
vasopressin,Antidiuretic hormone for septic shock,"VASOPRESSIN 20 UNITS/100 ML IV INFUSION, VASOPRESSIN 0.01-0.04 UNITS/MIN IV",vasoactives
dopamine,Vasopressor and inotrope for hemodynamic support,"DOPAMINE 400 MG/250 ML IV INFUSION, DOPAMINE 5-20 MCG/KG/MIN IV",vasoactives
dobutamine,Inotropic agent to increase cardiac contractility,"DOBUTAMINE 500 MG/250 ML IV INFUSION, DOBUTAMINE 2-20 MCG/KG/MIN IV",vasoactives
phenylephrine,Alpha-1 agonist vasopressor,"PHENYLEPHRINE 200 MG/250 ML IV INFUSION, PHENYLEPHRINE 0.5-3 MCG/KG/MIN IV",vasoactives
milrinone,Phosphodiesterase inhibitor inotrope,"MILRINONE 40 MG/200 ML IV INFUSION, MILRINONE 0.375-0.75 MCG/KG/MIN IV",vasoactives
insulin,Hormone for glucose management,"INSULIN REGULAR 100 UNITS/100 ML IV INFUSION, INSULIN 1-10 UNITS/HR IV",endocrine
propofol,Sedative-hypnotic for anesthesia and sedation,"PROPOFOL 1000 MG/100 ML IV INFUSION, PROPOFOL 5-50 MCG/KG/MIN IV",sedation
dexmedetomidine,Alpha-2 agonist sedative,"DEXMEDETOMIDINE 200 MCG/50 ML IV INFUSION, DEXMEDETOMIDINE 0.2-1.5 MCG/KG/HR IV",sedation
midazolam,Benzodiazepine for anxiety and sedation,"MIDAZOLAM 100 MG/100 ML IV INFUSION, MIDAZOLAM 1-10 MG/HR IV",sedation
fentanyl,Opioid analgesic,"FENTANYL 2500 MCG/50 ML IV INFUSION, FENTANYL 25-200 MCG/HR IV",sedation
heparin,Anticoagulant for thrombosis treatment and prevention,"HEPARIN 25000 UNITS/250 ML IV INFUSION, HEPARIN 12-18 UNITS/KG/HR IV",anticoagulation
"""

RESPIRATORY_CSV = """device_category,description,device_name_examples
IMV,Invasive Mechanical Ventilation,"Endotracheal Tube Ventilation, Tracheostomy Ventilation"
NIPPV,Non-Invasive Positive Pressure Ventilation,"BiPAP, AVAPS"
CPAP,Continuous Positive Airway Pressure,"CPAP Mask, CPAP Therapy"
High Flow NC,High Flow Nasal Cannula,"High Flow Oxygen Therapy, Optiflow"
Face Mask,Face Mask oxygen delivery,"Simple Face Mask, Non-Rebreather Mask"
Trach Collar,Tracheostomy collar,"Trach Collar with Aerosol, T-Piece"
Nasal Cannula,Standard nasal cannula,"Low Flow Nasal Cannula, NC 1-6L"
Room Air,Breathing room air without supplemental oxygen,"RA, Room Air - No O2"
Other,Other respiratory support device not specified above,"Venturi Mask, Oxygen Tent"
"""


def main():
    # Load the ricu concept dictionary
    script_dir = Path(__file__).parent
    concept_dict_path = script_dir / "concept-dict.json"
    
    if not concept_dict_path.exists():
        print(f"Error: concept-dict.json not found at {concept_dict_path}")
        print("Please download from: https://raw.githubusercontent.com/eth-mds/ricu/main/inst/extdata/config/concept-dict.json")
        return
    
    with open(concept_dict_path, 'r', encoding='utf-8') as f:
        concept_dict = json.load(f)
    
    # Parse ricu concepts
    ricu_mappings = parse_ricu_concepts(concept_dict)
    print(f"Loaded {len(ricu_mappings)} ricu concepts")
    
    # Output directory
    output_dir = script_dir.parent / "mappings"
    
    # Extra columns to add
    extra_columns = ['ricu_concept', 'mimic_iii_itemid', 'mimic_iv_itemid', 'eicu_ids', 'hirid_ids', 'aumc_ids']
    
    # Process vitals
    print("Processing vitals...")
    vitals_rows = list(csv.DictReader(VITALS_CSV.strip().splitlines()))
    enhanced_vitals = enhance_csv_with_mappings(vitals_rows, 'vital_category', ricu_mappings, CLIF_TO_RICU_MAPPINGS)
    write_enhanced_csv(enhanced_vitals, output_dir / "vitals" / "clif_vitals_categories.csv", extra_columns)
    
    # Process labs
    print("Processing labs...")
    labs_rows = list(csv.DictReader(LABS_CSV.strip().splitlines()))
    enhanced_labs = enhance_csv_with_mappings(labs_rows, 'lab_category', ricu_mappings, CLIF_TO_RICU_MAPPINGS)
    write_enhanced_csv(enhanced_labs, output_dir / "labs" / "clif_lab_categories.csv", extra_columns)
    
    # Process medications
    print("Processing medications...")
    meds_rows = list(csv.DictReader(MEDICATIONS_CSV.strip().splitlines()))
    enhanced_meds = enhance_csv_with_mappings(meds_rows, 'med_category', ricu_mappings, CLIF_TO_RICU_MAPPINGS)
    write_enhanced_csv(enhanced_meds, output_dir / "medications" / "clif_medication_categories.csv", extra_columns)
    
    # Process respiratory (no ricu mappings, just add empty columns)
    print("Processing respiratory support...")
    resp_rows = list(csv.DictReader(RESPIRATORY_CSV.strip().splitlines()))
    # Respiratory doesn't have direct ricu mappings, but we add the columns for consistency
    for row in resp_rows:
        row['ricu_concept'] = ''
        row['mimic_iii_itemid'] = ''
        row['mimic_iv_itemid'] = ''
        row['eicu_ids'] = ''
        row['hirid_ids'] = ''
        row['aumc_ids'] = ''
    write_enhanced_csv(resp_rows, output_dir / "respiratory_support" / "clif_respiratory_support_device_categories.csv", extra_columns)
    
    print(f"\nOutput written to: {output_dir}")
    print("\nMapping coverage summary:")
    
    # Summary stats
    for name, rows in [("Vitals", enhanced_vitals), ("Labs", enhanced_labs), ("Medications", enhanced_meds)]:
        total = len(rows)
        mapped = sum(1 for r in rows if r.get('ricu_concept'))
        print(f"  {name}: {mapped}/{total} categories mapped to ricu ({100*mapped/total:.0f}%)")


if __name__ == "__main__":
    main()
