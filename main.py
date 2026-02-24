#!/usr/bin/env python3
"""
OpenCLIF ETL - Convert open ICU datasets to CLIF format

Usage:
    uv run main.py --source eicu --path /path/to/eicu
    uv run main.py --source sic --path /path/to/sicdb
    uv run main.py --source hirid --path /path/to/hirid
    uv run main.py --source aumc --path /path/to/amsterdamumcdb
"""

import argparse
from pathlib import Path
import polars as pl
from typing import Optional


# Supported datasets
SUPPORTED_SOURCES = ["eicu", "hirid", "aumc", "sic"]


def load_mappings(mapping_type: str) -> pl.DataFrame:
    """Load CLIF mapping CSV for a given type (vitals, labs, medications, respiratory_support)."""
    mapping_dir = Path(__file__).parent / "mappings" / mapping_type
    csv_files = list(mapping_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No mapping file found in {mapping_dir}")
    return pl.read_csv(csv_files[0])


def get_source_column(source: str) -> str:
    """Get the column name for a given source dataset."""
    return f"{source}_ids"


class OpenCLIFETL:
    """ETL processor for converting open ICU datasets to CLIF format."""
    
    def __init__(self, source: str, data_path: Path, output_path: Optional[Path] = None):
        self.source = source
        self.data_path = Path(data_path)
        self.output_path = output_path or (self.data_path / "clif_output")
        self.source_col = get_source_column(source)
        
        # Load all mappings
        self.vitals_map = load_mappings("vitals")
        self.labs_map = load_mappings("labs")
        self.meds_map = load_mappings("medications")
        self.resp_map = load_mappings("respiratory_support")
        
        print(f"Loaded mappings for {source}")
        print(f"  Vitals: {len(self.vitals_map)} categories")
        print(f"  Labs: {len(self.labs_map)} categories")
        print(f"  Medications: {len(self.meds_map)} categories")
        print(f"  Respiratory: {len(self.resp_map)} categories")
    
    def run(self):
        """Run the full ETL pipeline."""
        print(f"\n{'='*60}")
        print(f"OpenCLIF ETL: {self.source.upper()}")
        print(f"{'='*60}")
        print(f"Source path: {self.data_path}")
        print(f"Output path: {self.output_path}")
        
        # Create output directory
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        # Dispatch to source-specific ETL
        if self.source == "eicu":
            self.etl_eicu()
        elif self.source == "sic":
            self.etl_sic()
        elif self.source == "hirid":
            self.etl_hirid()
        elif self.source == "aumc":
            self.etl_aumc()
        else:
            raise ValueError(f"Unsupported source: {self.source}")
        
        print(f"\n✅ ETL complete! Output written to: {self.output_path}")
    
    # =========================================================================
    # eICU ETL
    # =========================================================================
    def etl_eicu(self):
        """ETL for eICU-CRD dataset."""
        print("\n[eICU] Starting ETL...")
        
        # Check for required files
        required_files = ["patient.csv", "vitalperiodic.csv", "lab.csv"]
        for f in required_files:
            fpath = self.data_path / f
            if not fpath.exists():
                print(f"  ⚠️  Missing: {f}")
        
        self._etl_eicu_vitals()
        self._etl_eicu_labs()
    
    def _etl_eicu_vitals(self):
        """Transform eICU vitalPeriodic to CLIF vitals."""
        vital_file = self.data_path / "vitalperiodic.csv"
        if not vital_file.exists():
            print("  [vitals] Skipping - vitalperiodic.csv not found")
            return
        
        print("  [vitals] Processing vitalperiodic.csv...")
        
        # eICU vitals use column names, not item IDs
        # Map from eICU column -> CLIF category
        eicu_vital_map = {
            "temperature": "temp_c",
            "heartrate": "heart_rate", 
            "systemicsystolic": "sbp",
            "systemicdiastolic": "dbp",
            "sao2": "spo2",
            "respiration": "respiratory_rate",
            "systemicmean": "map",
        }
        
        # Read source data
        df = pl.scan_csv(vital_file)
        
        # Get columns that exist
        available_cols = df.collect_schema().names()
        cols_to_extract = ["patientunitstayid", "observationoffset"]
        cols_to_extract.extend([c for c in eicu_vital_map.keys() if c in available_cols])
        
        # Select and unpivot to long format
        df = df.select(cols_to_extract).collect()
        
        # Melt to long format
        id_cols = ["patientunitstayid", "observationoffset"]
        value_cols = [c for c in cols_to_extract if c not in id_cols]
        
        if value_cols:
            df_long = df.unpivot(
                index=id_cols,
                on=value_cols,
                variable_name="eicu_col",
                value_name="vital_value"
            )
            
            # Map to CLIF categories
            df_long = df_long.with_columns(
                pl.col("eicu_col").replace(eicu_vital_map).alias("vital_category")
            )
            
            # Rename columns to CLIF schema
            df_clif = df_long.rename({
                "patientunitstayid": "hospitalization_id",
                "observationoffset": "recorded_dttm_offset_min"
            }).select([
                "hospitalization_id",
                "recorded_dttm_offset_min", 
                "vital_category",
                "vital_value"
            ]).filter(pl.col("vital_value").is_not_null())
            
            # Write output
            output_file = self.output_path / "vitals.parquet"
            df_clif.write_parquet(output_file)
            print(f"    ✓ Wrote {len(df_clif):,} rows to {output_file.name}")
    
    def _etl_eicu_labs(self):
        """Transform eICU lab to CLIF labs."""
        lab_file = self.data_path / "lab.csv"
        if not lab_file.exists():
            print("  [labs] Skipping - lab.csv not found")
            return
        
        print("  [labs] Processing lab.csv...")
        
        # Build eICU labname -> CLIF category map
        lab_map = {}
        for row in self.labs_map.iter_rows(named=True):
            eicu_ids = row.get("eicu_ids", "")
            if eicu_ids and str(eicu_ids) != "nan":
                lab_map[eicu_ids.lower()] = row["lab_category"]
        
        # Read and transform
        df = pl.scan_csv(lab_file).select([
            "patientunitstayid",
            "labresultoffset", 
            "labname",
            "labresult"
        ]).collect()
        
        # Map to CLIF categories
        df = df.with_columns(
            pl.col("labname").str.to_lowercase().replace(lab_map).alias("lab_category")
        )
        
        # Rename to CLIF schema
        df_clif = df.rename({
            "patientunitstayid": "hospitalization_id",
            "labresultoffset": "lab_collect_dttm_offset_min",
            "labresult": "lab_value"
        }).select([
            "hospitalization_id",
            "lab_collect_dttm_offset_min",
            "lab_category", 
            "lab_value"
        ]).filter(pl.col("lab_value").is_not_null())
        
        output_file = self.output_path / "labs.parquet"
        df_clif.write_parquet(output_file)
        print(f"    ✓ Wrote {len(df_clif):,} rows to {output_file.name}")
    
    # =========================================================================
    # SICdb ETL  
    # =========================================================================
    def etl_sic(self):
        """ETL for SICdb (Salzburg ICU Database)."""
        print("\n[SICdb] Starting ETL...")
        
        # SICdb uses DataID for item identification
        # Main tables: data_float_h (vitals), laboratory
        self._etl_sic_vitals()
        self._etl_sic_labs()
    
    def _etl_sic_vitals(self):
        """Transform SICdb data_float_h to CLIF vitals."""
        vital_file = self.data_path / "data_float_h.csv"
        if not vital_file.exists():
            # Try parquet
            vital_file = self.data_path / "data_float_h.parquet"
        if not vital_file.exists():
            print("  [vitals] Skipping - data_float_h not found")
            return
        
        print(f"  [vitals] Processing {vital_file.name}...")
        
        # Build DataID -> CLIF category map from mappings
        id_map = {}
        for row in self.vitals_map.iter_rows(named=True):
            sic_ids = row.get("sic_ids", "")
            if sic_ids and str(sic_ids) != "nan" and str(sic_ids).strip():
                for sid in str(sic_ids).split(";"):
                    sid = sid.strip()
                    if sid.isdigit():
                        id_map[int(sid)] = row["vital_category"]
        
        print(f"    Mapping {len(id_map)} SIC DataIDs to CLIF categories")
        
        # Read source
        if vital_file.suffix == ".parquet":
            df = pl.scan_parquet(vital_file)
        else:
            df = pl.scan_csv(vital_file)
        
        # Filter to mapped IDs and transform
        mapped_ids = list(id_map.keys())
        df = df.filter(pl.col("DataID").is_in(mapped_ids)).collect()
        
        if len(df) == 0:
            print("    ⚠️  No matching DataIDs found")
            return
        
        # Map to CLIF categories
        df = df.with_columns(
            pl.col("DataID").replace(id_map).alias("vital_category")
        )
        
        # Rename to CLIF schema (SICdb uses CaseID for encounter)
        df_clif = df.rename({
            "CaseID": "hospitalization_id",
            "Offset": "recorded_dttm_offset_min",
            "Val": "vital_value"
        }).select([
            "hospitalization_id",
            "recorded_dttm_offset_min",
            "vital_category",
            "vital_value"
        ]).filter(pl.col("vital_value").is_not_null())
        
        output_file = self.output_path / "vitals.parquet"
        df_clif.write_parquet(output_file)
        print(f"    ✓ Wrote {len(df_clif):,} rows to {output_file.name}")
    
    def _etl_sic_labs(self):
        """Transform SICdb laboratory to CLIF labs."""
        lab_file = self.data_path / "laboratory.csv"
        if not lab_file.exists():
            lab_file = self.data_path / "laboratory.parquet"
        if not lab_file.exists():
            print("  [labs] Skipping - laboratory not found")
            return
        
        print(f"  [labs] Processing {lab_file.name}...")
        
        # Build LaboratoryID -> CLIF category map
        id_map = {}
        for row in self.labs_map.iter_rows(named=True):
            sic_ids = row.get("sic_ids", "")
            if sic_ids and str(sic_ids) != "nan" and str(sic_ids).strip():
                for sid in str(sic_ids).split(";"):
                    sid = sid.strip()
                    if sid.isdigit():
                        id_map[int(sid)] = row["lab_category"]
        
        print(f"    Mapping {len(id_map)} SIC LaboratoryIDs to CLIF categories")
        
        # Read source
        if lab_file.suffix == ".parquet":
            df = pl.scan_parquet(lab_file)
        else:
            df = pl.scan_csv(lab_file)
        
        mapped_ids = list(id_map.keys())
        df = df.filter(pl.col("LaboratoryID").is_in(mapped_ids)).collect()
        
        if len(df) == 0:
            print("    ⚠️  No matching LaboratoryIDs found")
            return
        
        df = df.with_columns(
            pl.col("LaboratoryID").replace(id_map).alias("lab_category")
        )
        
        df_clif = df.rename({
            "CaseID": "hospitalization_id",
            "Offset": "lab_collect_dttm_offset_min",
            "LaboratoryValue": "lab_value"
        }).select([
            "hospitalization_id",
            "lab_collect_dttm_offset_min",
            "lab_category",
            "lab_value"
        ]).filter(pl.col("lab_value").is_not_null())
        
        output_file = self.output_path / "labs.parquet"
        df_clif.write_parquet(output_file)
        print(f"    ✓ Wrote {len(df_clif):,} rows to {output_file.name}")
    
    # =========================================================================
    # HiRID ETL
    # =========================================================================
    def etl_hirid(self):
        """ETL for HiRID dataset."""
        print("\n[HiRID] Starting ETL...")
        
        # HiRID uses variableid for item identification
        # Main tables: observations, pharma
        self._etl_hirid_vitals()
        self._etl_hirid_labs()
    
    def _etl_hirid_vitals(self):
        """Transform HiRID observations to CLIF vitals."""
        obs_dir = self.data_path / "observation_tables"
        if not obs_dir.exists():
            obs_dir = self.data_path / "observations"
        if not obs_dir.exists():
            # Try single file
            obs_file = self.data_path / "observations.parquet"
            if not obs_file.exists():
                print("  [vitals] Skipping - observations not found")
                return
        
        print("  [vitals] Processing observations...")
        
        # Build variableid -> CLIF category map
        id_map = {}
        for row in self.vitals_map.iter_rows(named=True):
            hirid_ids = row.get("hirid_ids", "")
            if hirid_ids and str(hirid_ids) != "nan" and str(hirid_ids).strip():
                for hid in str(hirid_ids).split(";"):
                    hid = hid.strip()
                    if hid.isdigit():
                        id_map[int(hid)] = row["vital_category"]
        
        # Also add lab mappings since HiRID puts labs in observations
        for row in self.labs_map.iter_rows(named=True):
            hirid_ids = row.get("hirid_ids", "")
            if hirid_ids and str(hirid_ids) != "nan" and str(hirid_ids).strip():
                for hid in str(hirid_ids).split(";"):
                    hid = hid.strip()
                    if hid.isdigit():
                        id_map[int(hid)] = row["lab_category"]
        
        print(f"    Mapping {len(id_map)} HiRID variableids")
        
        # Read observations (may be partitioned)
        if obs_dir.is_dir():
            parquet_files = list(obs_dir.glob("*.parquet"))
            if parquet_files:
                df = pl.scan_parquet(parquet_files)
            else:
                csv_files = list(obs_dir.glob("*.csv"))
                df = pl.scan_csv(csv_files)
        else:
            df = pl.scan_parquet(obs_file)
        
        mapped_ids = list(id_map.keys())
        df = df.filter(pl.col("variableid").is_in(mapped_ids)).collect()
        
        if len(df) == 0:
            print("    ⚠️  No matching variableids found")
            return
        
        df = df.with_columns(
            pl.col("variableid").replace(id_map).alias("vital_category")
        )
        
        df_clif = df.rename({
            "patientid": "hospitalization_id",
            "datetime": "recorded_dttm",
            "value": "vital_value"
        }).select([
            "hospitalization_id",
            "recorded_dttm",
            "vital_category",
            "vital_value"
        ]).filter(pl.col("vital_value").is_not_null())
        
        output_file = self.output_path / "vitals.parquet"
        df_clif.write_parquet(output_file)
        print(f"    ✓ Wrote {len(df_clif):,} rows to {output_file.name}")
    
    def _etl_hirid_labs(self):
        """HiRID labs are in observations - handled by vitals ETL."""
        print("  [labs] Labs included in observations processing")
    
    # =========================================================================
    # AmsterdamUMCdb ETL
    # =========================================================================
    def etl_aumc(self):
        """ETL for AmsterdamUMCdb dataset."""
        print("\n[AUMC] Starting ETL...")
        
        # AUMC uses itemid for item identification
        # Main tables: numericitems, drugitems, listitems
        self._etl_aumc_vitals()
        self._etl_aumc_labs()
    
    def _etl_aumc_vitals(self):
        """Transform AUMC numericitems to CLIF vitals."""
        vital_file = self.data_path / "numericitems.csv"
        if not vital_file.exists():
            vital_file = self.data_path / "numericitems.parquet"
        if not vital_file.exists():
            print("  [vitals] Skipping - numericitems not found")
            return
        
        print(f"  [vitals] Processing {vital_file.name}...")
        
        # Build itemid -> CLIF category map
        id_map = {}
        for row in self.vitals_map.iter_rows(named=True):
            aumc_ids = row.get("aumc_ids", "")
            if aumc_ids and str(aumc_ids) != "nan" and str(aumc_ids).strip():
                for aid in str(aumc_ids).split(";"):
                    aid = aid.strip()
                    if aid.isdigit():
                        id_map[int(aid)] = row["vital_category"]
        
        # Add lab mappings
        for row in self.labs_map.iter_rows(named=True):
            aumc_ids = row.get("aumc_ids", "")
            if aumc_ids and str(aumc_ids) != "nan" and str(aumc_ids).strip():
                for aid in str(aumc_ids).split(";"):
                    aid = aid.strip()
                    if aid.isdigit():
                        id_map[int(aid)] = row["lab_category"]
        
        print(f"    Mapping {len(id_map)} AUMC itemids")
        
        if vital_file.suffix == ".parquet":
            df = pl.scan_parquet(vital_file)
        else:
            df = pl.scan_csv(vital_file)
        
        mapped_ids = list(id_map.keys())
        df = df.filter(pl.col("itemid").is_in(mapped_ids)).collect()
        
        if len(df) == 0:
            print("    ⚠️  No matching itemids found")
            return
        
        df = df.with_columns(
            pl.col("itemid").replace(id_map).alias("vital_category")
        )
        
        df_clif = df.rename({
            "admissionid": "hospitalization_id",
            "measuredat": "recorded_dttm",
            "value": "vital_value"
        }).select([
            "hospitalization_id",
            "recorded_dttm",
            "vital_category",
            "vital_value"
        ]).filter(pl.col("vital_value").is_not_null())
        
        output_file = self.output_path / "vitals.parquet"
        df_clif.write_parquet(output_file)
        print(f"    ✓ Wrote {len(df_clif):,} rows to {output_file.name}")
    
    def _etl_aumc_labs(self):
        """AUMC labs are in numericitems - handled by vitals ETL."""
        print("  [labs] Labs included in numericitems processing")


def main():
    parser = argparse.ArgumentParser(
        description="OpenCLIF ETL - Convert open ICU datasets to CLIF format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run main.py --source eicu --path /data/eicu
  uv run main.py --source sic --path /data/sicdb
  uv run main.py --source hirid --path /data/hirid
  uv run main.py --source aumc --path /data/amsterdamumcdb
        """
    )
    
    parser.add_argument(
        "--source", "-s",
        required=True,
        choices=SUPPORTED_SOURCES,
        help="Source dataset name"
    )
    
    parser.add_argument(
        "--path", "-p",
        required=True,
        type=Path,
        help="Path to source dataset directory"
    )
    
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=None,
        help="Output directory (default: <path>/clif_output)"
    )
    
    args = parser.parse_args()
    
    # Validate path exists
    if not args.path.exists():
        print(f"❌ Error: Path does not exist: {args.path}")
        return 1
    
    # Run ETL
    etl = OpenCLIFETL(
        source=args.source,
        data_path=args.path,
        output_path=args.output
    )
    etl.run()
    
    return 0


if __name__ == "__main__":
    exit(main())
