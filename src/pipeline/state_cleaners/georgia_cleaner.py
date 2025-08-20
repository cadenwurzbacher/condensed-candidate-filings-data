#!/usr/bin/env python3
"""
Georgia State Data Cleaner

This module cleans and standardizes Georgia political candidate data
to conform to the unified schema used by other state cleaners (Alaska baseline).
"""

import os
import re
import logging
from datetime import datetime
from typing import List, Optional, Tuple

import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
DEFAULT_OUTPUT_DIR = "data/processed"  # Default output directory for cleaned data
DEFAULT_INPUT_DIR = "Raw State Data - Current"  # Default input directory

def list_available_input_files(input_dir: str = DEFAULT_INPUT_DIR) -> List[str]:
    """
    List all available Excel files for Georgia in the input directory.

    Args:
        input_dir: Directory to search for input files

    Returns:
        List of available Excel file paths
    """
    if not os.path.exists(input_dir):
        logger.warning(f"Input directory {input_dir} does not exist")
        return []

    excel_files: List[str] = []
    for file in os.listdir(input_dir):
        if file.endswith((".xlsx", ".xls")) and not file.startswith("~$") and "georgia" in file.lower():
            excel_files.append(os.path.join(input_dir, file))

    return sorted(excel_files)

class GeorgiaCleaner:
    """Handles cleaning and standardization of Georgia political candidate data."""

    def __init__(self, output_dir: str = DEFAULT_OUTPUT_DIR):
        self.state_name = "Georgia"
        self.output_dir = output_dir

        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")

    # -----------------------------
    # Public API
    # -----------------------------
    def clean_georgia_data(self, df: pd.DataFrame, source_filename: Optional[str] = None) -> pd.DataFrame:
        """
        Clean and standardize Georgia candidate data according to final schema.

        Args:
            df: Raw Georgia candidate data DataFrame
            source_filename: Optional original filename for extracting election year/type

        Returns:
            Cleaned DataFrame conforming to the final schema (Alaska baseline order)
        """
        logger.info(f"Starting Georgia data cleaning for {len(df)} records...")

        cleaned_df = df.copy()

        # Step 1: Normalize source columns to a common set
        cleaned_df = self._normalize_source_columns(cleaned_df)

        # Step 2: Election metadata
        cleaned_df = self._process_election_data(cleaned_df, source_filename)

        # Step 3: Office and district
        cleaned_df = self._process_office_and_district(cleaned_df)

        # Step 4: Candidate names
        cleaned_df = self._process_full_name_displays(cleaned_df)

        # Step 5: Party normalization
        cleaned_df = self._standardize_parties(cleaned_df)

        # Step 6: Contact info
        cleaned_df = self._clean_contact_info(cleaned_df)

        # Step 7: Required columns
        cleaned_df = self._add_required_columns(cleaned_df)

        # Step 8: Remove duplicate/raw columns that were superseded
        cleaned_df = self._remove_duplicate_columns(cleaned_df)

        # Step 9: Reorder to match Alaska baseline
        cleaned_df = self._reorder_columns(cleaned_df)

        # Ensure district column is string/object and not coerced to numeric
        if "district" in cleaned_df.columns:
            cleaned_df["district"] = cleaned_df["district"].astype("object")

        logger.info(f"Georgia data cleaning completed. Final shape: {cleaned_df.shape}")
        return cleaned_df

    # -----------------------------
    # Normalization helpers
    # -----------------------------
    def _normalize_source_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map commonly varying Georgia column names to a consistent set used downstream.

        Target normalized raw columns prior to derivation:
        - raw_name
        - raw_office
        - raw_district
        - raw_party
        - raw_election
        - raw_year
        - raw_filing_date
        - raw_phone
        - raw_email
        - raw_address
        - raw_website
        """
        logger.info("Normalizing source columns...")

        def pick(df_: pd.DataFrame, candidates: List[str]) -> Optional[str]:
            for c in candidates:
                if c in df_.columns:
                    return c
            # case-insensitive match fall-back
            lower_map = {c.lower(): c for c in df_.columns}
            for c in candidates:
                if c.lower() in lower_map:
                    return lower_map[c.lower()]
            return None

        name_col = pick(
            df,
            [
                "Name",
                "Candidate Name",
                "Candidate",
                "Candidate_Name",
                "name",
            ],
        )
        office_col = pick(
            df,
            [
                "Office Name",      # Georgia's actual column name
                "Office",
                "Office Sought",
                "Office Sought Name",
                "office",
            ],
        )
        district_col = pick(df, ["District", "district", "Office District", "Seat", "Seat/District"])
        party_col = pick(df, ["Candidate Party", "Party", "party", "Party Affiliation", "Party Name", "Party Abbreviation"])
        election_col = pick(df, ["Election", "Election Name", "Election Description", "Election_Type", "Election Date Name"])  # free text
        year_col = pick(df, ["Year", "Election Year", "year"])  # numeric
        filing_date_col = pick(df, ["Date Filed", "Filing Date", "filed_date", "Filing_Date"])
        phone_col = pick(df, ["Phone", "Phone Number", "phone", "PhoneNumber"])
        email_col = pick(df, ["Email", "E-mail", "email"])
        address_col = pick(df, ["Address", "Mailing Address", "address", "Residence Address"])
        website_col = pick(df, ["Website", "URL", "Website URL", "website"])

        # Attach normalized raw columns (do not drop originals yet)
        df["raw_name"] = df[name_col] if name_col else None
        df["raw_office"] = df[office_col] if office_col else None
        df["raw_district"] = df[district_col] if district_col else None
        df["raw_party"] = df[party_col] if party_col else None
        df["raw_election"] = df[election_col] if election_col else None
        df["raw_year"] = df[year_col] if year_col else None
        df["raw_filing_date"] = df[filing_date_col] if filing_date_col else None
        df["raw_phone"] = df[phone_col] if phone_col else None
        df["raw_email"] = df[email_col] if email_col else None
        df["raw_address"] = df[address_col] if address_col else None
        df["raw_website"] = df[website_col] if website_col else None

        return df

    # -----------------------------
    # Election data
    # -----------------------------
    def _process_election_data(self, df: pd.DataFrame, filename: Optional[str]) -> pd.DataFrame:
        logger.info("Processing election data...")

        def infer_year_from_text(text: Optional[str]) -> Optional[int]:
            if text is None or pd.isna(text):
                return None
            match = re.search(r"(20\d{2})", str(text))
            return int(match.group(1)) if match else None

        # Election year priority: raw_year col -> raw_election text -> filename
        years: List[Optional[int]] = []
        for _, row in df.iterrows():
            year_val = None
            if pd.notna(row.get("raw_year")):
                try:
                    year_val = int(str(row.get("raw_year")).strip().split(".")[0])
                except Exception:
                    year_val = None
            if year_val is None:
                year_val = infer_year_from_text(row.get("raw_election"))
            years.append(year_val)

        # Fallback: if most are None, try filename
        file_year = None
        if filename:
            m = re.search(r"(20\d{2})", filename)
            if m:
                file_year = int(m.group(1))

        df["election_year"] = [y if y is not None else file_year for y in years]

        # Election type: derive from raw_election text or filename; default General
        def infer_type(texts: List[str]) -> str:
            joined = " ".join([t for t in texts if t])
            l = joined.lower()
            if "primary" in l or "presidential" in l:
                return "Primary"
            if "special" in l:
                return "Special"
            if "general" in l:
                return "General"
            # Georgia municipal/county races are generally nonpartisan on ballot; still mark as General unless stated
            return "General"

        df["election_type"] = df.apply(
            lambda r: infer_type([str(r.get("raw_election")) if pd.notna(r.get("raw_election")) else "", filename or ""]),
            axis=1,
        )

        # Attempt to parse a date from raw_election text (MM/DD/YYYY etc.)
        def parse_date_from_text(text: Optional[str]) -> Optional[str]:
            if text is None or pd.isna(text):
                return None
            s = str(text)
            date_patterns = [
                r"(\d{1,2})/(\d{1,2})/(\d{4})",
                r"(\d{1,2})-(\d{1,2})-(\d{4})",
                r"(\d{4})-(\d{1,2})-(\d{1,2})",
            ]
            for pat in date_patterns:
                m = re.search(pat, s)
                if m:
                    # Standardize to YYYY-MM-DD
                    if pat.startswith("("):  # MM/DD/YYYY or MM-DD-YYYY
                        a, b, c = m.groups()
                        if len(c) == 4:
                            return f"{int(c):04d}-{int(a):02d}-{int(b):02d}"
                    else:
                        y, mo, d = m.groups()
                        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
            return None

        df["election_date"] = df["raw_election"].apply(parse_date_from_text)

        # Ensure election_year is Int64 nullable type for consistency
        df["election_year"] = pd.Series(df["election_year"]).astype("Int64")

        return df

    # -----------------------------
    # Office and district
    # -----------------------------
    def _process_office_and_district(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Processing office and district information...")

        def extract_office_and_district(office_val: Optional[str], district_val: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
            if office_val is None or (isinstance(office_val, float) and pd.isna(office_val)):
                office_str = ""
            else:
                office_str = str(office_val).strip()

            district_str = None
            if district_val is not None and not (isinstance(district_val, float) and pd.isna(district_val)):
                district_str = str(district_val).strip()

            s_upper = office_str.upper()

            # Federal
            if "PRESIDENT" in s_upper and "VICE" not in s_upper:
                return "US President", None
            if ("REPRESENTATIVE" in s_upper and "CONGRESS" in s_upper) or "U.S. HOUSE" in s_upper or "US HOUSE" in s_upper:
                # Try district from office text
                m = re.search(r"(CONGRESSIONAL\s+DISTRICT|DISTRICT)\s+([A-Za-z0-9-]+)", office_str, flags=re.IGNORECASE)
                if m:
                    return "US Representative", m.group(2)
                if district_str:
                    return "US Representative", district_str
                return "US Representative", None
            if "SENATOR" in s_upper and ("U.S" in s_upper or "US " in s_upper):
                return "US Senator", None

            # State legislature
            if ("STATE SENATE" in s_upper) or re.search(r"SENATE\s+DIST(RICT)?", s_upper):
                m = re.search(r"DIST(RICT)?\s+([A-Za-z0-9-]+)", office_str, flags=re.IGNORECASE)
                return "State Senator", (m.group(2) if m else (district_str or None))

            if ("STATE HOUSE" in s_upper) or ("STATE REPRESENTATIVE" in s_upper) or re.search(r"HOUSE\s+DIST(RICT)?", s_upper):
                m = re.search(r"DIST(RICT)?\s+([A-Za-z0-9-]+)", office_str, flags=re.IGNORECASE)
                return "State Representative", (m.group(2) if m else (district_str or None))

            # Public Service Commission
            if "PUBLIC SERVICE COMMISSION" in s_upper or "PSC" in s_upper:
                m = re.search(r"DIST(RICT)?\s+([A-Za-z0-9-]+)", office_str, flags=re.IGNORECASE)
                return "Public Service Commissioner", (m.group(2) if m else (district_str or None))

            # County/City at-large and district references
            if re.search(r"AT[-\s]?LARGE", s_upper):
                return office_str.title(), "At-Large"

            # Generic district extraction if not captured
            if district_str is None:
                m = re.search(r"DIST(RICT)?\s+([A-Za-z0-9-]+)", office_str, flags=re.IGNORECASE)
                if m:
                    district_str = m.group(2)

            # Default: title case office, district as is
            return (office_str.title() if office_str else None), (district_str or None)

        results = df.apply(lambda r: extract_office_and_district(r.get("raw_office"), r.get("raw_district")), axis=1)
        df["office"] = [o for o, _ in results]
        df["district"] = [d for _, d in results]

        # Clean up district datatype and strings
        df["district"] = df["district"].apply(lambda x: None if (pd.isna(x) or x == "") else str(x))

        return df

    # -----------------------------
    # Names
    # -----------------------------
    def _process_full_name_displays(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Processing candidate names...")

        def clean_name(name_str: Optional[str]) -> Optional[str]:
            if name_str is None or pd.isna(name_str):
                return None
            s = str(name_str).strip().strip("\"'")
            s = re.sub(r"\s+", " ", s)
            return s

        df["full_name_display"] = df["raw_name"].apply(clean_name)

        # Parse components
        df = self._parse_names(df)
        return df

    def _parse_names(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Parsing candidate names...")

        df["first_name"] = pd.NA
        df["middle_name"] = pd.NA
        df["last_name"] = pd.NA
        df["prefix"] = pd.NA
        df["suffix"] = pd.NA
        df["nickname"] = pd.NA
        # Don't overwrite full_name_display - it was already set above

        for idx, row in df.iterrows():
            name = row.get("full_name_display")
            original_name = row.get("raw_name")

            if pd.isna(name) or not name:
                continue

            parsed = self._parse_standard_name(name, original_name)
            df.at[idx, "first_name"] = parsed[0]
            df.at[idx, "middle_name"] = parsed[1]
            df.at[idx, "last_name"] = parsed[2]
            df.at[idx, "prefix"] = parsed[3]
            df.at[idx, "suffix"] = parsed[4]
            df.at[idx, "nickname"] = parsed[5]
            df.at[idx, "full_name_display"] = parsed[6]

        return df

    def _parse_standard_name(
        self, name: str, original_name: Optional[str]
    ) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Parse name into components. Handles Last, First Middle and First Middle Last formats."""
        first_name: Optional[str] = None
        middle_name: Optional[str] = None
        last_name: Optional[str] = None
        prefix: Optional[str] = None
        suffix: Optional[str] = None
        nickname: Optional[str] = None
        display_name: Optional[str] = None

        if pd.isna(name) or not name:
            return None, None, None, None, None, None, None

        name = str(name).strip().strip("\"'")
        name = re.sub(r"\s+", " ", name)

        # Extract nickname from original
        if pd.notna(original_name):
            original_str = str(original_name)
            m = re.search(r'["""\u201c\u201d]([^"""\u201c\u201d]+)["""\u201c\u201d]', original_str)
            if m:
                nickname = m.group(1)
                name = re.sub(r'["""\u201c\u201d][^"""\u201c\u201d]+["""\u201c\u201d]', '', original_str)
                name = re.sub(r"\s+", " ", name).strip()

        # Extract suffix
        suffix_pattern = r"\b(Jr\.?|Sr\.?|II|III|IV|V|VI|VII|VIII|IX|X)\b"
        m = re.search(suffix_pattern, name, flags=re.IGNORECASE)
        if m:
            suffix = m.group(1)
            name = re.sub(suffix_pattern, '', name, flags=re.IGNORECASE).strip()
            name = re.sub(r"\.$", "", name).strip()

        # With comma => Last, First Middle
        if "," in name:
            parts = [p.strip() for p in name.split(",")]
            if len(parts) >= 2:
                last_name = parts[0]
                first_middle = parts[1].split()
                if len(first_middle) == 1:
                    first_name = first_middle[0]
                elif len(first_middle) >= 2:
                    first_name = first_middle[0]
                    middle_name = " ".join(first_middle[1:])
                display_name = self._build_display_name(first_name, middle_name, last_name, suffix, nickname)
                return first_name, middle_name, last_name, prefix, suffix, nickname, display_name

        # Space separated
        parts = name.split()
        if len(parts) == 1:
            return parts[0], None, None, None, suffix, nickname, parts[0]
        if len(parts) == 2:
            return parts[0], None, parts[1], None, suffix, nickname, f"{parts[0]} {parts[1]}"
        # 3+ parts: first, middle..., last
        first_name = parts[0]
        last_name = parts[-1]
        middle_name = " ".join(parts[1:-1]) if len(parts) > 2 else None
        display_name = self._build_display_name(first_name, middle_name, last_name, suffix, nickname)
        return first_name, middle_name, last_name, prefix, suffix, nickname, display_name

    def _build_display_name(
        self, first_name: Optional[str], middle_name: Optional[str], last_name: Optional[str], suffix: Optional[str], nickname: Optional[str]
    ) -> str:
        if not first_name:
            return ""
        parts: List[str] = [first_name]
        if nickname:
            parts.append(f'"{nickname}"')
        if middle_name:
            parts.append(middle_name)
        if last_name:
            parts.append(last_name)
        if suffix:
            parts.append(suffix)
        return " ".join(parts).strip()

    # -----------------------------
    # Party
    # -----------------------------
    def _standardize_parties(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Standardizing party names...")

        party_mapping = {
            # Common
            "democrat": "Democratic",
            "democratic": "Democratic",
            "democratic party": "Democratic",
            "republican": "Republican",
            "republican party": "Republican",
            "independent": "Independent",
            "libertarian": "Libertarian",
            "green": "Green",
            "constitution": "Constitution",
            "nonpartisan": "Nonpartisan",
            "non-partisan": "Nonpartisan",
            "np": "Nonpartisan",
            "npa": "Nonpartisan",
            "unaffiliated": "Unaffiliated",
            "write-in": "Write-in",
        }

        def standardize_party(party_val: Optional[str]) -> Optional[str]:
            if party_val is None or pd.isna(party_val):
                return None
            pl = str(party_val).strip().lower()
            return party_mapping.get(pl, party_val)

        df["party"] = df["raw_party"].apply(standardize_party)
        return df

    # -----------------------------
    # Contact info
    # -----------------------------
    def _clean_contact_info(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Cleaning contact information...")

        def clean_phone(phone_str: Optional[str]) -> Optional[str]:
            if phone_str is None or pd.isna(phone_str):
                return None
            digits = re.sub(r"[^\d]", "", str(phone_str))
            if len(digits) == 10:
                return digits
            if len(digits) == 11 and digits.startswith("1"):
                return digits[1:]
            return None

        def clean_email(email_str: Optional[str]) -> Optional[str]:
            if email_str is None or pd.isna(email_str):
                return None
            email = str(email_str).strip().lower()
            if "@" in email and "." in email.split("@")[-1]:
                return email
            return None

        def clean_address(address_str: Optional[str]) -> Optional[str]:
            if address_str is None or pd.isna(address_str):
                return None
            s = str(address_str).strip().strip("\"'")
            s = re.sub(r"\s+", " ", s)
            return s or None

        def clean_website(website_str: Optional[str]) -> Optional[str]:
            if website_str is None or pd.isna(website_str):
                return None
            s = str(website_str).strip()
            return s if s and s.lower() != "nan" else None

        df["phone"] = df["raw_phone"].apply(clean_phone)
        df["email"] = df["raw_email"].apply(clean_email)
        
        # Combine address components into single address field
        def combine_address(row):
            parts = []
            if pd.notna(row.get('Street Number')) and pd.notna(row.get('Street Name')):
                parts.append(f"{row['Street Number']} {row['Street Name']}")
            elif pd.notna(row.get('Street Name')):
                parts.append(str(row['Street Name']))
            
            if pd.notna(row.get('City')):
                parts.append(str(row['City']))
            
            if pd.notna(row.get('State')):
                parts.append(str(row['State']))
            
            if pd.notna(row.get('Zip')):
                parts.append(str(row['Zip']))
            
            return ', '.join(parts) if parts else None
        
        df["address"] = df.apply(combine_address, axis=1)
        
        # Derive address_state from explicit State column if present; else parse from address
        def extract_state_from_row(row) -> Optional[str]:
            state_val = row.get('State') if 'State' in row.index else None
            if pd.notna(state_val) and str(state_val).strip():
                s = str(state_val).strip().upper()
                if re.fullmatch(r"[A-Z]{2}", s):
                    return s
            addr = row.get('address')
            if addr is None or pd.isna(addr):
                return None
            text = str(addr)
            m = re.search(r"\b([A-Z]{2})\s+\d{5}(?:-\d{4})?\b", text)
            return m.group(1) if m else None
        
        df['address_state'] = df.apply(extract_state_from_row, axis=1)
        
        df["website"] = df["raw_website"].apply(clean_website)

        return df

    # -----------------------------
    # Schema and housekeeping
    # -----------------------------
    def _add_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Adding required columns...")

        df["state"] = self.state_name

        # Preserve originals similar to other cleaners
        df["original_name"] = df["raw_name"].copy()
        df["original_state"] = df["state"].copy()
        df["original_election_year"] = df["election_year"].copy()
        df["original_office"] = df["raw_office"].copy()
        df["original_filing_date"] = df["raw_filing_date"].copy() if "raw_filing_date" in df.columns else None

        # Map address components to required columns
        if 'City' in df.columns:
            df['city'] = df['City'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        else:
            
            
            
            
            df['city'] = pd.NA
            
        if 'Zip' in df.columns:
            df['zip_code'] = df['Zip'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        else:
            
            
            
            
            df['zip_code'] = pd.NA
            
        if 'County (If Local Contest)' in df.columns:
            df['county'] = df['County (If Local Contest)'].apply(lambda x: str(x).strip() if pd.notna(x) else None)
        else:
            
            
            
            
            df['county'] = pd.NA
            
        # Other required but often absent columns
        required_missing = [
            "filing_date",
            "facebook",
            "twitter",
            "address_state",
        ]
        for c in required_missing:
            if c not in df.columns:
                df[c] = pd.NA

        # Map filing date if we captured it
        if "raw_filing_date" in df.columns:
            df["filing_date"] = df["raw_filing_date"].astype("string").replace({"<NA>": None, "nan": None})

        # Identifiers (filled later in the pipeline)
        df["id"] = ""
        df["stable_id"] = ""

        return df

    def _remove_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Removing duplicate/raw columns...")
        columns_to_remove = [
            # Raw staging columns
            "raw_name",
            "raw_office",
            "raw_district",
            "raw_party",
            "raw_election",
            "raw_year",
            "raw_filing_date",
            "raw_phone",
            "raw_email",
            "raw_address",
            "raw_website",
        ]
        to_drop = [c for c in columns_to_remove if c in df.columns]
        if to_drop:
            df = df.drop(columns=to_drop)
            logger.info(f"Dropped {len(to_drop)} normalized staging columns")
        return df

    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Reordering columns to match Alaska baseline schema...")
        alaska_order = [
            'election_year',
            'election_type',
            'office',
            'district',
            'full_name_display',
            'first_name',
            'middle_name',
            'last_name',
            'prefix',
            'suffix',
            'nickname',
            'party',
            'phone',
            'email',
            'address',
            'website',
            'state',
            'address_state',
            'original_name',
            'original_state',
            'original_election_year',
            'original_office',
            'original_filing_date',
            'id',
            'stable_id',
            'county',
            'city',
            'zip_code',
            'filing_date',
            'election_date',
            'facebook',
            'twitter'
        ]
        # Ensure all baseline columns exist
        for col in alaska_order:
            if col not in df.columns:
                df[col] = pd.NA
        # Return only baseline columns, dropping any extras
        return df[alaska_order]

def clean_georgia_candidates(input_file: str, output_file: Optional[str] = None, output_dir: str = DEFAULT_OUTPUT_DIR) -> pd.DataFrame:
    """
    Main function to clean Georgia candidate data.

    Args:
        input_file: Path to the input Excel file
        output_file: Optional path to save the cleaned data (if None, will use default naming in output_dir)
        output_dir: Directory to save cleaned data (default: "cleaned_data")

    Returns:
        Cleaned DataFrame
    """
    logger.info(f"Loading Georgia data from {input_file}...")
    df = pd.read_excel(input_file)

    cleaner = GeorgiaCleaner(output_dir=output_dir)
    cleaned_df = cleaner.clean_georgia_data(df, os.path.basename(input_file))

    # Output file naming
    if output_file is None:
        base = os.path.splitext(os.path.basename(input_file))[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"{base}_cleaned_{timestamp}.xlsx")

    if not os.path.dirname(output_file):
        output_file = os.path.join(output_dir, output_file)

    # Save both CSV (to preserve types) and Excel (for convenience)
    os.makedirs(output_dir, exist_ok=True)
    csv_output = output_file.replace(".xlsx", ".csv")
    cleaned_df.to_csv(csv_output, index=False)
    logger.info("Data saved as CSV to preserve data types")

    cleaned_df.to_excel(output_file, index=False)
    logger.info("Data also saved as Excel")

    # Optionally enforce key dtypes similar to other cleaners
    typed_df = cleaned_df.copy()
    if "election_year" in typed_df.columns:
        typed_df["election_year"] = typed_df["election_year"].astype("Int64")
    if "original_election_year" in typed_df.columns:
        typed_df["original_election_year"] = typed_df["original_election_year"].astype("Int64")
    for col in ["party", "id", "stable_id"]:
        if col in typed_df.columns:
            typed_df[col] = typed_df[col].astype("object")
    typed_df.to_csv(csv_output, index=False)

    return cleaned_df

if __name__ == "__main__":
    print("Available Georgia input files:")
    files = list_available_input_files()
    if files:
        for i, path in enumerate(files, 1):
            print(f"  {i}. {os.path.basename(path)}")
    else:
        print("  No Georgia Excel files found in input directory")
        raise SystemExit(1)

    # Example usage: process the first available file
    if files:
        input_path = files[0]
        out_dir = DEFAULT_OUTPUT_DIR
        print(f"\nProcessing: {os.path.basename(input_path)}")
        print(f"Output directory: {out_dir}")
        cleaned = clean_georgia_candidates(input_path, output_dir=out_dir)
        print(f"\nCleaned {len(cleaned)} records")
        print(f"Columns: {list(cleaned.columns)}")
        print(f"Data saved to: {out_dir}/")

