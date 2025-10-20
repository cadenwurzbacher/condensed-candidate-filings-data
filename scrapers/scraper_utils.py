"""
Utility functions for web scrapers to reduce code duplication.
"""

import os
import re
import pandas as pd
import openpyxl
from datetime import datetime


def ensure_raw_data_dir(subdir: str = 'raw') -> str:
    """
    Ensure the data output directory exists.

    Args:
        subdir: Subdirectory under 'data' (default: 'raw')

    Returns:
        Path to the created directory
    """
    dir_path = os.path.join('data', subdir)
    os.makedirs(dir_path, exist_ok=True)
    return dir_path


def save_to_formatted_excel(
    df: pd.DataFrame,
    state_name: str,
    output_dir: str = None,
    sheet_name: str = 'Candidates',
    include_timestamp: bool = True,
    header_color: str = '4472C4',
    auto_filter: bool = True,
    max_column_width: int = 50
) -> str:
    """
    Save DataFrame to an Excel file with standard formatting.

    This function provides consistent Excel output across all scrapers with:
    - Bold, colored headers
    - Auto-adjusted column widths
    - Optional auto-filters
    - Timestamped filenames

    Args:
        df: DataFrame to save
        state_name: Name of state (e.g., 'colorado', 'kansas')
        output_dir: Output directory (default: data/raw)
        sheet_name: Name of the Excel sheet
        include_timestamp: Whether to include timestamp in filename
        header_color: Hex color for header background (default: '4472C4' - blue)
        auto_filter: Whether to add auto-filter to headers
        max_column_width: Maximum column width in characters

    Returns:
        Path to the created Excel file
    """
    if output_dir is None:
        output_dir = ensure_raw_data_dir()

    # Create filename
    if include_timestamp:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        excel_file = f'{state_name}_candidates_{timestamp}.xlsx'
    else:
        excel_file = f'{state_name}_candidates.xlsx'

    excel_path = os.path.join(output_dir, excel_file)

    # Create Excel writer
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)

        # Get the workbook and worksheet
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]

        # Format headers
        header_font = openpyxl.styles.Font(bold=True, size=12)
        header_fill = openpyxl.styles.PatternFill(
            start_color=header_color,
            end_color=header_color,
            fill_type='solid'
        )
        header_font_color = openpyxl.styles.Font(color='FFFFFF', bold=True)

        for cell in worksheet[1]:
            cell.font = header_font_color
            cell.fill = header_fill

        # Auto-adjust column widths
        for column in worksheet.columns:
            max_length = 0
            column_letter = openpyxl.utils.get_column_letter(column[0].column)

            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass

            adjusted_width = min(max_length + 2, max_column_width)
            worksheet.column_dimensions[column_letter].width = adjusted_width

        # Add filters to headers
        if auto_filter:
            worksheet.auto_filter.ref = worksheet.dimensions

    return excel_path


def extract_district_from_office(office: str) -> str:
    """
    Extract district number from office name.

    Args:
        office: Office name (e.g., "State Senate District 5")

    Returns:
        District number as string, or None if not found
    """
    if not office:
        return None
    match = re.search(r'District (\d+)', office)
    return match.group(1) if match else None


def clean_phone_number(phone: str) -> str:
    """
    Clean and format a phone number.

    Args:
        phone: Raw phone number string

    Returns:
        Cleaned phone number or None
    """
    if pd.isna(phone) or not phone:
        return None

    phone = str(phone).strip()

    # Remove common prefixes
    phone = re.sub(r'^(Home:|Cell:|Phone:)\s*', '', phone, flags=re.IGNORECASE)

    # Remove non-digit characters
    digits = re.sub(r'\D', '', phone)

    # Format as XXX-XXX-XXXX if we have 10 digits
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits.startswith('1'):
        return f"+1-{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
    elif digits:
        return digits

    return None
