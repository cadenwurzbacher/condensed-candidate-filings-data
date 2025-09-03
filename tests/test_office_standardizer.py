"""
Tests for the office standardizer component.
"""

import pytest
import pandas as pd
from pipeline.office_standardizer import OfficeStandardizer

class TestOfficeStandardizer:
    """Test the office name standardizer."""
    
    def test_standardizer_initialization(self):
        """Test that standardizer initializes correctly."""
        standardizer = OfficeStandardizer()
        
        assert standardizer.office_mappings is not None
        assert standardizer.standard_categories is not None
        assert len(standardizer.office_mappings) > 0
        assert len(standardizer.standard_categories) > 0
    
    def test_standard_categories_structure(self):
        """Test that standard categories have the expected structure."""
        standardizer = OfficeStandardizer()
        
        expected_categories = [
            'ALL_OFFICES', 'US_SENATE', 'US_HOUSE', 'GOVERNOR',
            'STATE_SENATE', 'STATE_HOUSE', 'STATE_ATTORNEY_GENERAL',
            'STATE_TREASURER', 'SECRETARY_OF_STATE', 'MAYOR',
            'CITY_COUNCIL', 'COUNTY_COMMISSION', 'SCHOOL_BOARD',
            'OTHER_LOCAL_OFFICE'
        ]
        
        for category in expected_categories:
            assert category in standardizer.standard_categories
    
    def test_office_name_standardization(self):
        """Test individual office name standardization."""
        standardizer = OfficeStandardizer()
        
        # Test federal offices
        result = standardizer.standardize_office_name("US Senator")
        assert result[0] == 'US_SENATE'
        assert result[1] == 1.0  # High confidence
        
        result = standardizer.standardize_office_name("United States Representative")
        assert result[0] == 'US_HOUSE'
        assert result[1] == 1.0
        
        # Test state offices
        result = standardizer.standardize_office_name("State Senator")
        assert result[0] == 'STATE_SENATE'
        assert result[1] == 1.0
        
        result = standardizer.standardize_office_name("Governor")
        assert result[0] == 'GOVERNOR'
        assert result[1] == 1.0
        
        # Test local offices
        result = standardizer.standardize_office_name("Mayor")
        assert result[0] == 'MAYOR'
        assert result[1] == 1.0
        
        result = standardizer.standardize_office_name("City Council Member")
        assert result[0] == 'CITY_COUNCIL'
        assert result[1] == 1.0
    
    def test_partial_matching(self):
        """Test partial matching for office names."""
        standardizer = OfficeStandardizer()
        
        # Test partial matches
        result = standardizer.standardize_office_name("Senator")
        assert result[0] in ['US_SENATE', 'STATE_SENATE']
        assert result[1] >= 0.8  # Should have high confidence
        
        result = standardizer.standardize_office_name("Representative")
        assert result[0] in ['US_HOUSE', 'STATE_HOUSE']
        assert result[1] >= 0.8
    
    def test_edge_cases(self):
        """Test edge cases and error handling."""
        standardizer = OfficeStandardizer()
        
        # Test empty/None values
        result = standardizer.standardize_office_name("")
        assert result[0] == 'UNKNOWN'
        assert result[1] == 0.0
        
        result = standardizer.standardize_office_name(None)
        assert result[0] == 'UNKNOWN'
        assert result[1] == 0.0
        
        # Test whitespace-only
        result = standardizer.standardize_office_name("   ")
        assert result[0] == 'UNKNOWN'
        assert result[1] == 0.0
    
    def test_dataset_standardization(self):
        """Test standardizing an entire dataset."""
        standardizer = OfficeStandardizer()
        
        # Create test dataset
        test_data = pd.DataFrame({
            'office': [
                'US Senator',
                'State Representative',
                'Mayor',
                'City Council Member',
                'Unknown Office'
            ]
        })
        
        # Standardize the dataset
        result_df = standardizer.standardize_dataset(test_data, 'office')
        
        # Check that new columns were added
        assert 'office_standardized' in result_df.columns
        assert 'office_confidence' in result_df.columns
        assert 'office_category' in result_df.columns
        
        # Check that some offices were standardized
        assert result_df['office_standardized'].notna().any()
        assert result_df['office_confidence'].notna().any()
        
        # Check that known offices have high confidence
        us_senate_row = result_df[result_df['office'] == 'US Senator'].iloc[0]
        assert us_senate_row['office_standardized'] == 'US_SENATE'
        assert us_senate_row['office_confidence'] == 1.0
    
    def test_confidence_scores(self):
        """Test that confidence scores are reasonable."""
        standardizer = OfficeStandardizer()
        
        # Test exact matches (should have confidence 1.0)
        result = standardizer.standardize_office_name("US Senator")
        assert result[1] == 1.0
        
        # Test partial matches (should have confidence 0.8+)
        result = standardizer.standardize_office_name("Senator")
        assert result[1] >= 0.8
        
        # Test low confidence cases
        result = standardizer.standardize_office_name("Some Random Office")
        assert result[1] <= 0.5  # Should have low confidence
    
    def test_category_mapping(self):
        """Test that standardized categories map to readable names."""
        standardizer = OfficeStandardizer()
        
        # Test a few key mappings
        assert standardizer.standard_categories['US_SENATE'] == 'U.S. Senate'
        assert standardizer.standard_categories['US_HOUSE'] == 'U.S. House'
        assert standardizer.standard_categories['GOVERNOR'] == 'Governor'
        assert standardizer.standard_categories['MAYOR'] == 'Mayor'
    
    def test_unknown_office_handling(self):
        """Test handling of unknown office types."""
        standardizer = OfficeStandardizer()
        
        # Test completely unknown office
        result = standardizer.standardize_office_name("Space Captain")
        assert result[0] == 'UNKNOWN' or result[1] <= 0.5
        
        # Test office with some recognizable words
        result = standardizer.standardize_office_name("Assistant Deputy Space Captain")
        # Should either be UNKNOWN or have low confidence
        assert result[0] == 'UNKNOWN' or result[1] <= 0.6
