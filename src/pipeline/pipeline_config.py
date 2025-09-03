#!/usr/bin/env python3
"""
Pipeline Configuration for CandidateFilings.com Data Processing

This module provides centralized configuration for all pipeline toggles and options,
allowing flexible control over database operations, file outputs, and processing phases.
"""

from typing import Optional

class PipelineConfig:
    """
    Centralized configuration for all pipeline toggles and options.
    
    This class allows fine-grained control over:
    - Database operations (connection, upload, staging)
    - File outputs (structured, cleaned, final, audit reports)
    - Pipeline phases (enable/disable individual phases)
    - Data processing options (address parsing, standardization, etc.)
    - Performance and error handling settings
    """
    
    def __init__(self):
        """Initialize with default configuration (full pipeline enabled)."""
        
        # Database Options
        self.enable_database_connection = True
        self.enable_database_upload = True
        self.enable_staging_table = True
        self.enable_production_table = True
        self.enable_smart_staging = True
        
        # File Output Options
        self.save_structured_files = False      # Phase 1 output
        self.save_cleaned_files = False        # Phase 3 output  
        self.save_final_file = True            # Phase 5 output
        self.save_audit_reports = True         # Quality reports
        self.save_logs = True                  # Execution logs
        
        # Pipeline Phase Toggles
        self.enable_phase_1_structural = True
        self.enable_phase_2_id_generation = True
        self.enable_phase_3_state_cleaning = True
        self.enable_phase_4_national_standards = True
        self.enable_phase_5_final_processing = True
        
        # Data Processing Options
        self.enable_address_parsing = True
        self.enable_office_standardization = True
        self.enable_party_standardization = True
        self.enable_deduplication = True
        self.enable_data_audit = True
        
        # Performance Options
        self.enable_parallel_processing = False
        self.max_workers = 4
        self.chunk_size = 1000
        
        # Error Handling
        self.continue_on_state_error = True
        self.continue_on_phase_error = False
        self.retry_failed_states = True
        self.max_retries = 3
        
        # File Cleanup Options
        self.clear_intermediate_files = False
        
        # Data Directory Configuration
        self.data_dir = "data"
        self.raw_data_dir = "data/raw"
        self.structured_dir = "data/structured"
        self.cleaner_dir = "data/cleaner"
        self.final_dir = "data/final"
        self.processed_dir = "data/processed"
        self.reports_dir = "data/reports"
        self.logs_dir = "data/logs"
    
    def set_no_database_mode(self):
        """Configure for no-database operation."""
        self.enable_database_connection = False
        self.enable_database_upload = False
        self.enable_staging_table = False
        self.enable_production_table = False
        self.enable_smart_staging = False
    
    def set_debug_mode(self):
        """Configure for debug mode (save all intermediate files)."""
        self.save_structured_files = True
        self.save_cleaned_files = True
        self.save_audit_reports = True
        self.save_logs = True
        self.enable_data_audit = True
    
    def set_memory_only_mode(self):
        """Configure for memory-only processing (minimal file output)."""
        self.save_structured_files = False
        self.save_cleaned_files = False
        self.save_audit_reports = False
        self.save_final_file = True  # Keep final output
    
    def set_file_based_mode(self):
        """Configure for file-based processing (save all files)."""
        self.save_structured_files = True
        self.save_cleaned_files = True
        self.save_final_file = True
        self.save_audit_reports = True
    
    def get_enabled_phases(self) -> list:
        """Get list of enabled pipeline phases."""
        phases = []
        if self.enable_phase_1_structural:
            phases.append("Phase 1: Structural Parsing")
        if self.enable_phase_2_id_generation:
            phases.append("Phase 2: ID Generation")
        if self.enable_phase_3_state_cleaning:
            phases.append("Phase 3: State Cleaning")
        if self.enable_phase_4_national_standards:
            phases.append("Phase 4: National Standards")
        if self.enable_phase_5_final_processing:
            phases.append("Phase 5: Final Processing")
        return phases
    
    def get_database_status(self) -> dict:
        """Get database configuration status."""
        return {
            'connection_enabled': self.enable_database_connection,
            'upload_enabled': self.enable_database_upload,
            'staging_enabled': self.enable_staging_table,
            'production_enabled': self.enable_production_table,
            'smart_staging_enabled': self.enable_smart_staging
        }
    
    def get_file_output_status(self) -> dict:
        """Get file output configuration status."""
        return {
            'structured_files': self.save_structured_files,
            'cleaned_files': self.save_cleaned_files,
            'final_file': self.save_final_file,
            'audit_reports': self.save_audit_reports,
            'logs': self.save_logs
        }
    
    def __str__(self) -> str:
        """String representation of configuration."""
        enabled_phases = self.get_enabled_phases()
        db_status = self.get_database_status()
        file_status = self.get_file_output_status()
        
        return f"""
Pipeline Configuration:
======================
Enabled Phases: {len(enabled_phases)}/{5}
{chr(10).join(f"  {phase}" for phase in enabled_phases)}

Database: {'Enabled' if db_status['connection_enabled'] else 'Disabled'}
File Outputs: {sum(file_status.values())}/{len(file_status)} enabled
Data Processing: {sum([self.enable_address_parsing, self.enable_office_standardization, 
                      self.enable_party_standardization, self.enable_deduplication, 
                      self.enable_data_audit])}/5 enabled
        """
