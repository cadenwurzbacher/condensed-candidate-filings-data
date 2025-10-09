#!/usr/bin/env python3
"""
Smart Staging Manager

This module handles intelligent staging operations including:
- Automatic quality assessment
- Smart change detection
- Automated promotion decisions
- INSERT/UPDATE operations (NO automatic deletions)
"""

import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Any
from enum import Enum

logger = logging.getLogger(__name__)

class ChangeType(Enum):
    """Types of data changes"""
    INSERT = "insert"      # New record
    UPDATE = "update"      # Modified existing record
    NO_CHANGE = "no_change"  # No changes detected

class QualityLevel(Enum):
    """Data quality levels"""
    EXCELLENT = "excellent"    # > 98% - Auto-promote
    GOOD = "good"              # 95-98% - Auto-promote with warning
    ACCEPTABLE = "acceptable"  # 90-95% - Manual review recommended
    POOR = "poor"              # < 90% - Manual review required

class SmartStagingManager:
    """
    Manages intelligent staging operations with automated quality gates
    NO automatic deletions - only manual deletions allowed
    """
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.quality_thresholds = {
            'excellent': 0.98,
            'good': 0.95,
            'acceptable': 0.90,
            'poor': 0.00
        }
        
    def analyze_staging_data(self, staging_data: pd.DataFrame, production_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze staging data and determine appropriate actions
        
        Args:
            staging_data: New data from pipeline
            production_data: Existing production data
            
        Returns:
            Analysis results with recommendations
        """
        logger.info("Analyzing staging data for smart promotion decisions...")
        
        analysis = {
            'total_records': len(staging_data),
            'change_summary': {},
            'quality_score': 0.0,
            'quality_level': QualityLevel.POOR,
            'recommendation': 'manual_review',
            'auto_promote': False,
            'manual_review_needed': True,
            'changes': []
        }
        
        try:
            # 1. Detect changes (INSERT/UPDATE only - NO automatic deletions)
            changes = self._detect_changes(staging_data, production_data)
            analysis['changes'] = changes
            analysis['change_summary'] = self._summarize_changes(changes)
            
            # 2. Calculate quality score
            quality_score = self._calculate_quality_score(staging_data)
            analysis['quality_score'] = quality_score
            analysis['quality_level'] = self._get_quality_level(quality_score)
            
            # 3. Determine promotion strategy
            promotion_strategy = self._determine_promotion_strategy(changes, quality_score)
            analysis.update(promotion_strategy)
            
            logger.info(f"Analysis complete: {analysis['recommendation']} recommended")
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze staging data: {e}")
            analysis['recommendation'] = 'manual_review'
            analysis['manual_review_needed'] = True
            return analysis
    
    def _detect_changes(self, staging_data: pd.DataFrame, production_data: pd.DataFrame) -> List[Dict]:
        """Detect what has changed between staging and production (INSERT/UPDATE only)"""
        changes = []
        
        if production_data.empty:
            # First run - all records are new
            for _, row in staging_data.iterrows():
                changes.append({
                    'stable_id': row.get('stable_id'),
                    'change_type': ChangeType.INSERT,
                    'reason': 'new_record',
                    'details': 'First pipeline run'
                })
            return changes
        
        # Create lookup for production data
        production_lookup = production_data.set_index('stable_id').to_dict('index')
        
        for _, staging_row in staging_data.iterrows():
            stable_id = staging_row.get('stable_id')
            if not stable_id:
                continue
                
            if stable_id in production_lookup:
                # Record exists - check for updates
                production_row = production_lookup[stable_id]
                if self._has_significant_changes(staging_row, production_row):
                    changes.append({
                        'stable_id': stable_id,
                        'change_type': ChangeType.UPDATE,
                        'reason': 'data_updated',
                        'details': self._get_change_details(staging_row, production_row)
                    })
                else:
                    changes.append({
                        'stable_id': stable_id,
                        'change_type': ChangeType.NO_CHANGE,
                        'reason': 'no_changes',
                        'details': 'Record unchanged'
                    })
            else:
                # New record
                changes.append({
                    'stable_id': stable_id,
                    'change_type': ChangeType.INSERT,
                    'reason': 'new_record',
                    'details': 'New candidate discovered'
                })
        
        # IMPORTANT: We do NOT automatically detect deletions
        # Records that exist in production but not in new raw data are NOT deleted
        # This is because candidates may have:
        # - Withdrawn their candidacy
        # - Been disqualified
        # - Filed for different offices
        # - Still be valid from previous filings
        # 
        # Only manual deletions through the manual_data_operations.py script are allowed
        
        return changes
    
    def _has_significant_changes(self, staging_row: pd.Series, production_row: Dict) -> bool:
        """Determine if changes are significant enough to warrant an update"""
        # Fields that indicate significant changes
        significant_fields = [
            'candidate_name', 'office', 'party', 'county', 'district',
            'address', 'city', 'state', 'election_year'
        ]
        
        for field in significant_fields:
            staging_val = staging_row.get(field)
            production_val = production_row.get(field)
            
            if pd.isna(staging_val) and pd.isna(production_val):
                continue
            elif pd.isna(staging_val) or pd.isna(production_val):
                return True
            elif str(staging_val).strip() != str(production_val).strip():
                return True
        
        return False
    
    def _get_change_details(self, staging_row: pd.Series, production_row: Dict) -> str:
        """Get detailed description of what changed"""
        changed_fields = []
        
        for field in ['candidate_name', 'office', 'party', 'county', 'district']:
            staging_val = staging_row.get(field)
            production_val = production_row.get(field)
            
            if pd.isna(staging_val) and pd.isna(production_val):
                continue
            elif pd.isna(staging_val) or pd.isna(production_val):
                changed_fields.append(f"{field}: {production_val} → {staging_val}")
            elif str(staging_val).strip() != str(production_val).strip():
                changed_fields.append(f"{field}: {production_val} → {staging_val}")
        
        return "; ".join(changed_fields) if changed_fields else "Minor updates"
    
    def _summarize_changes(self, changes: List[Dict]) -> Dict[str, int]:
        """Summarize the types of changes detected"""
        summary = {
            'inserts': 0,
            'updates': 0,
            'deletes': 0,  # Always 0 - no automatic deletions
            'no_changes': 0,
            'total': len(changes)
        }
        
        for change in changes:
            change_type = change['change_type']
            if change_type == ChangeType.INSERT:
                summary['inserts'] += 1
            elif change_type == ChangeType.UPDATE:
                summary['updates'] += 1
            elif change_type == ChangeType.NO_CHANGE:
                summary['no_changes'] += 1
        
        return summary
    
    def _calculate_quality_score(self, data: pd.DataFrame) -> float:
        """Calculate overall data quality score (0.0 to 1.0)"""
        if data.empty:
            return 0.0
        
        quality_checks = []
        
        # Required field completeness
        required_fields = ['candidate_name', 'office', 'state', 'election_year']
        for field in required_fields:
            if field in data.columns:
                completeness = data[field].notna().sum() / len(data)
                quality_checks.append(completeness)
        
        # Data consistency checks
        if 'stable_id' in data.columns:
            # Check for duplicate stable IDs
            unique_ratio = data['stable_id'].nunique() / len(data)
            quality_checks.append(unique_ratio)
        
        # Address quality (if available)
        if 'address' in data.columns:
            address_quality = data['address'].notna().sum() / len(data)
            quality_checks.append(address_quality)
        
        # Party information quality
        if 'party' in data.columns:
            party_quality = data['party'].notna().sum() / len(data)
            quality_checks.append(party_quality)
        
        # Calculate average quality score
        if quality_checks:
            return sum(quality_checks) / len(quality_checks)
        else:
            return 0.0
    
    def _get_quality_level(self, quality_score: float) -> QualityLevel:
        """Convert quality score to quality level"""
        if quality_score >= self.quality_thresholds['excellent']:
            return QualityLevel.EXCELLENT
        elif quality_score >= self.quality_thresholds['good']:
            return QualityLevel.GOOD
        elif quality_score >= self.quality_thresholds['acceptable']:
            return QualityLevel.ACCEPTABLE
        else:
            return QualityLevel.POOR
    
    def _determine_promotion_strategy(self, changes: List[Dict], quality_score: float) -> Dict[str, Any]:
        """Determine the best promotion strategy based on changes and quality"""
        
        change_summary = self._summarize_changes(changes)
        total_changes = change_summary['inserts'] + change_summary['updates']
        
        strategy = {
            'auto_promote': False,
            'manual_review_needed': True,
            'recommendation': 'manual_review',
            'reason': ''
        }
        
        # Case 1: No significant changes, high quality
        if (total_changes == 0 and quality_score >= self.quality_thresholds['excellent']):
            strategy.update({
                'auto_promote': True,
                'manual_review_needed': False,
                'recommendation': 'auto_promote',
                'reason': 'No changes detected, excellent quality'
            })
        
        # Case 2: Only new records, excellent quality
        elif (change_summary['inserts'] > 0 and 
              change_summary['updates'] == 0 and
              quality_score >= self.quality_thresholds['excellent']):
            strategy.update({
                'auto_promote': True,
                'manual_review_needed': False,
                'recommendation': 'auto_promote_new_records',
                'reason': 'Only new records, excellent quality'
            })
        
        # Case 3: Minor updates, good quality
        elif (total_changes <= 10 and 
              quality_score >= self.quality_thresholds['good']):
            strategy.update({
                'auto_promote': True,
                'manual_review_needed': False,
                'recommendation': 'auto_promote_minor_changes',
                'reason': 'Minor changes, good quality'
            })
        
        # Case 4: Significant changes or quality concerns
        elif (change_summary['updates'] > 20 or
              quality_score < self.quality_thresholds['acceptable']):
            strategy.update({
                'auto_promote': False,
                'manual_review_needed': True,
                'recommendation': 'manual_review_required',
                'reason': 'Significant changes or quality concerns'
            })
        
        # Case 5: Default to manual review
        else:
            strategy.update({
                'auto_promote': False,
                'manual_review_needed': True,
                'recommendation': 'manual_review_recommended',
                'reason': 'Standard review process'
            })
        
        return strategy
    
    def execute_promotion_strategy(self, staging_data: pd.DataFrame, strategy: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the recommended promotion strategy
        
        Args:
            staging_data: Data to promote
            strategy: Promotion strategy from analysis
            
        Returns:
            Execution results
        """
        logger.info(f"Executing promotion strategy: {strategy['recommendation']}")
        
        results = {
            'success': False,
            'strategy_executed': strategy['recommendation'],
            'records_processed': 0,
            'errors': [],
            'warnings': []
        }
        
        try:
            if strategy['auto_promote']:
                # Execute automatic promotion
                if strategy['recommendation'] == 'auto_promote_new_records':
                    results = self._auto_promote_new_records(staging_data)
                elif strategy['recommendation'] == 'auto_promote_minor_changes':
                    results = self._auto_promote_minor_changes(staging_data)
                else:
                    results = self._auto_promote_all(staging_data)
            else:
                # Prepare for manual review
                results = self._prepare_for_manual_review(staging_data, strategy)
            
            results['success'] = True
            
        except Exception as e:
            logger.error(f"Failed to execute promotion strategy: {e}")
            results['errors'].append(str(e))
        
        return results
    
    def _auto_promote_new_records(self, staging_data: pd.DataFrame) -> Dict[str, Any]:
        """Automatically promote only new records"""
        logger.info("Auto-promoting new records only...")
        
        # Filter for new records (no stable_id in production)
        production_data = self.db_manager.execute_query("SELECT stable_id FROM filings")
        production_ids = set(production_data['stable_id'].dropna())
        
        new_records = staging_data[~staging_data['stable_id'].isin(production_ids)]
        
        # Insert new records
        success = self.db_manager.upload_dataframe(
            new_records, 'filings', if_exists='append', index=False
        )
        
        return {
            'strategy_executed': 'auto_promote_new_records',
            'records_processed': len(new_records),
            'new_records_inserted': len(new_records),
            'existing_records_updated': 0,
            'records_deleted': 0
        }
    
    def _auto_promote_minor_changes(self, staging_data: pd.DataFrame) -> Dict[str, Any]:
        """Automatically promote minor changes"""
        logger.info("Auto-promoting minor changes...")
        
        # Use incremental update for minor changes
        return self._perform_incremental_update(staging_data)
    
    def _auto_promote_all(self, staging_data: pd.DataFrame) -> Dict[str, Any]:
        """Automatically promote all data (full replacement)"""
        logger.info("Auto-promoting all data...")
        
        # Clear existing data and insert new
        self.db_manager.execute_query("DELETE FROM filings")
        
        success = self.db_manager.upload_dataframe(
            staging_data, 'filings', if_exists='append', index=False
        )
        
        return {
            'strategy_executed': 'auto_promote_all',
            'records_processed': len(staging_data),
            'new_records_inserted': len(staging_data),
            'existing_records_updated': 0,
            'records_deleted': 0
        }
    
    def _perform_incremental_update(self, staging_data: pd.DataFrame) -> Dict[str, Any]:
        """Perform incremental update (INSERT/UPDATE) without DELETE"""
        logger.info("Performing incremental update...")
        
        results = {
            'strategy_executed': 'incremental_update',
            'records_processed': len(staging_data),
            'new_records_inserted': 0,
            'existing_records_updated': 0,
            'records_deleted': 0
        }
        
        # Get existing production data
        production_data = self.db_manager.execute_query("SELECT stable_id FROM filings")
        production_ids = set(production_data['stable_id'].dropna())
        
        for _, staging_row in staging_data.iterrows():
            stable_id = staging_row.get('stable_id')
            if not stable_id:
                continue
            
            if stable_id in production_ids:
                # Update existing record
                self._update_existing_record(staging_row)
                results['existing_records_updated'] += 1
            else:
                # Insert new record
                self._insert_new_record(staging_row)
                results['new_records_inserted'] += 1
        
        return results
    
    def _update_existing_record(self, staging_row: pd.Series):
        """Update an existing record in production"""
        # Build UPDATE query
        update_fields = []
        update_values = []
        
        for column in staging_row.index:
            if column != 'stable_id' and pd.notna(staging_row[column]):
                update_fields.append(f"{column} = %s")
                update_values.append(staging_row[column])
        
        if update_fields:
            update_values.append(staging_row['stable_id'])  # For WHERE clause
            query = f"""
                UPDATE filings 
                SET {', '.join(update_fields)}, last_updated_date = CURRENT_TIMESTAMP
                WHERE stable_id = %s
            """
            self.db_manager.execute_query(query, update_values)
    
    def _insert_new_record(self, staging_row: pd.Series):
        """Insert a new record into production"""
        # Set first_added_date and last_updated_date
        staging_row['first_added_date'] = datetime.now()
        staging_row['last_updated_date'] = datetime.now()
        
        # Insert record
        insert_data = pd.DataFrame([staging_row])
        self.db_manager.upload_dataframe(
            insert_data, 'filings', if_exists='append', index=False
        )
    
    def _prepare_for_manual_review(self, staging_data: pd.DataFrame, strategy: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare data for manual review"""
        logger.info("Preparing data for manual review...")
        
        # Add change tracking metadata
        staging_data['change_type'] = 'pending_review'
        staging_data['review_timestamp'] = datetime.now()
        staging_data['review_required'] = True
        staging_data['auto_promote_reason'] = strategy['reason']
        
        # Save to staging with review metadata
        success = self.db_manager.upload_dataframe(
            staging_data, 'staging_candidates', if_exists='replace', index=False
        )
        
        return {
            'strategy_executed': 'manual_review_prepared',
            'records_processed': len(staging_data),
            'review_required': True,
            'staging_updated': success
        }
