#!/usr/bin/env python3
"""
Data Quality Monitor for the Enterprise Political Candidates Data Pipeline.

This module provides comprehensive data quality monitoring, validation,
and reporting capabilities to ensure data integrity throughout the pipeline.
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
import logging
from collections import defaultdict
import json
from pipeline_config import VALIDATION_RULES, QUALITY_THRESHOLDS, PARTY_MAPPING, STATE_MAPPING

logger = logging.getLogger(__name__)

class DataQualityMonitor:
    """Monitors and validates data quality throughout the pipeline."""
    
    def __init__(self):
        self.quality_metrics = {}
        self.validation_errors = []
        self.data_issues = []
        self.quality_report = {}
    
    def validate_dataset(self, df: pd.DataFrame, stage: str = "unknown") -> Dict[str, Any]:
        """
        Comprehensive dataset validation.
        
        Args:
            df: DataFrame to validate
            stage: Pipeline stage for reporting
            
        Returns:
            Dictionary containing validation results and metrics
        """
        logger.info(f"Validating dataset at stage: {stage}")
        
        validation_results = {
            'stage': stage,
            'timestamp': datetime.now().isoformat(),
            'total_records': len(df),
            'validation_passed': True,
            'metrics': {},
            'errors': [],
            'warnings': [],
            'recommendations': []
        }
        
        # Basic structure validation
        structure_validation = self._validate_structure(df)
        validation_results['metrics'].update(structure_validation)
        
        # Data type validation
        type_validation = self._validate_data_types(df)
        validation_results['metrics'].update(type_validation)
        
        # Content validation
        content_validation = self._validate_content(df)
        validation_results['metrics'].update(content_validation)
        
        # Business rule validation
        business_validation = self._validate_business_rules(df)
        validation_results['metrics'].update(business_validation)
        
        # Duplicate detection
        duplicate_analysis = self._analyze_duplicates(df)
        validation_results['metrics'].update(duplicate_analysis)
        
        # Completeness analysis
        completeness_analysis = self._analyze_completeness(df)
        validation_results['metrics'].update(completeness_analysis)
        
        # Quality scoring
        quality_score = self._calculate_quality_score(validation_results['metrics'])
        validation_results['quality_score'] = quality_score
        
        # Determine if validation passed
        validation_results['validation_passed'] = self._evaluate_validation_results(
            validation_results['metrics']
        )
        
        # Store results
        self.quality_metrics[stage] = validation_results
        
        return validation_results
    
    def _validate_structure(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate basic structure of the dataset."""
        metrics = {
            'structure': {
                'total_columns': len(df.columns),
                'total_rows': len(df),
                'empty_dataset': len(df) == 0,
                'missing_required_columns': []
            }
        }
        
        # Check for required columns
        required_columns = QUALITY_THRESHOLDS['minimum_required_fields']
        missing_columns = [col for col in required_columns if col not in df.columns]
        metrics['structure']['missing_required_columns'] = missing_columns
        
        if missing_columns:
            self.data_issues.append(f"Missing required columns: {missing_columns}")
        
        return metrics
    
    def _validate_data_types(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate data types of columns."""
        metrics = {
            'data_types': {
                'type_issues': [],
                'conversion_errors': []
            }
        }
        
        # Expected data types for key columns
        expected_types = {
            'candidate_name': 'object',
            'state': 'object',
            'office': 'object',
            'party': 'object',
            'district': 'object',
            'filing_date': 'datetime64[ns]',
            'election_date': 'datetime64[ns]',
            'phone': 'object',
            'email': 'object',
            'zip_code': 'object'
        }
        
        for column, expected_type in expected_types.items():
            if column in df.columns:
                actual_type = str(df[column].dtype)
                if actual_type != expected_type:
                    metrics['data_types']['type_issues'].append({
                        'column': column,
                        'expected': expected_type,
                        'actual': actual_type
                    })
        
        return metrics
    
    def _validate_content(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate content of specific columns."""
        metrics = {
            'content_validation': {
                'phone_validation': {'valid': 0, 'invalid': 0, 'null': 0},
                'email_validation': {'valid': 0, 'invalid': 0, 'null': 0},
                'zip_validation': {'valid': 0, 'invalid': 0, 'null': 0},
                'date_validation': {'valid': 0, 'invalid': 0, 'null': 0},
                'party_validation': {'valid': 0, 'invalid': 0, 'null': 0},
                'state_validation': {'valid': 0, 'invalid': 0, 'null': 0}
            }
        }
        
        # Validate phone numbers
        if 'phone' in df.columns:
            phone_validation = self._validate_phone_numbers(df['phone'])
            metrics['content_validation']['phone_validation'] = phone_validation
        
        # Validate email addresses
        if 'email' in df.columns:
            email_validation = self._validate_email_addresses(df['email'])
            metrics['content_validation']['email_validation'] = email_validation
        
        # Validate zip codes
        if 'zip_code' in df.columns:
            zip_validation = self._validate_zip_codes(df['zip_code'])
            metrics['content_validation']['zip_validation'] = zip_validation
        
        # Validate dates
        date_columns = ['filing_date', 'election_date']
        for col in date_columns:
            if col in df.columns:
                date_validation = self._validate_dates(df[col])
                metrics['content_validation']['date_validation'] = date_validation
        
        # Validate party names
        if 'party' in df.columns:
            party_validation = self._validate_party_names(df['party'])
            metrics['content_validation']['party_validation'] = party_validation
        
        # Validate state names
        if 'state' in df.columns:
            state_validation = self._validate_state_names(df['state'])
            metrics['content_validation']['state_validation'] = state_validation
        
        return metrics
    
    def _validate_business_rules(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate business rules and constraints."""
        metrics = {
            'business_rules': {
                'future_election_dates': 0,
                'past_filing_dates': 0,
                'invalid_date_combinations': 0,
                'missing_candidate_names': 0,
                'empty_office_fields': 0
            }
        }
        
        # Check for future election dates
        if 'election_date' in df.columns:
            future_dates = df[df['election_date'] > datetime.now() + timedelta(days=365)]
            metrics['business_rules']['future_election_dates'] = len(future_dates)
        
        # Check for past filing dates (more than 10 years ago)
        if 'filing_date' in df.columns:
            past_dates = df[df['filing_date'] < datetime.now() - timedelta(days=3650)]
            metrics['business_rules']['past_filing_dates'] = len(past_dates)
        
        # Check for invalid date combinations (filing after election)
        if 'filing_date' in df.columns and 'election_date' in df.columns:
            invalid_combinations = df[
                (df['filing_date'].notna()) & 
                (df['election_date'].notna()) & 
                (df['filing_date'] > df['election_date'])
            ]
            metrics['business_rules']['invalid_date_combinations'] = len(invalid_combinations)
        
        # Check for missing candidate names
        if 'candidate_name' in df.columns:
            missing_names = df[df['candidate_name'].isna() | (df['candidate_name'] == '')]
            metrics['business_rules']['missing_candidate_names'] = len(missing_names)
        
        # Check for empty office fields
        if 'office' in df.columns:
            empty_offices = df[df['office'].isna() | (df['office'] == '')]
            metrics['business_rules']['empty_office_fields'] = len(empty_offices)
        
        return metrics
    
    def _analyze_duplicates(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze duplicate records."""
        metrics = {
            'duplicates': {
                'exact_duplicates': 0,
                'near_duplicates': 0,
                'duplicate_candidates': 0,
                'duplicate_rate': 0.0
            }
        }
        
        # Check for exact duplicates
        exact_duplicates = df.duplicated().sum()
        metrics['duplicates']['exact_duplicates'] = exact_duplicates
        
        # Check for near duplicates (same candidate, office, state, but different details)
        if 'candidate_name' in df.columns and 'office' in df.columns and 'state' in df.columns:
            key_columns = ['candidate_name', 'office', 'state']
            near_duplicates = df.duplicated(subset=key_columns).sum()
            metrics['duplicates']['near_duplicates'] = near_duplicates
        
        # Check for duplicate candidates in same election
        if 'candidate_name' in df.columns and 'election_date' in df.columns:
            candidate_election_duplicates = df.duplicated(
                subset=['candidate_name', 'election_date']
            ).sum()
            metrics['duplicates']['duplicate_candidates'] = candidate_election_duplicates
        
        # Calculate duplicate rate
        if len(df) > 0:
            total_duplicates = exact_duplicates + near_duplicates
            metrics['duplicates']['duplicate_rate'] = total_duplicates / len(df)
        
        return metrics
    
    def _analyze_completeness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data completeness."""
        metrics = {
            'completeness': {
                'column_completeness': {},
                'overall_completeness': 0.0,
                'critical_field_completeness': 0.0
            }
        }
        
        # Calculate completeness for each column
        for column in df.columns:
            non_null_count = df[column].notna().sum()
            completeness_rate = non_null_count / len(df) if len(df) > 0 else 0
            metrics['completeness']['column_completeness'][column] = {
                'non_null_count': non_null_count,
                'null_count': len(df) - non_null_count,
                'completeness_rate': completeness_rate
            }
        
        # Calculate overall completeness
        total_cells = len(df) * len(df.columns)
        non_null_cells = df.notna().sum().sum()
        metrics['completeness']['overall_completeness'] = non_null_cells / total_cells if total_cells > 0 else 0
        
        # Calculate critical field completeness
        critical_fields = QUALITY_THRESHOLDS['minimum_required_fields']
        critical_completeness = []
        for field in critical_fields:
            if field in df.columns:
                field_completeness = df[field].notna().sum() / len(df) if len(df) > 0 else 0
                critical_completeness.append(field_completeness)
        
        if critical_completeness:
            metrics['completeness']['critical_field_completeness'] = np.mean(critical_completeness)
        
        return metrics
    
    def _validate_phone_numbers(self, series: pd.Series) -> Dict[str, int]:
        """Validate phone number format."""
        pattern = VALIDATION_RULES['phone']['pattern']
        valid = 0
        invalid = 0
        null = 0
        
        for value in series:
            if pd.isna(value):
                null += 1
            elif re.match(pattern, str(value)):
                valid += 1
            else:
                invalid += 1
        
        return {'valid': valid, 'invalid': invalid, 'null': null}
    
    def _validate_email_addresses(self, series: pd.Series) -> Dict[str, int]:
        """Validate email address format."""
        pattern = VALIDATION_RULES['email']['pattern']
        valid = 0
        invalid = 0
        null = 0
        
        for value in series:
            if pd.isna(value):
                null += 1
            elif re.match(pattern, str(value)):
                valid += 1
            else:
                invalid += 1
        
        return {'valid': valid, 'invalid': invalid, 'null': null}
    
    def _validate_zip_codes(self, series: pd.Series) -> Dict[str, int]:
        """Validate zip code format."""
        pattern = VALIDATION_RULES['zip_code']['pattern']
        valid = 0
        invalid = 0
        null = 0
        
        for value in series:
            if pd.isna(value):
                null += 1
            elif re.match(pattern, str(value)):
                valid += 1
            else:
                invalid += 1
        
        return {'valid': valid, 'invalid': invalid, 'null': null}
    
    def _validate_dates(self, series: pd.Series) -> Dict[str, int]:
        """Validate date values."""
        min_date = pd.to_datetime(VALIDATION_RULES['election_date']['min_date'])
        max_date = pd.to_datetime(VALIDATION_RULES['election_date']['max_date'])
        valid = 0
        invalid = 0
        null = 0
        
        for value in series:
            if pd.isna(value):
                null += 1
            else:
                try:
                    date_value = pd.to_datetime(value)
                    if min_date <= date_value <= max_date:
                        valid += 1
                    else:
                        invalid += 1
                except:
                    invalid += 1
        
        return {'valid': valid, 'invalid': invalid, 'null': null}
    
    def _validate_party_names(self, series: pd.Series) -> Dict[str, int]:
        """Validate party names against known party list."""
        valid_parties = set(PARTY_MAPPING.values())
        valid = 0
        invalid = 0
        null = 0
        
        for value in series:
            if pd.isna(value):
                null += 1
            elif str(value).strip() in valid_parties:
                valid += 1
            else:
                invalid += 1
        
        return {'valid': valid, 'invalid': invalid, 'null': null}
    
    def _validate_state_names(self, series: pd.Series) -> Dict[str, int]:
        """Validate state names against known state list."""
        valid_states = set(STATE_MAPPING.values())
        valid = 0
        invalid = 0
        null = 0
        
        for value in series:
            if pd.isna(value):
                null += 1
            elif str(value).strip() in valid_states:
                valid += 1
            else:
                invalid += 1
        
        return {'valid': valid, 'invalid': invalid, 'null': null}
    
    def _calculate_quality_score(self, metrics: Dict[str, Any]) -> float:
        """Calculate overall data quality score."""
        score_components = []
        
        # Structure score (20%)
        if 'structure' in metrics:
            structure_score = 1.0 if not metrics['structure']['missing_required_columns'] else 0.5
            score_components.append(structure_score * 0.2)
        
        # Completeness score (25%)
        if 'completeness' in metrics:
            completeness_score = metrics['completeness']['critical_field_completeness']
            score_components.append(completeness_score * 0.25)
        
        # Content validation score (30%)
        if 'content_validation' in metrics:
            content_scores = []
            for field, validation in metrics['content_validation'].items():
                if validation['valid'] + validation['null'] > 0:
                    field_score = validation['valid'] / (validation['valid'] + validation['null'])
                    content_scores.append(field_score)
            
            if content_scores:
                avg_content_score = np.mean(content_scores)
                score_components.append(avg_content_score * 0.3)
        
        # Duplicate score (15%)
        if 'duplicates' in metrics:
            duplicate_rate = metrics['duplicates']['duplicate_rate']
            duplicate_score = max(0, 1 - duplicate_rate * 10)  # Penalize high duplicate rates
            score_components.append(duplicate_score * 0.15)
        
        # Business rules score (10%)
        if 'business_rules' in metrics:
            business_issues = sum(metrics['business_rules'].values())
            business_score = max(0, 1 - business_issues / 1000)  # Penalize business rule violations
            score_components.append(business_score * 0.1)
        
        # Calculate final score
        if score_components:
            return sum(score_components)
        else:
            return 0.0
    
    def _evaluate_validation_results(self, metrics: Dict[str, Any]) -> bool:
        """Evaluate if validation results meet quality thresholds."""
        # Check critical field completeness
        if 'completeness' in metrics:
            critical_completeness = metrics['completeness']['critical_field_completeness']
            if critical_completeness < QUALITY_THRESHOLDS['null_threshold']:
                return False
        
        # Check duplicate rate
        if 'duplicates' in metrics:
            duplicate_rate = metrics['duplicates']['duplicate_rate']
            if duplicate_rate > QUALITY_THRESHOLDS['duplicate_threshold']:
                return False
        
        # Check overall quality score
        quality_score = self._calculate_quality_score(metrics)
        if quality_score < QUALITY_THRESHOLDS['validation_threshold']:
            return False
        
        return True
    
    def generate_quality_report(self) -> Dict[str, Any]:
        """Generate comprehensive quality report."""
        report = {
            'report_generated': datetime.now().isoformat(),
            'pipeline_stages': list(self.quality_metrics.keys()),
            'overall_summary': {},
            'stage_details': self.quality_metrics,
            'recommendations': []
        }
        
        # Calculate overall summary
        if self.quality_metrics:
            total_records = sum(
                stage['total_records'] for stage in self.quality_metrics.values()
            )
            avg_quality_score = np.mean([
                stage['quality_score'] for stage in self.quality_metrics.values()
            ])
            validation_passed = all(
                stage['validation_passed'] for stage in self.quality_metrics.values()
            )
            
            report['overall_summary'] = {
                'total_records_processed': total_records,
                'average_quality_score': avg_quality_score,
                'all_stages_passed_validation': validation_passed,
                'total_stages': len(self.quality_metrics)
            }
        
        # Generate recommendations
        report['recommendations'] = self._generate_recommendations()
        
        return report
    
    def _generate_recommendations(self) -> List[str]:
        """Generate data quality improvement recommendations."""
        recommendations = []
        
        for stage, metrics in self.quality_metrics.items():
            # Check completeness issues
            if 'completeness' in metrics:
                completeness = metrics['completeness']
                for field, field_metrics in completeness['column_completeness'].items():
                    if field_metrics['completeness_rate'] < 0.8:
                        recommendations.append(
                            f"Improve completeness for {field} in {stage} "
                            f"(current: {field_metrics['completeness_rate']:.1%})"
                        )
            
            # Check duplicate issues
            if 'duplicates' in metrics:
                duplicate_rate = metrics['duplicates']['duplicate_rate']
                if duplicate_rate > 0.05:
                    recommendations.append(
                        f"Address duplicate records in {stage} "
                        f"(duplicate rate: {duplicate_rate:.1%})"
                    )
            
            # Check validation issues
            if 'content_validation' in metrics:
                for field, validation in metrics['content_validation'].items():
                    total = validation['valid'] + validation['invalid']
                    if total > 0:
                        invalid_rate = validation['invalid'] / total
                        if invalid_rate > 0.1:
                            recommendations.append(
                                f"Improve {field} validation in {stage} "
                                f"(invalid rate: {invalid_rate:.1%})"
                            )
        
        return recommendations
    
    def save_quality_report(self, filename: str = None) -> str:
        """Save quality report to file."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"quality_report_{timestamp}.json"
        
        report = self.generate_quality_report()
        
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"Quality report saved to: {filename}")
        return filename
    
    def print_quality_summary(self):
        """Print a summary of quality metrics to console."""
        if not self.quality_metrics:
            logger.info("No quality metrics available.")
            return
        
        print("\n" + "="*60)
        print("DATA QUALITY SUMMARY")
        print("="*60)
        
        for stage, metrics in self.quality_metrics.items():
            print(f"\nStage: {stage.upper()}")
            print(f"  Records: {metrics['total_records']:,}")
            print(f"  Quality Score: {metrics['quality_score']:.2%}")
            print(f"  Validation Passed: {'✅' if metrics['validation_passed'] else '❌'}")
            
            if 'completeness' in metrics:
                critical_completeness = metrics['completeness']['critical_field_completeness']
                print(f"  Critical Field Completeness: {critical_completeness:.1%}")
            
            if 'duplicates' in metrics:
                duplicate_rate = metrics['duplicates']['duplicate_rate']
                print(f"  Duplicate Rate: {duplicate_rate:.1%}")
        
        # Overall summary
        if len(self.quality_metrics) > 1:
            total_records = sum(stage['total_records'] for stage in self.quality_metrics.values())
            avg_score = np.mean([stage['quality_score'] for stage in self.quality_metrics.values()])
            all_passed = all(stage['validation_passed'] for stage in self.quality_metrics.values())
            
            print(f"\nOVERALL SUMMARY")
            print(f"  Total Records: {total_records:,}")
            print(f"  Average Quality Score: {avg_score:.2%}")
            print(f"  All Stages Passed: {'✅' if all_passed else '❌'}")
        
        print("="*60) 