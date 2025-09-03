"""
Pipeline Managers Package

This package contains specialized manager classes for the CandidateFilings pipeline:
- DataManager: Handles file operations and data persistence
- DatabaseManager: Handles database operations and connections
- DataProcessor: Handles data processing and transformation
"""

from .data_manager import DataManager
from .database_manager import DatabaseManager
from .data_processor import DataProcessor

__all__ = [
    'DataManager',
    'DatabaseManager', 
    'DataProcessor'
]
