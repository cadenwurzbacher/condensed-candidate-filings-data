#!/usr/bin/env python3
"""
Dynamic Import System for State Cleaners and Structural Cleaners

This module automatically discovers and imports all state cleaners and structural cleaners,
eliminating the need for hardcoded import lists in main_pipeline.py.
"""

import importlib
import inspect
from pathlib import Path
from typing import Dict, Type, List
import logging

logger = logging.getLogger(__name__)

class DynamicImporter:
    """Dynamically imports state cleaners and structural cleaners."""
    
    def __init__(self):
        self.state_cleaners: Dict[str, Type] = {}
        self.structural_cleaners: Dict[str, Type] = {}
        self._discover_cleaners()
    
    def _discover_cleaners(self):
        """Discover and import all available cleaners."""
        self._discover_state_cleaners()
        self._discover_structural_cleaners()
    
    def _discover_state_cleaners(self):
        """Discover all state cleaners in the state_cleaners directory."""
        state_cleaners_dir = Path(__file__).parent / "state_cleaners"
        
        if not state_cleaners_dir.exists():
            logger.warning(f"State cleaners directory not found: {state_cleaners_dir}")
            return
        
        # Find all *_cleaner.py files (excluding base_cleaner.py)
        cleaner_files = [f for f in state_cleaners_dir.glob("*_cleaner.py") if f.stem != "base_cleaner"]

        for file_path in cleaner_files:
            try:
                # Extract state name from filename (e.g., "alaska_cleaner.py" -> "alaska")
                state_name = file_path.stem.replace("_cleaner", "")
                
                # Import the module using relative import
                module_name = f"src.pipeline.state_cleaners.{file_path.stem}"
                module = importlib.import_module(module_name)
                
                # Find the cleaner class (should be named like "AlaskaCleaner")
                cleaner_class = None
                for name, obj in inspect.getmembers(module):
                    if (inspect.isclass(obj) and 
                        name.endswith("Cleaner") and 
                        name != "BaseStateCleaner" and
                        hasattr(obj, 'clean_data')):
                        cleaner_class = obj
                        break
                
                if cleaner_class:
                    self.state_cleaners[state_name] = cleaner_class
                    logger.debug(f"Discovered state cleaner: {state_name} -> {cleaner_class.__name__}")
                else:
                    logger.warning(f"No cleaner class found in {file_path}")
                    
            except Exception as e:
                logger.error(f"Failed to import state cleaner from {file_path}: {e}")
    
    def _discover_structural_cleaners(self):
        """Discover all structural cleaners in the structural_cleaners directory."""
        structural_cleaners_dir = Path(__file__).parent / "structural_cleaners"

        if not structural_cleaners_dir.exists():
            logger.warning(f"Structural cleaners directory not found: {structural_cleaners_dir}")
            return

        # Find all *_structural_cleaner.py files (excluding base_structural_cleaner.py)
        cleaner_files = [f for f in structural_cleaners_dir.glob("*_structural_cleaner.py")
                        if f.stem != "base_structural_cleaner"]
        
        for file_path in cleaner_files:
            try:
                # Extract state name from filename (e.g., "alaska_structural_cleaner.py" -> "alaska")
                state_name = file_path.stem.replace("_structural_cleaner", "")
                
                # Import the module using relative import
                module_name = f"src.pipeline.structural_cleaners.{file_path.stem}"
                module = importlib.import_module(module_name)
                
                # Find the cleaner class (should be named like "AlaskaStructuralCleaner")
                cleaner_class = None
                for name, obj in inspect.getmembers(module):
                    if (inspect.isclass(obj) and
                        name.endswith("StructuralCleaner") and
                        name != "BaseStructuralCleaner" and
                        (hasattr(obj, 'clean') or hasattr(obj, 'clean_data'))):
                        cleaner_class = obj
                        break
                
                if cleaner_class:
                    self.structural_cleaners[state_name] = cleaner_class
                    logger.debug(f"Discovered structural cleaner: {state_name} -> {cleaner_class.__name__}")
                else:
                    logger.warning(f"No structural cleaner class found in {file_path}")
                    
            except Exception as e:
                logger.error(f"Failed to import structural cleaner from {file_path}: {e}")
    
    def get_state_cleaner(self, state_name: str) -> Type:
        """Get a state cleaner class by state name."""
        state_name = state_name.lower()
        if state_name not in self.state_cleaners:
            raise ValueError(f"No state cleaner found for state: {state_name}")
        return self.state_cleaners[state_name]
    
    def get_structural_cleaner(self, state_name: str) -> Type:
        """Get a structural cleaner class by state name."""
        state_name = state_name.lower()
        if state_name not in self.structural_cleaners:
            raise ValueError(f"No structural cleaner found for state: {state_name}")
        return self.structural_cleaners[state_name]
    
    def get_available_states(self) -> List[str]:
        """Get list of all available states."""
        return list(self.state_cleaners.keys())
    
    def get_available_structural_states(self) -> List[str]:
        """Get list of all available structural states."""
        return list(self.structural_cleaners.keys())
    
    def has_state_cleaner(self, state_name: str) -> bool:
        """Check if a state cleaner exists for the given state."""
        return state_name.lower() in self.state_cleaners
    
    def has_structural_cleaner(self, state_name: str) -> bool:
        """Check if a structural cleaner exists for the given state."""
        return state_name.lower() in self.structural_cleaners
    
    def get_cleaner_summary(self) -> str:
        """Get a summary of discovered cleaners."""
        state_count = len(self.state_cleaners)
        structural_count = len(self.structural_cleaners)
        
        summary = f"Discovered {state_count} state cleaners and {structural_count} structural cleaners:\n"
        
        if self.state_cleaners:
            summary += "\nState Cleaners:\n"
            for state, cleaner_class in sorted(self.state_cleaners.items()):
                summary += f"  {state}: {cleaner_class.__name__}\n"
        
        if self.structural_cleaners:
            summary += "\nStructural Cleaners:\n"
            for state, cleaner_class in sorted(self.structural_cleaners.items()):
                summary += f"  {state}: {cleaner_class.__name__}\n"
        
        return summary

# Global instance for easy access
dynamic_importer = DynamicImporter()
