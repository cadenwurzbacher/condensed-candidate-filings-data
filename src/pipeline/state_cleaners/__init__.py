"""
State Cleaners Package

This package contains all individual state candidate data cleaning scripts.
Each cleaner is designed to handle the specific data format and requirements
of its respective state.
"""

__version__ = "1.0.0"
__author__ = "Data Cleaning Team"

# List of available state cleaners
AVAILABLE_STATES = [
    'alaska', 'arizona', 'arkansas', 'colorado', 'delaware',
    'georgia', 'idaho', 'illinois', 'indiana', 'kansas',
    'kentucky', 'louisiana', 'missouri', 'montana', 'nebraska',
    'new_mexico', 'oklahoma', 'oregon', 'south_carolina',
    'south_dakota', 'vermont', 'washington', 'west_virginia', 'wyoming'
]

def get_available_states():
    """Return list of available state cleaners."""
    return AVAILABLE_STATES.copy()

def get_state_cleaner(state_name):
    """Get the cleaner function for a specific state."""
    try:
        module_name = f"{state_name}_cleaner"
        module = __import__(module_name)
        cleaner_func = getattr(module, f"clean_{state_name}_candidates")
        return cleaner_func
    except (ImportError, AttributeError):
        raise ValueError(f"No cleaner found for state: {state_name}")
