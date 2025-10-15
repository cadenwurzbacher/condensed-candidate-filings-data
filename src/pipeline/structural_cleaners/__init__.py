"""
Structural Cleaners Package

This package contains structural cleaners for Phase 1 of the pipeline.
Structural cleaners extract structured data from messy raw files without
transforming the data content - they only parse and organize the structure.

REFACTORED: All 34 state cleaners now use BaseStructuralCleaner, reducing
~11,000 lines of duplicate code to ~2,000 lines total!
"""

from .base_structural_cleaner import BaseStructuralCleaner
from .field_extractor import FieldExtractor
from .alaska_structural_cleaner import AlaskaStructuralCleaner
from .arizona_structural_cleaner import ArizonaStructuralCleaner
from .arkansas_structural_cleaner import ArkansasStructuralCleaner
from .colorado_structural_cleaner import ColoradoStructuralCleaner
from .delaware_structural_cleaner import DelawareStructuralCleaner
from .florida_structural_cleaner import FloridaStructuralCleaner
from .georgia_structural_cleaner import GeorgiaStructuralCleaner
from .hawaii_structural_cleaner import HawaiiStructuralCleaner
from .idaho_structural_cleaner import IdahoStructuralCleaner
from .illinois_structural_cleaner import IllinoisStructuralCleaner
from .indiana_structural_cleaner import IndianaStructuralCleaner
from .iowa_structural_cleaner import IowaStructuralCleaner
from .kansas_structural_cleaner import KansasStructuralCleaner
from .kentucky_structural_cleaner import KentuckyStructuralCleaner
from .louisiana_structural_cleaner import LouisianaStructuralCleaner
from .maryland_structural_cleaner import MarylandStructuralCleaner
from .massachusetts_structural_cleaner import MassachusettsStructuralCleaner
from .missouri_structural_cleaner import MissouriStructuralCleaner
from .montana_structural_cleaner import MontanaStructuralCleaner
from .nebraska_structural_cleaner import NebraskaStructuralCleaner
from .new_mexico_structural_cleaner import NewMexicoStructuralCleaner
from .new_york_structural_cleaner import NewYorkStructuralCleaner
from .north_carolina_structural_cleaner import NorthCarolinaStructuralCleaner
from .north_dakota_structural_cleaner import NorthDakotaStructuralCleaner
from .oklahoma_structural_cleaner import OklahomaStructuralCleaner
from .oregon_structural_cleaner import OregonStructuralCleaner
from .pennsylvania_structural_cleaner import PennsylvaniaStructuralCleaner
from .south_carolina_structural_cleaner import SouthCarolinaStructuralCleaner
from .vermont_structural_cleaner import VermontStructuralCleaner
from .virginia_structural_cleaner import VirginiaStructuralCleaner
from .washington_structural_cleaner import WashingtonStructuralCleaner
from .west_virginia_structural_cleaner import WestVirginiaStructuralCleaner
from .wyoming_structural_cleaner import WyomingStructuralCleaner
from .south_dakota_structural_cleaner import SouthDakotaStructuralCleaner

__all__ = [
    'AlaskaStructuralCleaner',
    'ArizonaStructuralCleaner',
    'ArkansasStructuralCleaner',
    'ColoradoStructuralCleaner',
    'DelawareStructuralCleaner',
    'FloridaStructuralCleaner',
    'GeorgiaStructuralCleaner',
    'HawaiiStructuralCleaner',
    'IdahoStructuralCleaner',
    'IllinoisStructuralCleaner',
    'IndianaStructuralCleaner',
    'IowaStructuralCleaner',
    'KansasStructuralCleaner',
    'KentuckyStructuralCleaner',
    'LouisianaStructuralCleaner',
    'MarylandStructuralCleaner',
    'MassachusettsStructuralCleaner',
    'MissouriStructuralCleaner',
    'MontanaStructuralCleaner',
    'NebraskaStructuralCleaner',
    'NewMexicoStructuralCleaner',
    'NewYorkStructuralCleaner',
    'NorthCarolinaStructuralCleaner',
    'NorthDakotaStructuralCleaner',
    'OklahomaStructuralCleaner',
    'OregonStructuralCleaner',
    'PennsylvaniaStructuralCleaner',
    'SouthCarolinaStructuralCleaner',
    'VermontStructuralCleaner',
    'VirginiaStructuralCleaner',
    'WashingtonStructuralCleaner',
    'WestVirginiaStructuralCleaner',
    'WyomingStructuralCleaner',
    'SouthDakotaStructuralCleaner',
]
