#!/usr/bin/env python3
"""
Targeted fixer for common state cleaner issues:
- Fix missing self. on DataFrame.apply helper calls
- Remove accidental "def self." in function definitions
- Fix apply(standardize_party) to use self.standardize_party
- Remove class-level stray "return df" that precede method defs
This script avoids reformatting and preserves existing indentation style.
"""

import re
from pathlib import Path

def fix_file(path: Path) -> bool:
    original = path.read_text(encoding='utf-8')
    content = original

    # 1) apply(process_office_district) -> apply(self.process_office_district)
    content = re.sub(r"apply\(\s*process_office_district\s*\)", "apply(self.process_office_district)", content)

    # 2) apply(clean_name) -> apply(self.clean_name)
    content = re.sub(r"apply\(\s*clean_name\s*\)", "apply(self.clean_name)", content)

    # 3) apply(standardize_party) -> apply(self.standardize_party)
    content = re.sub(r"apply\(\s*standardize_party\s*\)", "apply(self.standardize_party)", content)

    # 4) Remove mistaken function defs like: def self.xxx(...): -> def xxx(...):
    content = re.sub(r"\bdef\s+self\.(\w+)\(", r"def \1(", content)

    # 5) Remove class-level stray return df that occur before method definitions
    # Pattern: newline, some indentation (or none), return df, blank line, indentation, def _
    content = re.sub(r"\n(\s*)return\s+df\s*\n\n(\s*)def\s+_", lambda m: f"\n\n{m.group(2)}def _", content)

    # 6) Fix common party assignment regression: df['party'] = df['Party'].apply(self.standardize_party)
    # If accidental variant existed like df['party'] = df[standardize_party].apply(self.standardize_party)
    content = re.sub(r"df\['party'\]\s*=\s*df\[standardize_party\]\.apply\(self\.standardize_party\)",
                     "df['party'] = df['Party'].apply(self.standardize_party)", content)

    if content != original:
        path.write_text(content, encoding='utf-8')
        return True
    return False


def main():
    base = Path('src/pipeline/state_cleaners')
    files = [p for p in base.glob('*.py') if p.name != '__init__.py']
    changed = 0
    for p in files:
        if fix_file(p):
            print(f"✅ Fixed common issues in {p}")
            changed += 1
        else:
            print(f"OK {p}")
    print(f"\nDone. Files changed: {changed}/{len(files)}")

if __name__ == '__main__':
    main()
