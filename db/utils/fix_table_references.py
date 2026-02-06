"""
Utility Script: Apply Bronze Schema to SQL Queries

Updates all SQL queries in db/ scripts to use bronze.* table references
for consistency with the medallion architecture.
"""

import re
from pathlib import Path

from typing import List, Union

def list_python_files(
    root: Union[str, Path],
    recursive: bool = True,
    absolute: bool = False,
    ignore_hidden: bool = True,
) -> List[str]:
    """
    List all Python (.py) files under a root folder.

    Args:
        root: Root folder path.
        recursive: If True, scan sub-folders recursively.
        absolute: If True, return absolute paths; else relative to root.
        ignore_hidden: If True, skip hidden folders/files (starting with '.').

    Returns:
        A sorted list of file paths (as strings).
    """
    root = Path(root).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Root path does not exist: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Root path is not a directory: {root}")

    pattern = "**/*.py" if recursive else "*.py"
    files = []

    for p in root.glob(pattern):
        if not p.is_file():
            continue

        # skip hidden files/folders if requested
        if ignore_hidden:
            # any part begins with "."
            if any(part.startswith(".") for part in p.relative_to(root).parts):
                continue

        files.append(str(p if absolute else p.relative_to(root)))

    return sorted(files)



def update_table_references(file_path):
    """Update plan_info and formulary table references to bronze.* schema."""
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    
    # Pattern: FROM/JOIN plan_info (but not bronze.plan_info)
    content = re.sub(
        r'(FROM|JOIN)\s+plan_info\b',
        r'\1 bronze.brz_plan_info',
        content
    )
    
    # Pattern: FROM/JOIN basic_drugs_formulary
    content = re.sub(
        r'(FROM|JOIN)\s+basic_drugs_formulary\b',
        r'\1 bronze.brz_basic_formulary',
        content
    )
    
    # Pattern: FROM/JOIN beneficiary_cost
    content = re.sub(
        r'(FROM|JOIN)\s+beneficiary_cost\b',
        r'\1 bronze.brz_beneficiary_cost',
        content
    )
    
    # Pattern: FROM/JOIN insulin_cost
    content = re.sub(
        r'(FROM|JOIN)\s+insulin_cost\b',
        r'\1 bronze.brz_insulin_cost',
        content
    )
    
    # Pattern: FROM/JOIN geographic
    content = re.sub(
        r'(FROM|JOIN)\s+geographic\b',
        r'\1 bronze.brz_geographic',
        content
    )
    
    # Pattern: FROM/JOIN pharmacy_networks
    content = re.sub(
        r'(FROM|JOIN)\s+pharmacy_networks\b',
        r'\1 bronze.brz_pharmacy_networks',
        content
    )
    
    if content != original_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    
    return False


if __name__ == "__main__":
    # db_dir = Path(__file__).parent
    # print(db_dir)
    ROOT_DIR = r"C:\Users\nghia.n\OneDrive - COLLECTIUS SYSTEMS PTE. LTD\Documents\1.Personal\1.Learning\1. Practice\master\thesis\agent-code"

    py_files = list_python_files(
        ROOT_DIR,
        recursive=True,
        absolute=True,      # set True if you want full paths
        ignore_hidden=True,
    )
    
    changed_files = []
    
    # print(py_files)
    for py_file in py_files:
        if "fix_table_references" in py_file:
            continue
        
        if update_table_references(py_file):
            changed_files.append(py_file)
    
    print(f"Updated {len(changed_files)} files:")
    for f in changed_files:
        print(f"  - {f}")
