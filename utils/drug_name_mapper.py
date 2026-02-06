"""
Drug Name to NDC Mapping Module

This module provides drug name search functionality to make the UI more user-friendly.
Instead of requiring users to know NDC codes, they can search by drug name.

Data Source:
- Basic formulary table contains NDC codes and some drug names
- This creates a lookup table for common medications

Enhancement:
In production, this would integrate with FDA NDC Directory or RxNorm API for
comprehensive drug name → NDC mapping.
"""

import pandas as pd
import numpy as np
from pathlib import Path


class DrugNameMapper:
    """
    Map drug names to NDC codes for user-friendly search.
    """
    
    def __init__(self, formulary_path='data/SPUF/basic_formulary.parquet'):
        """
        Initialize drug name mapper.
        
        Args:
            formulary_path (str): Path to basic formulary parquet file
        """
        # Load formulary (contains NDC and potentially drug names)
        self.formulary = pd.read_parquet(formulary_path)
        
        # Create manual mapping for common drugs (curated list)
        # In production, this would be loaded from comprehensive NDC directory
        self.drug_name_map = self._create_drug_name_map()
        
        print(f"✓ Drug mapper initialized with {len(self.drug_name_map):,} drug entries")
    
    def _create_drug_name_map(self):
        """
        Create drug name to NDC mapping.
        
        This combines:
        1. Manual curated list of common medications
        2. Automated extraction from formulary (if drug names are present)
        
        Returns:
            dict: {drug_name_lower: [ndc1, ndc2, ...]}
        """
        # Manual curated list of common Medicare Part D medications
        # This ensures common drugs are searchable
        manual_mappings = {
            # === Insulin (Common) ===
            'insulin lispro': ['00002871501', '00002871517', '00002871591', '00002871599'],
            'humalog': ['00002871501', '00002871517', '00002871591', '00002871599'],
            'insulin': ['00002871501', '00002871517', '00002871591', '00002871599'],
            
            # === Diabetes Medications ===
            'metformin': ['68992301001', '68992301003'],
            'metformin hcl': ['68992301001', '68992301003'],
            'glucophage': ['68992301001', '68992301003'],
            
            # === Cardiovascular ===
            'lisinopril': ['68788974706'],
            'atorvastatin': ['00143971401'],
            'lipitor': ['00143971401'],
            
            # === Common Generic Drugs ===
            'amlodipine': ['00172375980'],
            'omeprazole': ['00527141301'],
            'losartan': ['00527101210'],
            'levothyroxine': ['00000540100'],
            'synthroid': ['00000540100'],
        }
        
        # Convert to lowercase for case-insensitive search
        drug_map = {}
        for drug_name, ndc_list in manual_mappings.items():
            drug_map[drug_name.lower()] = ndc_list
        
        # TODO: Extract from formulary if drug names are available
        # This would require additional data fields in SPUF
        
        return drug_map
    
    def search_drugs(self, query, limit=10):
        """
        Search for drugs by name.
        
        Args:
            query (str): Drug name search query
            limit (int): Maximum number of results to return
        
        Returns:
            list: List of {drug_name, ndc, relevance} dicts
        """
        query_lower = query.lower().strip()
        
        if len(query_lower) < 2:
            return []  # Require at least 2 characters
        
        # Search in drug name map
        results = []
        
        for drug_name, ndc_list in self.drug_name_map.items():
            # Check if query matches drug name
            if query_lower in drug_name:
                # Calculate relevance (exact match = higher score)
                relevance = 1.0 if query_lower == drug_name else 0.5
                
                # Add all NDCs for this drug
                for ndc in ndc_list:
                    results.append({
                        'drug_name': drug_name.title(),
                        'ndc': ndc,
                        'relevance': relevance
                    })
        
        # Sort by relevance
        results = sorted(results, key=lambda x: x['relevance'], reverse=True)
        
        return results[:limit]
    
    def get_drug_name(self, ndc):
        """
        Get drug name for an NDC code (reverse lookup).
        
        Args:
            ndc (str): NDC code
        
        Returns:
            str: Drug name, or "Unknown Drug" if not found
        """
        for drug_name, ndc_list in self.drug_name_map.items():
            if ndc in ndc_list:
                return drug_name.title()
        
        return f"Drug (NDC {ndc})"
    
    def get_autocomplete_suggestions(self, query, limit=5):
        """
        Get autocomplete suggestions for drug search input.
        
        Args:
            query (str): Partial drug name
            limit (int): Maximum number of suggestions
        
        Returns:
            list: List of suggested drug names
        """
        query_lower = query.lower().strip()
        
        if len(query_lower) < 2:
            return []
        
        # Find matching drug names
        suggestions = []
        for drug_name in self.drug_name_map.keys():
            if query_lower in drug_name:
                # Calculate match quality
                if drug_name.startswith(query_lower):
                    priority = 2  # Starts with query = highest priority
                else:
                    priority = 1  # Contains query = lower priority
                
                suggestions.append((drug_name.title(), priority))
        
        # Sort by priority and alphabetically
        suggestions = sorted(suggestions, key=lambda x: (-x[1], x[0]))
        
        return [name for name, _ in suggestions[:limit]]


def main():
    """
    Demo usage of DrugNameMapper.
    """
    print("="*60)
    print("Drug Name Search Demo")
    print("="*60)
    
    # Initialize mapper
    mapper = DrugNameMapper()
    
    # Test searches
    test_queries = ['insulin', 'metformin', 'lipitor', 'diabetes']
    
    for query in test_queries:
        print(f"\nSearch: '{query}'")
        results = mapper.search_drugs(query, limit=5)
        
        if results:
            print(f"  Found {len(results)} results:")
            for result in results:
                print(f"    - {result['drug_name']}: {result['ndc']}")
        else:
            print("  No results found")
    
    # Test autocomplete
    print(f"\n\nAutocomplete for 'insu':")
    suggestions = mapper.get_autocomplete_suggestions('insu')
    for suggestion in suggestions:
        print(f"  - {suggestion}")


if __name__ == "__main__":
    main()
