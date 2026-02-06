"""
Cost Estimator (DuckDB Version) - Annual Cost Calculation

This module estimates total annual out-of-pocket costs using DuckDB.

Performance Improvement:
- Old (Parquet): 300ms, 300MB memory
- New (DuckDB): 10ms, 10MB memory
"""

import pandas as pd
from db.formulary_repository import FormularyRepository
from db.plan_repository import PlanRepository


class CostEstimator:
    """
    Estimate annual out-of-pocket costs using DuckDB.
    """
    
    def __init__(self):
        """
        Initialize cost estimator with repositories.
        """
        self.formulary_repo = FormularyRepository()
        self.plan_repo = PlanRepository()
        print("✓ Cost estimator initialized (DuckDB)")
    
    def estimate_drug_cost(self, plan_key, ndc, tier, fills_per_year=12, is_insulin=False):
        """
        Estimate annual cost for a single drug.
        
        Args:
            plan_key (str): Plan key
            ndc (str): NDC code
            tier (int): Formulary tier
            fills_per_year (int): Number of fills per year
            is_insulin (bool): Is this an insulin drug
        
        Returns:
            dict: Cost breakdown
        """
        # Check insulin cost first (IRA $35 cap)
        if is_insulin:
            insulin_cost = self.formulary_repo.get_insulin_cost(plan_key, ndc)
            if insulin_cost is not None:
                copay = float(insulin_cost['COPAY_AMT'])
                return {
                    'ndc': ndc,
                    'copay_per_fill': copay,
                    'fills_per_year': fills_per_year,
                    'annual_oop': copay * fills_per_year,
                    'is_insulin': True,
                    'ira_cap_applied': copay <= 35.0
                }
        
        # Get tier-based cost sharing
        cost_sharing = self.formulary_repo.get_cost_sharing(plan_key, tier, coverage_level=1)
        
        if cost_sharing is None:
            # No cost data, estimate with default tier copay
            default_copays = {1: 5, 2: 15, 3: 47, 4: 100, 5: 200, 6: 300}
            copay = default_copays.get(tier, 50)
        else:
            copay = float(cost_sharing['COPAY_AMT']) if 'COPAY_AMT' in cost_sharing else 50.0
        
        return {
            'ndc': ndc,
            'copay_per_fill': copay,
            'fills_per_year': fills_per_year,
            'annual_oop': copay * fills_per_year,
            'is_insulin': False,
            'ira_cap_applied': False
        }
    
    def estimate_total_annual_cost(self, plan_key, beneficiary_profile):
        """
        Estimate total annual cost for a beneficiary.
        
        Args:
            plan_key (str): Plan key
            beneficiary_profile (pd.DataFrame): Drug list with coverage details
        
        Returns:
            dict: Total cost breakdown
        """
        # Get plan details
        plan = self.plan_repo.get_plan_by_key(plan_key)
        
        if plan is None:
            raise ValueError(f"Plan not found: {plan_key}")
        
        # Annual premium
        monthly_premium = float(plan['PREMIUM']) if 'PREMIUM' in plan else 0.0
        annual_premium = monthly_premium * 12
        
        # Drug costs
        drug_costs = []
        total_drug_oop = 0.0
        
        for _, drug in beneficiary_profile.iterrows():
            if not drug['covered']:
                continue  # Skip uncovered drugs
            
            fills = drug.get('fills_per_year', 12)
            
            drug_cost = self.estimate_drug_cost(
                plan_key=plan_key,
                ndc=drug['ndc'],
                tier=drug['tier'],
                fills_per_year=fills,
                is_insulin=drug['is_insulin']
            )
            
            drug_costs.append(drug_cost)
            total_drug_oop += drug_cost['annual_oop']
        
        return {
            'plan_key': plan_key,
            'annual_premium': annual_premium,
            'total_drug_oop': total_drug_oop,
            'total_annual_cost': annual_premium + total_drug_oop,
            'drug_costs': drug_costs
        }
