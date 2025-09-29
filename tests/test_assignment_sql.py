import pytest
from unittest.mock import patch, Mock
from gxt.commands.run import build_assignment_sql


@pytest.mark.unit
class TestAssignmentSQL:
    """Test the assignment SQL generation logic."""
    
    def test_build_assignment_sql_basic(self):
        """Test basic assignment SQL generation."""
        # Test data - use actual function signature
        audience_sql = "SELECT user_id FROM test_table"
        hash_sql_expr = "MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(user_id AS STRING),'::','10'))), 1000000)/1000000.0"
        variants = [
            {"name": "control", "exposure": 0.5},
            {"name": "treatment", "exposure": 0.5}
        ]
        randomization_unit = "user_id"
        
        # Call the function
        result = build_assignment_sql(
            audience_sql=audience_sql,
            hash_sql_expr=hash_sql_expr,
            variants=variants,
            randomization_unit=randomization_unit
        )
        
        # Assertions
        assert audience_sql in result
        assert hash_sql_expr in result
        assert "control" in result
        assert "treatment" in result
        assert randomization_unit in result
        
        # Check SQL structure
        assert "WITH audience AS" in result
        assert "SELECT" in result
        assert "CASE" in result
        assert "WHEN" in result
    
    def test_build_assignment_sql_with_different_variants(self):
        """Test assignment SQL with different variant configurations."""
        audience_sql = "SELECT user_id FROM `project.dataset.table`"
        hash_sql_expr = "MOD(ABS(FARM_FINGERPRINT(user_id)), 1000000)/1000000.0"
        variants = [
            {"name": "control", "exposure": 0.3},
            {"name": "treatment_a", "exposure": 0.3},
            {"name": "treatment_b", "exposure": 0.4}
        ]
        randomization_unit = "user_id"
        
        result = build_assignment_sql(
            audience_sql=audience_sql,
            hash_sql_expr=hash_sql_expr,
            variants=variants,
            randomization_unit=randomization_unit
        )
        
        assert "control" in result
        assert "treatment_a" in result
        assert "treatment_b" in result
        assert "`project.dataset.table`" in result
        
    def test_build_assignment_sql_different_randomization_units(self):
        """Test assignment SQL with different randomization units."""
        base_params = {
            "audience_sql": "SELECT account_id FROM test_table",
            "hash_sql_expr": "MOD(ABS(FARM_FINGERPRINT(account_id)), 1000000)/1000000.0",
            "variants": [{"name": "control", "exposure": 0.5}, {"name": "treatment", "exposure": 0.5}]
        }
        
        for unit in ["account_id", "customer_id", "email"]:
            result = build_assignment_sql(
                **base_params,
                randomization_unit=unit
            )
            
            assert unit in result
    
    def test_build_assignment_sql_preserves_audience_logic(self):
        """Test that complex audience SQL is preserved correctly."""
        audience_sql = """
        SELECT DISTINCT user_id 
        FROM user_table u
        JOIN segment_table s ON u.user_id = s.user_id
        WHERE s.segment_name = 'premium'
          AND u.created_date >= '2024-01-01'
        """
        hash_sql_expr = "MOD(ABS(FARM_FINGERPRINT(user_id)), 1000000)/1000000.0"
        variants = [{"name": "control", "exposure": 0.6}, {"name": "treatment", "exposure": 0.4}]
        randomization_unit = "user_id"
        
        result = build_assignment_sql(
            audience_sql=audience_sql,
            hash_sql_expr=hash_sql_expr,
            variants=variants,
            randomization_unit=randomization_unit
        )
        
        # The audience SQL should be preserved in the CTE
        assert "WITH audience AS" in result
        assert "SELECT DISTINCT user_id" in result
        assert "JOIN segment_table s" in result
        assert "WHERE s.segment_name = 'premium'" in result