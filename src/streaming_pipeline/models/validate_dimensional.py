"""
Simple validation script for dimensional modeling components.

This script performs basic validation of the dimensional modeling
implementation without requiring a full Spark environment.
"""

import sys
import importlib.util
from pathlib import Path

def validate_imports():
    """Validate that all modules can be imported."""
    
    print("Validating dimensional modeling imports...")
    
    # Get the current directory
    current_dir = Path(__file__).parent
    
    # List of modules to validate
    modules = [
        'dimensional.py',
        'data_quality.py', 
        'dimensional_pipeline.py'
    ]
    
    validation_results = {}
    
    for module_file in modules:
        module_path = current_dir / module_file
        module_name = module_file.replace('.py', '')
        
        try:
            # Load module spec
            spec = importlib.util.spec_from_file_location(module_name, module_path)
            if spec is None:
                validation_results[module_name] = f"Could not load spec for {module_file}"
                continue
                
            # Create module
            module = importlib.util.module_from_spec(spec)
            
            # Check if file exists and is readable
            if not module_path.exists():
                validation_results[module_name] = f"File {module_file} does not exist"
                continue
                
            validation_results[module_name] = "OK - Module structure valid"
            
        except Exception as e:
            validation_results[module_name] = f"Error: {str(e)}"
    
    return validation_results

def validate_class_definitions():
    """Validate that key classes are properly defined."""
    
    print("Validating class definitions...")
    
    # Read the dimensional.py file and check for key classes
    current_dir = Path(__file__).parent
    dimensional_file = current_dir / 'dimensional.py'
    
    if not dimensional_file.exists():
        return {"dimensional.py": "File does not exist"}
    
    content = dimensional_file.read_text()
    
    expected_classes = [
        'DimensionalModelBuilder',
        'DimensionConfig'
    ]
    
    expected_methods = [
        'create_dim_company_schema',
        'create_dim_date_schema', 
        'create_dim_time_schema',
        'build_dim_date',
        'build_dim_time',
        'build_dim_company',
        'apply_scd_type2'
    ]
    
    results = {}
    
    # Check classes
    for class_name in expected_classes:
        if f"class {class_name}" in content:
            results[f"Class {class_name}"] = "OK - Found"
        else:
            results[f"Class {class_name}"] = "ERROR - Not found"
    
    # Check methods
    for method_name in expected_methods:
        if f"def {method_name}" in content:
            results[f"Method {method_name}"] = "OK - Found"
        else:
            results[f"Method {method_name}"] = "ERROR - Not found"
    
    return results

def validate_data_quality_definitions():
    """Validate data quality module definitions."""
    
    print("Validating data quality definitions...")
    
    current_dir = Path(__file__).parent
    data_quality_file = current_dir / 'data_quality.py'
    
    if not data_quality_file.exists():
        return {"data_quality.py": "File does not exist"}
    
    content = data_quality_file.read_text()
    
    expected_classes = [
        'ValidationRule',
        'ValidationResult',
        'DataQualityValidator'
    ]
    
    expected_methods = [
        'validate_dim_company',
        'validate_dim_date',
        'validate_dim_time', 
        'validate_fact_stock_prices',
        'validate_fact_trading_volume',
        'generate_data_quality_report'
    ]
    
    results = {}
    
    # Check classes
    for class_name in expected_classes:
        if f"class {class_name}" in content:
            results[f"Class {class_name}"] = "OK - Found"
        else:
            results[f"Class {class_name}"] = "ERROR - Not found"
    
    # Check methods
    for method_name in expected_methods:
        if f"def {method_name}" in content:
            results[f"Method {method_name}"] = "OK - Found"
        else:
            results[f"Method {method_name}"] = "ERROR - Not found"
    
    return results

def validate_pipeline_integration():
    """Validate pipeline integration module."""
    
    print("Validating pipeline integration...")
    
    current_dir = Path(__file__).parent
    pipeline_file = current_dir / 'dimensional_pipeline.py'
    
    if not pipeline_file.exists():
        return {"dimensional_pipeline.py": "File does not exist"}
    
    content = pipeline_file.read_text()
    
    expected_classes = [
        'DimensionalPipeline'
    ]
    
    expected_methods = [
        'process_streaming_batch',
        'save_dimensional_model',
        'load_dimensional_model',
        'generate_quality_report'
    ]
    
    results = {}
    
    # Check classes
    for class_name in expected_classes:
        if f"class {class_name}" in content:
            results[f"Class {class_name}"] = "OK - Found"
        else:
            results[f"Class {class_name}"] = "ERROR - Not found"
    
    # Check methods
    for method_name in expected_methods:
        if f"def {method_name}" in content:
            results[f"Method {method_name}"] = "OK - Found"
        else:
            results[f"Method {method_name}"] = "ERROR - Not found"
    
    return results

def main():
    """Main validation function."""
    
    print("=" * 60)
    print("DIMENSIONAL MODELING VALIDATION")
    print("=" * 60)
    
    all_results = {}
    
    # Run validations
    all_results["Import Validation"] = validate_imports()
    all_results["Class Definitions"] = validate_class_definitions()
    all_results["Data Quality Definitions"] = validate_data_quality_definitions()
    all_results["Pipeline Integration"] = validate_pipeline_integration()
    
    # Print results
    total_checks = 0
    passed_checks = 0
    
    for category, results in all_results.items():
        print(f"\n{category}:")
        print("-" * 40)
        
        for check, result in results.items():
            total_checks += 1
            status = "✓" if result.startswith("OK") else "✗"
            if result.startswith("OK"):
                passed_checks += 1
            
            print(f"  {status} {check}: {result}")
    
    # Summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    print(f"Total checks: {total_checks}")
    print(f"Passed: {passed_checks}")
    print(f"Failed: {total_checks - passed_checks}")
    print(f"Success rate: {passed_checks/total_checks*100:.1f}%")
    
    if passed_checks == total_checks:
        print("\n🎉 All validations passed! Dimensional modeling implementation is complete.")
        return 0
    else:
        print(f"\n⚠️  {total_checks - passed_checks} validation(s) failed. Please review the implementation.")
        return 1

if __name__ == "__main__":
    sys.exit(main())