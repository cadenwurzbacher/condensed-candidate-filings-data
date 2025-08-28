# CandidateFilings Pipeline Tests

This directory contains comprehensive tests for the CandidateFilings.com data processing pipeline.

## 🧪 **Test Coverage**

### **Core Pipeline Tests** (`test_main_pipeline.py`)
- ✅ Pipeline initialization and configuration
- ✅ Raw data file discovery and processing
- ✅ State file merging and validation
- ✅ Office standardization integration
- ✅ State cleaner mapping validation
- ✅ Pipeline status tracking
- ✅ Error handling in file operations
- ✅ Cleanup operations

### **Office Standardizer Tests** (`test_office_standardizer.py`)
- ✅ Standardizer initialization and configuration
- ✅ Office name categorization
- ✅ Confidence scoring
- ✅ Partial matching algorithms
- ✅ Edge case handling
- ✅ Dataset standardization
- ✅ Category mapping validation

### **Error Handling Tests** (`test_error_handling.py`)
- ✅ Graceful degradation on component failures
- ✅ Empty DataFrame handling
- ✅ Missing column handling
- ✅ File I/O error handling
- ✅ Database connection failure handling
- ✅ Office standardization failure handling
- ✅ Audit failure handling
- ✅ Progress tracking validation
- ✅ Error logging capabilities

### **Database Tests** (`test_database.py`)
- ✅ Database manager initialization
- ✅ Connection success/failure handling
- ✅ Query execution
- ✅ DataFrame uploads
- ✅ Table operations
- ✅ Environment variable validation
- ✅ Chunked upload for large datasets
- ✅ Connection string formatting

## 🚀 **Running Tests**

### **Install Testing Dependencies**
```bash
pip install -r requirements.txt
```

### **Run All Tests**
```bash
# From project root
python -m pytest tests/ -v

# Or use the test runner
python tests/run_tests.py
```

### **Run Specific Test Files**
```bash
# Run main pipeline tests only
python tests/run_tests.py test_main_pipeline.py

# Run error handling tests only
python tests/run_tests.py test_error_handling.py

# Run office standardizer tests only
python tests/run_tests.py test_office_standardizer.py

# Run database tests only
python tests/run_tests.py test_database.py
```

### **Run with Coverage**
```bash
python -m pytest tests/ --cov=src --cov-report=html
```

## 📋 **Test Categories**

### **Unit Tests**
- Individual component functionality
- Method behavior validation
- Input/output validation
- Edge case handling

### **Integration Tests**
- Component interaction testing
- Data flow validation
- Pipeline stage coordination
- Error propagation testing

### **Error Handling Tests**
- Failure scenario testing
- Graceful degradation validation
- Error logging verification
- Recovery mechanism testing

### **Data Validation Tests**
- Schema compliance checking
- Data quality validation
- File format handling
- Column validation

## 🔧 **Test Configuration**

### **Fixtures** (`conftest.py`)
- `sample_candidate_data`: Sample candidate data for testing
- `temp_data_dir`: Temporary directory for test files
- `mock_database`: Mock database connection
- `sample_state_files`: Sample state data files

### **Mocking Strategy**
- Database connections mocked for unit testing
- File I/O operations use temporary directories
- State cleaners mocked for failure testing
- Environment variables mocked for database tests

## 📊 **Test Results**

### **Expected Output**
```
🧪 Running CandidateFilings Pipeline Tests...
==================================================
test_main_pipeline.py::TestMainPipeline::test_pipeline_initialization PASSED
test_main_pipeline.py::TestMainPipeline::test_pipeline_creates_directories PASSED
...
🎉 All tests passed!
```

### **Test Statistics**
- **Total Tests**: 40+ test methods
- **Coverage Areas**: Core pipeline, office standardizer, error handling, database
- **Test Types**: Unit, integration, error handling, validation
- **Mock Usage**: Database, file I/O, external dependencies

## 🚨 **Troubleshooting**

### **Common Test Issues**
1. **Import Errors**: Ensure you're running from project root
2. **Missing Dependencies**: Install with `pip install -r requirements.txt`
3. **File Permission Issues**: Check write access to temp directories
4. **Mock Configuration**: Verify mock setup in test files

### **Test Debugging**
```bash
# Run with detailed output
python -m pytest tests/ -v -s

# Run single test method
python -m pytest tests/test_main_pipeline.py::TestMainPipeline::test_pipeline_initialization -v

# Run with print statements
python -m pytest tests/ -s
```

## 🎯 **Adding New Tests**

### **Test File Structure**
```python
"""
Tests for [component name].
"""

import pytest
from [component_path] import [ComponentClass]

class Test[ComponentName]:
    """Test the [component name] component."""
    
    def test_[specific_functionality](self):
        """Test [specific functionality description]."""
        # Test implementation
        assert True
```

### **Test Naming Convention**
- Test classes: `Test[ComponentName]`
- Test methods: `test_[functionality_description]`
- Use descriptive names that explain what is being tested

### **Test Organization**
- Group related tests in the same class
- Use fixtures for common test data
- Mock external dependencies
- Test both success and failure scenarios

## 📈 **Continuous Integration**

### **GitHub Actions** (Recommended)
```yaml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.9
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests
        run: python -m pytest tests/ -v
```

### **Pre-commit Hooks**
```bash
# Install pre-commit
pip install pre-commit

# Run tests before commit
pre-commit install
```

## 🏆 **Test Quality Standards**

### **Coverage Requirements**
- **Minimum Coverage**: 80%
- **Critical Paths**: 100% coverage
- **Error Handling**: 90% coverage
- **Integration Points**: 95% coverage

### **Test Quality Checklist**
- [ ] Tests are descriptive and readable
- [ ] Edge cases are covered
- [ ] Error scenarios are tested
- [ ] Mocks are properly configured
- [ ] Test data is realistic
- [ ] Assertions are specific
- [ ] Tests are independent
- [ ] Cleanup is handled properly

## 📚 **Additional Resources**

- **Pytest Documentation**: https://docs.pytest.org/
- **Mock Documentation**: https://docs.python.org/3/library/unittest.mock.html
- **Testing Best Practices**: https://realpython.com/python-testing/
- **Pipeline Testing Patterns**: https://martinfowler.com/articles/microservice-testing/
