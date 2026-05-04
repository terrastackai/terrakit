# Agent Guidelines for Terrakit Development

## Environment Setup

### Virtual Environment
Always source the project's virtual environment before running any commands:

```bash
source .venv/bin/activate
```

For commands that need the virtual environment, use:
```bash
source .venv/bin/activate && <command>
```

## Test-Driven Development (TDD)

### Core Principles

1. **Write Tests First**: Before implementing any feature or fix, write the test that defines the expected behavior
2. **Red-Green-Refactor Cycle**:
   - **Red**: Write a failing test
   - **Green**: Write minimal code to make the test pass
   - **Refactor**: Improve code while keeping tests green

### Testing Structure

The project follows a structured testing approach:

```
tests/
├── component_tests/          # Unit and component-level tests
│   ├── chip/                # Tiling functionality tests
│   ├── download/            # Download functionality tests
│   │   └── data_connectors/ # Individual connector tests
│   ├── general_utils/       # Utility function tests
│   ├── store/               # Storage functionality tests
│   └── transform/           # Transformation tests
└── integration_tests/        # End-to-end integration tests
```

### Test Development Workflow

#### 1. Before Making Changes
- Read existing tests to understand current behavior
- Identify what needs to be tested
- Check test coverage for the area you're modifying

#### 2. Writing Tests
```bash
# Run tests to ensure current state
source .venv/bin/activate && pytest tests/component_tests/<module>/ -v

# Write your test first
# Example: tests/component_tests/download/test_new_feature.py
```

#### 3. Test Naming Conventions
- Test files: `test_<module_name>.py`
- Test functions: `test_<specific_behavior>()`
- Use descriptive names that explain what is being tested

#### 4. Running Tests

Run all tests:
```bash
source .venv/bin/activate && pytest
```

Run specific test file:
```bash
source .venv/bin/activate && pytest tests/component_tests/download/test_download_data.py -v
```

Run specific test:
```bash
source .venv/bin/activate && pytest tests/component_tests/download/test_download_data.py::test_specific_function -v
```

Run with coverage:
```bash
source .venv/bin/activate && pytest --cov=terrakit --cov-report=html
```

#### 5. Test Fixtures
- Use `conftest.py` files for shared fixtures
- Keep fixtures close to where they're used
- Document complex fixtures

### Development Process

#### Adding New Features

1. **Understand Requirements**
   - Read related documentation in `docs/`
   - Review existing similar implementations
   - Check `tests/` for existing test patterns

2. **Write Tests First**
   ```bash
   # Create test file
   # Write failing tests that define expected behavior
   source .venv/bin/activate && pytest tests/component_tests/<module>/test_new_feature.py -v
   # Tests should fail (Red)
   ```

3. **Implement Feature**
   - Write minimal code to pass tests
   - Follow existing code patterns
   - Keep implementation simple

4. **Verify Tests Pass**
   ```bash
   source .venv/bin/activate && pytest tests/component_tests/<module>/test_new_feature.py -v
   # Tests should pass (Green)
   ```

5. **Refactor**
   - Improve code quality
   - Ensure tests still pass
   - Add documentation

6. **Run Full Test Suite**
   ```bash
   source .venv/bin/activate && pytest
   ```

#### Fixing Bugs

1. **Write Failing Test**
   - Create a test that reproduces the bug
   - Verify the test fails

2. **Fix the Bug**
   - Implement the fix
   - Ensure the new test passes

3. **Verify No Regressions**
   ```bash
   source .venv/bin/activate && pytest
   ```

#### Modifying Existing Code

1. **Read Existing Tests**
   ```bash
   source .venv/bin/activate && pytest tests/component_tests/<module>/ -v
   ```

2. **Update Tests First**
   - Modify tests to reflect new expected behavior
   - Ensure tests fail appropriately

3. **Update Implementation**
   - Make code changes
   - Verify tests pass

### Code Quality Checks

Before committing, always run:

```bash
# Activate environment
source .venv/bin/activate

# Run tests
pytest

# Run type checking
mypy terrakit/

# Run linting (if configured)
# pre-commit hooks will run automatically on commit
```

### Testing Best Practices

1. **Test Independence**: Each test should be independent and not rely on other tests
2. **Clear Assertions**: Use descriptive assertion messages
3. **Mock External Dependencies**: Use mocks for external APIs, file systems, etc.
4. **Test Edge Cases**: Include tests for boundary conditions and error cases
5. **Keep Tests Fast**: Unit tests should run quickly; use integration tests for slower operations
6. **Maintain Test Data**: Store test resources in `tests/resources/`

### Common Testing Patterns

#### Testing Data Connectors
```python
# See: tests/component_tests/download/data_connectors/
# - Mock external API calls
# - Test parameter validation
# - Test data transformation
# - Test error handling
```

#### Testing Transformations
```python
# See: tests/component_tests/transform/
# - Test with sample data
# - Verify output format
# - Test edge cases (empty data, invalid data)
```

#### Testing Utilities
```python
# See: tests/component_tests/general_utils/
# - Test pure functions
# - Test with various input types
# - Test error conditions
```

### Integration Testing

Integration tests are in `tests/integration_tests/` and may require:
- External API credentials (stored in `.env`)
- Longer execution time
- Network access

Run integration tests separately:
```bash
source .venv/bin/activate && pytest tests/integration_tests/ -v
```

### Continuous Integration

- All tests must pass before merging
- Pre-commit hooks enforce code quality
- Check `.github/` for CI configuration

## Key Commands Reference

```bash
# Activate environment (always do this first)
source .venv/bin/activate

# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test directory
pytest tests/component_tests/download/ -v

# Run with coverage
pytest --cov=terrakit --cov-report=html

# Run type checking
mypy terrakit/

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

## Licensing and File Headers

### Apache 2.0 License

This project is licensed under the Apache License 2.0 and is copyright IBM Corporation.

### File Header Requirements

**All Python files must include a copyright header** with the following format:

```python
# © Copyright IBM Corporation YYYY[-YYYY]
# SPDX-License-Identifier: Apache-2.0
```

#### Header Rules

1. **New Files**: Use the current year
   ```python
   # © Copyright IBM Corporation 2026
   # SPDX-License-Identifier: Apache-2.0
   ```

2. **Modified Files**: Use year range from creation to current year
   ```python
   # © Copyright IBM Corporation 2025-2026
   # SPDX-License-Identifier: Apache-2.0
   ```

3. **Header Placement**: Must be at the very top of the file, before any imports or code

#### Automated Header Checking

The project uses `scripts/check_header.py` as a pre-commit hook to automatically:
- Validate all Python file headers
- Update copyright years when files are modified
- Add headers to new files

The script runs automatically on commit, but you can also run it manually:

```bash
# Check and update all Python files in a directory
source .venv/bin/activate && python scripts/check_header.py -d terrakit/

# Check and update specific files
source .venv/bin/activate && python scripts/check_header.py -f file1.py file2.py
```

#### Pre-commit Hook

The pre-commit hook will:
- Automatically update headers before each commit
- Fail the commit if headers are missing or incorrect
- Update the year range for modified files

If the pre-commit hook updates headers, you'll need to stage the changes and commit again:

```bash
git add .
git commit -m "Your commit message"
```

## Documentation

### Keeping Documentation in Sync

**Critical**: Documentation must always be kept in sync with code changes.

#### When to Update Documentation

1. **Adding New Features**
   - Update relevant documentation in `docs/`
   - Add API documentation in `docs/api/`
   - Create examples in `docs/examples/` when appropriate
   - Update README.md if the feature affects usage

2. **Modifying Existing Features**
   - Review and update affected documentation
   - Update code examples to reflect changes
   - Verify all references are still accurate

3. **Fixing Bugs**
   - Update documentation if the bug fix changes behavior
   - Correct any misleading documentation that may have contributed to the bug

4. **Changing APIs**
   - Update API documentation immediately
   - Mark deprecated features clearly
   - Provide migration guides for breaking changes

#### Documentation Checklist

Before completing any task, verify:
- [ ] Code changes are reflected in relevant `docs/` files
- [ ] API documentation in `docs/api/` is updated
- [ ] Examples in `docs/examples/` still work
- [ ] README.md is updated if necessary
- [ ] Docstrings in code match implementation
- [ ] Type hints are documented

#### Documentation Structure

```
docs/
├── index.md              # Main documentation entry point
├── *.md                  # Feature-specific guides
├── api/                  # API reference documentation
│   ├── *.md             # Module API docs
│   └── data_connectors/ # Connector-specific docs
└── examples/            # Usage examples and notebooks
```

Follow existing documentation patterns and style for consistency.

## Summary

**Always remember:**
1. ✅ Source `.venv/bin/activate` before any command
2. ✅ Write tests before code (TDD)
3. ✅ Run tests frequently during development
4. ✅ Ensure all tests pass before committing
5. ✅ Keep tests independent and fast
6. ✅ Document new features and changes