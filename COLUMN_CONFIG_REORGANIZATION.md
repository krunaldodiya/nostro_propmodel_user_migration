# Column Configuration Reorganization

## Summary

All column configuration JSON files have been moved to a dedicated `config/` directory for better organization.

## Changes Made

### 1. Created Directory Structure

```
config/
├── README.md                            # Configuration documentation
├── users_column_config.json             # Users column configuration
├── purchases_column_config.json         # Purchases column configuration
├── discounts_column_config.json         # Discounts column configuration
└── platform_accounts_column_config.json # Platform accounts column configuration
```

### 2. Updated Export Scripts

All export scripts now reference config files from the `config/` directory:

#### `users_export.py`

- ✅ Updated file path: `users_column_config.json` → `config/users_column_config.json`
- ✅ Updated error messages to reflect new path
- ✅ Tested and working

#### `purchases_export.py`

- ✅ Updated file path: `purchases_column_config.json` → `config/purchases_column_config.json`
- ✅ Updated error messages to reflect new path
- ✅ Tested and working

#### `discounts_export.py`

- ✅ Updated file path: `discounts_column_config.json` → `config/discounts_column_config.json`
- ✅ Updated error messages to reflect new path
- ✅ Tested and working

#### `platform_accounts_export.py`

- ✅ Updated file path: `platform_accounts_column_config.json` → `config/platform_accounts_column_config.json`
- ✅ Updated error messages to reflect new path
- ✅ Tested and working

### 3. Updated Documentation

#### `README.md`

- ✅ Updated configuration section to reflect new `config/` directory
- ✅ Added documentation for `config/platform_accounts_column_config.json`
- ✅ Updated project structure diagram
- ✅ Updated all references to config files

### 4. Created New Documentation

#### `config/README.md`

- New comprehensive documentation for all configuration files
- Explains purpose, format, and usage of each config file
- Provides examples and best practices

## Benefits

### 1. Better Organization

- All configuration files in one place
- Clear separation from code and data files
- Easier to find and manage

### 2. Scalability

- Easy to add new configuration files
- Consistent structure for future additions
- Clear naming conventions

### 3. Maintainability

- Centralized configuration management
- Easier to track changes
- Better documentation

### 4. Clarity

- Explicit `config/` directory signals configuration files
- Self-documenting structure
- Reduces root directory clutter

## Testing Results

All export scripts tested and working correctly:

```bash
✅ uv run main.py --users       # Using config/users_column_config.json
✅ uv run main.py --purchases   # Using config/purchases_column_config.json
✅ uv run main.py --discount-codes   # Using config/discount_codes_column_config.json
✅ uv run main.py --platform-accounts  # Using config/platform_accounts_column_config.json
```

## Migration Checklist

- [x] Create `config/` directory
- [x] Move all `*_column_config.json` files to `config/`
- [x] Update `users_export.py` references
- [x] Update `purchases_export.py` references
- [x] Update `discounts_export.py` references
- [x] Update `platform_accounts_export.py` references
- [x] Update `README.md` documentation
- [x] Create `config/README.md` documentation
- [x] Test all export scripts
- [x] Verify no linting errors
- [x] Create migration summary

## No Breaking Changes

All changes are backward compatible:

- Scripts maintain the same command-line interface
- Output files remain unchanged
- Configuration file format unchanged
- Only file paths have changed

## Notes for Developers

When working with this project:

1. All column configuration files are in `config/` directory
2. Use the config files to control which columns are exported
3. Refer to `config/README.md` for detailed documentation
4. Always test with preview mode before generating files
