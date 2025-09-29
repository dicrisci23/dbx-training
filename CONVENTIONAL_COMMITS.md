# Conventional Commits Guide 🚀

This guide explains how to write commit messages that work with our automated release system.

## What are Conventional Commits?

Conventional Commits is a specification for writing commit messages in a standardized format that helps with:
- Automatic semantic versioning
- Automatic release note generation
- Better project history and communication

## Basic Format

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

## Commit Types

### 🚨 Major Version Bumps (Breaking Changes)
These commits will trigger a **major** version bump (e.g., 1.2.3 → 2.0.0):

- `BREAKING CHANGE:` - Any commit with "BREAKING CHANGE:" in the body or footer
- `feat!:` or `fix!:` - Any type followed by `!` indicates breaking changes
- `breaking:` - Direct breaking change commit

**Examples:**
```bash
feat!: remove deprecated API endpoints

BREAKING CHANGE: The legacy /v1/api endpoints have been removed.
Use /v2/api endpoints instead.

breaking: change default databricks target from dev to prod

This changes the default deployment target for all bundles.
```

### ✨ Minor Version Bumps (New Features)
These commits will trigger a **minor** version bump (e.g., 1.2.3 → 1.3.0):

- `feat:` - New feature
- `feature:` - New feature (alternative)

**Examples:**
```bash
feat: add user authentication to databricks bundles

feat(lineage): implement column-level lineage tracking

feature: add support for delta lake time travel queries
```

### 🐛 Patch Version Bumps (Bug Fixes & Improvements)
These commits will trigger a **patch** version bump (e.g., 1.2.3 → 1.2.4):

- `fix:` - Bug fix
- `bug:` - Bug fix (alternative)
- `patch:` - Direct patch
- `chore:` - Maintenance tasks
- `docs:` - Documentation updates
- `style:` - Code style changes
- `refactor:` - Code refactoring
- `perf:` - Performance improvements
- `test:` - Adding or updating tests

**Examples:**
```bash
fix: resolve bundle validation error for missing variables

bug: correct SQL syntax in lineage pipeline

chore: update databricks CLI to latest version

docs: update README with new bundle deployment instructions

refactor: optimize data transformation pipeline performance
```

## Scopes (Optional)

Scopes provide context about what part of the codebase changed:

```bash
feat(auth): add SSO integration
fix(bundles): resolve deployment timeout issues
docs(lineage): update API documentation
chore(deps): update python dependencies
```

### Common Scopes for This Project:
- `bundles` - Databricks bundle configurations
- `lineage` - Data lineage functionality  
- `auth` - Authentication related changes
- `pipelines` - Data pipeline changes
- `workflows` - GitHub Actions workflows
- `docs` - Documentation
- `deps` - Dependencies
- `config` - Configuration files

## Multi-line Commits

For more detailed commits, use body and footer sections:

```bash
feat(lineage): add real-time column lineage tracking

This implements real-time tracking of column-level data lineage
across all Databricks tables and views. The feature includes:

- Automatic schema detection
- Cross-database relationship mapping
- Historical lineage preservation
- REST API endpoints for querying lineage

Closes #123
Refs #124, #125
```

## Breaking Changes in Detail

Breaking changes must be communicated clearly:

```bash
feat!: migrate to Unity Catalog

BREAKING CHANGE: All existing Hive metastore tables must be migrated
to Unity Catalog before this release can be deployed.

Migration steps:
1. Run the migration script: scripts/migrate_to_uc.py
2. Update all bundle configurations to use UC syntax
3. Test deployments in dev environment
4. Deploy to production

See MIGRATION.md for detailed instructions.
```

## Release Automation Behavior

### Automatic Releases
When you push/merge to main branch, the system analyzes recent commits:

| Commit Type | Version Bump | Example |
|-------------|--------------|---------|
| `BREAKING CHANGE:`, `feat!:`, `fix!:`, `breaking:` | Major (1.0.0 → 2.0.0) | Breaking API changes |
| `feat:`, `feature:` | Minor (1.0.0 → 1.1.0) | New features |
| `fix:`, `bug:`, `chore:`, `docs:`, etc. | Patch (1.0.0 → 1.0.1) | Bug fixes, improvements |

### Release Notes Generation
Commits are automatically categorized in release notes:

- **⚠️ BREAKING CHANGES** - `BREAKING CHANGE:`, `feat!:`, etc.
- **✨ New Features** - `feat:`, `feature:`
- **🐛 Bug Fixes** - `fix:`, `bug:`
- **📚 Documentation** - `docs:`
- **🔧 Other Changes** - Everything else

## Examples for This Project

### Databricks Bundle Changes
```bash
feat(bundles): add automated testing environment setup
fix(bundles): resolve variable interpolation in production target
chore(bundles): update to latest databricks runtime version
```

### Data Lineage Features
```bash
feat(lineage): implement table-level lineage visualization
fix(lineage): correct cross-database relationship detection
perf(lineage): optimize lineage query performance
```

### System Tables Integration
```bash
feat(system-tables): add automated billing analysis queries
fix(system-tables): handle missing system table permissions
docs(system-tables): add query examples and best practices
```

### Utility and Configuration
```bash
chore: update python dependencies to latest versions
feat(utils): add data quality validation utilities
fix(config): resolve databricks workspace URL configuration
```

## Best Practices

### ✅ Good Examples
```bash
feat: add multi-workspace deployment support
fix(lineage): resolve null pointer in table scan
docs: update bundle deployment guide with troubleshooting
chore(deps): bump databricks-cli from 0.17.0 to 0.18.0
```

### ❌ Avoid These
```bash
# Too vague
fix: stuff
update: changes
misc: updates

# Not following convention
Added new feature for users
Fixed a bug
Updated documentation
```

## Manual Releases

You can also create manual releases using the GitHub Actions "Manual Release" workflow:

1. Go to Actions tab in GitHub
2. Select "Manual Release" workflow
3. Click "Run workflow"
4. Choose version type or provide custom version
5. Add custom release notes if needed

## Commit Message Templates

Save time with these git commit templates:

### Basic Templates
```bash
# Feature
feat: 

# Bug fix  
fix: 

# Documentation
docs: 

# Chore
chore: 
```

### Set up git commit template:
```bash
# Create template file
cat > ~/.gitmessage << EOF
# <type>[optional scope]: <description>
# 
# [optional body]
#
# [optional footer(s)]
#
# Types: feat, fix, docs, style, refactor, perf, test, chore
# Scopes: bundles, lineage, auth, pipelines, workflows, docs, deps, config
# 
# Examples:
# feat(bundles): add new deployment target
# fix(lineage): resolve schema detection issue
# docs: update API documentation
EOF

# Configure git to use the template
git config commit.template ~/.gitmessage
```

## Troubleshooting

### "No Release Created"
If no release is created automatically:
- Check that your commits follow conventional format
- Ensure you're pushing to main/master branch
- Verify commit messages contain recognized types
- Check GitHub Actions logs for details

### "Wrong Version Bump"
If the version bump is incorrect:
- Check commit message format
- Use `!` for breaking changes: `feat!:` not `feat:`
- Add `BREAKING CHANGE:` in commit body for major bumps
- Use correct types: `feat:` for minor, `fix:` for patch

### "Missing Changes in Release Notes"
If changes don't appear in release notes:
- Ensure commits follow conventional format
- Check that commit types are recognized
- Review the automatic categorization rules

## Questions?

- Check existing releases for examples
- Review GitHub Actions workflow logs
- Ask the team for guidance on commit message format
- Use manual release workflow if automatic release doesn't work

Happy releasing! 🎉