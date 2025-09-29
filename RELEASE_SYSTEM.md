# Automated Release System Documentation 🚀

This document explains the complete automated release system for the Databricks project.

## Overview

The automated release system provides:
- **Automatic releases** when code is merged to main branch
- **Semantic versioning** based on commit messages
- **Automatic release notes** generation
- **Manual release triggers** for custom releases
- **Databricks bundle validation** before releases
- **Multiple release workflows** for different scenarios

## Release Workflows

### 1. Automatic Release (`release.yml`)

**Trigger:** Push/merge to main branch
**Purpose:** Create releases automatically based on commit analysis

**Features:**
- Analyzes commit messages for version bump determination
- Generates comprehensive release notes automatically
- Validates Databricks bundles before release
- Creates GitHub releases with source code assets
- Skips releases for documentation-only changes

**Version Bump Rules:**
- **Major (1.0.0 → 2.0.0):** Breaking changes, commits with `BREAKING CHANGE`, `!:`
- **Minor (1.0.0 → 1.1.0):** New features (`feat:`, `feature:`)
- **Patch (1.0.0 → 1.0.1):** Bug fixes (`fix:`, `chore:`, etc.)

### 2. Enhanced Release (`enhanced-release.yml`)

**Trigger:** Push to main OR manual trigger
**Purpose:** Advanced release creation with conventional commit analysis

**Features:**
- Full conventional commits support
- Advanced changelog generation with categorization
- Prerelease and draft release support
- Custom version tags
- Detailed contributor analysis
- Statistics and technical details

**Manual Trigger Options:**
- Version type selection (major, minor, patch, prerelease)
- Custom tag names
- Draft/prerelease flags
- Custom release content

### 3. Manual Release (`manual-release.yml`)

**Trigger:** Manual trigger only
**Purpose:** Create releases with full manual control

**Features:**
- Complete manual control over release details
- Custom version specification
- Custom release titles and notes
- Optional bundle validation skip
- Immediate release creation

**Use Cases:**
- Emergency releases
- Hotfixes
- Custom version schemes
- Special milestone releases

## File Structure

```
.github/
├── workflows/
│   ├── release.yml              # Basic automatic release
│   ├── enhanced-release.yml     # Advanced release with conventional commits
│   ├── manual-release.yml       # Manual release control
│   └── pr-databricks-bundles-validate.yml  # Existing PR validation
├── CODEOWNERS                   # Repository code owners
└── README.md                    # GitHub configuration documentation

.release-config.yml              # Release configuration template
CONVENTIONAL_COMMITS.md          # Commit message guidelines
```

## How It Works

### Automatic Release Process

1. **Code Push/Merge to Main**
   - Developer merges PR to main branch (after bundle validation in PR)
   - GitHub triggers the release workflow
   
2. **Commit Analysis**
   - System analyzes all commits since last release
   - Categorizes commits by type (feat, fix, breaking, etc.)
   - Determines appropriate version bump
   
3. **Version Calculation**
   - Gets current latest tag (or starts with v1.0.0)
   - Applies semantic versioning rules
   - Creates new version tag
   
4. **Release Notes Generation**
   - Categorizes changes by type
   - Includes commit details, contributors, statistics
   - Adds Databricks-specific information (bundle count, etc.)
   - Creates comprehensive changelog
   
5. **GitHub Release Creation**
   - Creates new GitHub release with generated tag
   - Uploads source code assets (.tar.gz, .zip)
   - Publishes release notes
   - Sends notifications

**Note:** Bundle validation happens during PR review, not at release time for better performance and reliability.

### Manual Release Process

1. **Workflow Trigger**
   - Navigate to Actions → Manual Release
   - Click "Run workflow"
   
2. **Configuration**
   - Select version type or provide custom version
   - Add custom title and notes if needed
   - Choose draft/prerelease options
   
3. **Validation & Creation**
   - Validates inputs and bundle configurations
   - Creates version tag and GitHub release
   - Provides immediate feedback

## Configuration

### Environment Variables & Secrets

**Required Secrets:**
- `GITHUB_TOKEN` - Automatically provided by GitHub Actions
- `SP_TOKEN` - Databricks service principal token (for bundle validation)

**Optional Secrets:**
- `SLACK_WEBHOOK_URL` - For Slack notifications
- `DISCORD_WEBHOOK_URL` - For Discord notifications

### Customization Options

**Release Configuration (`.release-config.yml`):**
```yaml
automation:
  auto_release: true
  release_branches: [main, master]

versioning:
  scheme: semantic
  initial_version: "1.0.0"
  tag_prefix: "v"

release_notes:
  include_contributors: true
  include_prs: true
  max_changes_per_category: 50
```

**Bundle Validation:**
- Validates all `databricks.yml` files found in repository
- Uses `dev` target by default
- Can be disabled for specific releases

## Commit Message Guidelines

Follow [Conventional Commits](./CONVENTIONAL_COMMITS.md) specification:

### Version Bump Triggers
```bash
# Major version (breaking changes)
feat!: remove legacy API endpoints
BREAKING CHANGE: API v1 removed

# Minor version (new features)
feat: add user authentication
feat(lineage): implement column tracking

# Patch version (fixes & improvements)
fix: resolve bundle validation error
chore: update dependencies
docs: improve API documentation
```

### Best Practices
- Use clear, descriptive commit messages
- Include scope when relevant: `feat(bundles):`
- Add body text for complex changes
- Reference issues: `Closes #123`
- Use imperative mood: "add" not "added"

## Release Notes Features

### Automatic Categorization
- **⚠️ BREAKING CHANGES** - Breaking changes and major updates
- **✨ New Features** - New functionality and enhancements
- **🐛 Bug Fixes** - Bug fixes and corrections
- **📚 Documentation** - Documentation updates
- **🔧 Improvements** - Refactoring and performance improvements
- **📦 Dependencies** - Dependency updates

### Additional Information
- **Contributors** - List of all contributors to the release
- **Statistics** - Commit count, file changes, contributor count
- **Databricks Info** - Bundle count, validation status
- **Technical Details** - SHA, workflow links, branch information

## Troubleshooting

### Common Issues

**No Release Created:**
- Check commit message format follows conventional commits
- Ensure push is to main/master branch
- Verify GitHub Actions have proper permissions
- Review workflow logs for errors

**Wrong Version Bump:**
- Check commit types in messages
- Use `!` for breaking changes: `feat!:` not `feat:`
- Add `BREAKING CHANGE:` in commit body for major bumps

**Bundle Validation Failures:**
- Ensure all `databricks.yml` files are valid
- Check Databricks CLI configuration
- Verify service principal token permissions
- Use manual release with validation skip if needed

**Missing Release Notes Content:**
- Ensure commits follow conventional format
- Check that commit messages are descriptive
- Review categorization rules in workflow

### Debugging Steps

1. **Check Recent Commits:**
   ```bash
   git log --oneline -10
   ```

2. **Validate Bundle Locally:**
   ```bash
   cd path/to/bundle
   databricks bundle validate --target dev
   ```

3. **Test Commit Message Format:**
   ```bash
   # Good examples:
   git commit -m "feat: add new feature"
   git commit -m "fix: resolve bug in component"
   git commit -m "feat!: breaking change implementation"
   ```

4. **Check Workflow Logs:**
   - Navigate to Actions tab in GitHub
   - Review failed workflow runs
   - Check individual step outputs

## Usage Examples

### Scenario 1: Regular Feature Release
```bash
# Developer commits
git commit -m "feat(bundles): add multi-environment support"
git commit -m "docs: update deployment guide"
git commit -m "fix(lineage): resolve schema detection issue"

# Merge PR to main → Automatic minor release (v1.2.0 → v1.3.0)
```

### Scenario 2: Emergency Hotfix
```bash
# Use manual release workflow
# Select "patch" version type
# Add custom notes about the emergency fix
# Deploy immediately
```

### Scenario 3: Major Version Release
```bash
# Developer commits with breaking changes
git commit -m "feat!: migrate to Unity Catalog

BREAKING CHANGE: All existing tables must be migrated to Unity Catalog.
See MIGRATION.md for detailed instructions."

# Merge PR to main → Automatic major release (v1.3.0 → v2.0.0)
```

## Best Practices

### For Developers
- Write clear, descriptive commit messages
- Follow conventional commits specification
- Test changes locally before creating PRs
- Use appropriate commit types for version bumping
- Include scope information when relevant

### for Release Managers
- Monitor automated releases for accuracy
- Use manual releases for special circumstances
- Keep release configuration up to date
- Regularly review and improve release notes quality
- Coordinate major releases with team communication

### For Repository Maintenance
- Regularly update GitHub Actions versions
- Monitor workflow performance and success rates
- Keep Databricks CLI and dependencies updated
- Review and improve release note templates
- Maintain documentation accuracy

## Migration from Manual Releases

If you're migrating from manual releases:

1. **Enable Automatic Releases:**
   - Merge this configuration to main branch
   - Train team on conventional commit messages
   
2. **Test with Feature Branch:**
   - Create test releases on feature branches
   - Validate workflow behavior
   
3. **Gradual Adoption:**
   - Start with manual release workflow
   - Gradually move to automatic releases
   - Monitor and adjust as needed

## Support & Contact

For questions about the release system:
- Check workflow logs in GitHub Actions
- Review this documentation and conventional commits guide
- Contact the development team
- Create issues for bugs or feature requests

---

*This release system is designed to automate and streamline the release process while maintaining flexibility for special cases. Regular maintenance and updates ensure continued reliability and effectiveness.*