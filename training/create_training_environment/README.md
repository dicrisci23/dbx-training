# 🎓 Databricks Training Environment

This Databricks Asset Bundle automatically creates a complete training environment with the Medallion architecture (Bronze, Silver, Gold) and provides sample data for training purposes.

## 📋 Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) installed and configured
- Access to a Databricks Workspace
- Unity Catalog enabled
- Permissions to create catalogs, schemas, and volumes

## 🚀 Setup & Deployment

### Step 1: Clone Repository
```bash
git clone <repository-url>
cd databricks/create_training_environment
```

### Step 2: Configure Databricks Host ⚠️ **IMPORTANT**
Edit the `databricks.yml` file and change the `host` URL to your Databricks workspace:

```yaml
targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://your-databricks-instance.azuredatabricks.net/  # <- CHANGE THIS
```

**How to find your host:**
1. Go to your Databricks workspace
2. The URL in the address bar is your host (e.g. `https://adb-123456789.16.azuredatabricks.net/`)
3. Insert this URL into the `databricks.yml` file

### Step 3: Configure Catalog Name (optional)
Open `variables.yml` and adjust the catalog name:
```yaml
variables:
  catalog_name:
    default: training  # <- Change this for different environments
```

**Examples for different environments:**
- `training` (default)
- `development` 
- `staging`
- `production`

### Step 4: Validate Bundle
```bash
databricks bundle validate --var="catalog_name=training"
```

### Step 5: Deploy
```bash
databricks bundle deploy --target dev
```

### Step 6: Copy Sample Data (optional)
After deployment, you can copy the sample data:
```bash
databricks bundle run copy_sample_data_job --target dev
```

## ✅ What Gets Created Automatically

After deployment you will have the following resources in your catalog:

### **📊 Schemas (Medallion Architecture):**
- **`bronze`** - Raw ingested data
- **`silver`** - Cleaned and validated data  
- **`gold`** - Business-ready aggregated data
- **`raw`** - Raw data storage
- **`external`** - External data sources
- **`landing`** - Landing zone for incoming data

### **💾 Volumes:**
- **`sample_data`** - Located in the `raw` schema for training sample data

### **⚙️ Jobs:**
- **`copy_sample_data`** - Automatically copies sample files from the repository to the `sample_data` volume

## 🛠 Usage Examples

After deployment you can use the resources like this:

### **Working with Schemas:**
```sql
-- Use different layers of the medallion architecture
USE CATALOG training;

-- Bronze layer (raw data)
USE SCHEMA bronze;
CREATE TABLE raw_sales_data AS SELECT * FROM source_table;

-- Silver layer (cleaned data)
USE SCHEMA silver;
CREATE TABLE clean_sales_data AS 
SELECT * FROM training.bronze.raw_sales_data 
WHERE date_column IS NOT NULL;

-- Gold layer (aggregated data)
USE SCHEMA gold;
CREATE TABLE monthly_sales_summary AS
SELECT DATE_TRUNC('month', date_column) as month, SUM(amount) as total_sales
FROM training.silver.clean_sales_data
GROUP BY DATE_TRUNC('month', date_column);
```

### **Working with Sample Data:**
```sql
-- List files in sample data volume
LIST '/Volumes/training/raw/sample_data/'

-- Read sample data files
SELECT * FROM read_files('/Volumes/training/raw/sample_data/csv/*.csv', format => 'csv', header => true);

-- Load specific files
SELECT * FROM read_files('/Volumes/training/raw/sample_data/csv/WineQuality.csv', format => 'csv', header => true);
```

### **Running Jobs:**
```bash
# Deploy bundle (includes the job)
databricks bundle deploy --target dev

# Run copy job manually
databricks bundle run copy_sample_data_job --target dev

# Check job status in Databricks UI under "Workflows"
```

## 📁 Available Sample Data

The bundle contains various sample files for training purposes:

```
sample_data/
├── csv/
│   ├── cities.csv
│   ├── countries.csv  
│   ├── states.csv
│   └── WineQuality.csv
├── json/
│   ├── countries.json
│   └── states.json
├── xlsx/
│   └── FinancialsSampleData.xlsx
├── xml/
│   ├── countries.xml
│   └── states.xml
└── yml/
    ├── countries.yml
    └── states.yml
```

## 🏗 Architecture

This setup follows data engineering best practices:

```
training (catalog)
├── landing     → Incoming raw files
├── bronze      → Raw ingested data  
├── silver      → Cleaned & validated data
├── gold        → Business-ready data
├── raw         → Raw data storage
│   └── sample_data (volume) → Training sample data
└── external    → External data sources
```

## 🔧 Advanced Configuration

### Using Multiple Environments:
```bash
# Development
databricks bundle deploy --target dev --var="catalog_name=dev_training"

# Staging  
databricks bundle deploy --target staging --var="catalog_name=staging_training"

# Production
databricks bundle deploy --target prod --var="catalog_name=prod_training"
```

### Check Bundle Status:
```bash
# Show deployed resources
databricks bundle summary --target dev

# Validate bundle configuration
databricks bundle validate --target dev
```

## ⚠️ Important Notes

- **Databricks Host** in `databricks.yml` must point to your workspace
- **Only change the catalog name** for different environments
- **All schemas follow standard naming conventions** for data engineering best practices
- **The sample_data volume** is automatically placed in the raw schema
- **Unity Catalog** must be enabled in your workspace

## 🆘 Troubleshooting

### Common Problems and Solutions:

#### 1. **Host Configuration**
```bash
Error: cannot access workspace
```
**Solution:** Check the `host` URL in `databricks.yml`

#### 2. **Authentication**
```bash
Error: authentication failed
```
**Solution:** 
```bash
databricks configure
# or
databricks auth login --host https://your-databricks-instance.azuredatabricks.net/
```

#### 3. **Permissions**
```bash
Error: insufficient permissions
```
**Solution:** Make sure you have permissions to create catalogs, schemas, and volumes.

#### 4. **Catalog doesn't exist**
```bash
Error: catalog 'training' does not exist
```
**Solution:** 
- The catalog will be created automatically if you have the necessary permissions
- Alternatively: Use an existing catalog name in `variables.yml`

#### 5. **Validation failed**
```bash
databricks bundle validate --var="catalog_name=your_catalog_name"
```

## 📞 Support

If you encounter problems:
1. Check the Databricks host configuration in `databricks.yml`
2. Verify authentication: `databricks auth token`
3. Make sure Unity Catalog is enabled
4. Check permissions for catalog, schema, and volume creation
5. Contact your Databricks administrator for permission issues

## 🔗 Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Medallion Architecture](https://docs.databricks.com/lakehouse/medallion.html)