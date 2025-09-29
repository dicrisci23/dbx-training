# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks dbutils - Comprehensive Guide
# MAGIC
# MAGIC This notebook provides a comprehensive overview of Databricks `dbutils` - a set of utilities that provide access to Databricks-specific functionality.
# MAGIC
# MAGIC ## Table of Contents
# MAGIC 1. [Introduction to dbutils](#introduction)
# MAGIC 2. [File System Operations (dbutils.fs)](#filesystem)
# MAGIC 3. [Notebook Workflow Operations (dbutils.notebook)](#notebook)
# MAGIC 4. [Secrets Management (dbutils.secrets)](#secrets)
# MAGIC 5. [Widgets for Parameterization (dbutils.widgets)](#widgets)
# MAGIC 6. [Library Management (dbutils.library)](#library)
# MAGIC 7. [Job and Cluster Information (dbutils.jobs)](#jobs)
# MAGIC 8. [Data Operations (dbutils.data)](#data)
# MAGIC 9. [Credential Operations (dbutils.credentials)](#credentials)
# MAGIC 10. [Best Practices and Tips](#best-practices)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Introduction to dbutils {#introduction}
# MAGIC
# MAGIC `dbutils` is a utility module available in Databricks notebooks that provides convenient functions for:
# MAGIC - File system operations
# MAGIC - Notebook workflow management
# MAGIC - Secret management
# MAGIC - Widget creation and management
# MAGIC - Library installation
# MAGIC - And much more!
# MAGIC
# MAGIC Let's start by exploring what's available:

# COMMAND ----------

# Display all available dbutils modules
dbutils.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. File System Operations (dbutils.fs) {#filesystem}
# MAGIC
# MAGIC The file system utilities allow you to interact with various file systems including DBFS, S3, Azure Data Lake, and more.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Basic File System Commands

# COMMAND ----------

# List available file system commands
dbutils.fs.help()

# COMMAND ----------

# List contents of the root directory
display(dbutils.fs.ls("/"))

# COMMAND ----------

# List contents of DBFS root
display(dbutils.fs.ls("dbfs:/"))

# COMMAND ----------

# List FileStore (for uploaded files)

# try:
#     display(dbutils.fs.ls("/FileStore/"))
# except Exception as e:
#     print(f"FileStore directory might be empty or not accessible: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Creating and Managing Directories

# COMMAND ----------

# # Create a directory for our examples
# test_dir = "/tmp/dbutils_demo"
# dbutils.fs.mkdirs(test_dir)
# print(f"Created directory: {test_dir}")

# COMMAND ----------

# Verify the directory was created
#display(dbutils.fs.ls("/tmp/"))

# COMMAND ----------

# Use the current user information to get the username 
raw_user = spark.sql("select current_user()").first()[0]

# Take the part before @, replace "." with "_"
username = raw_user.split("@")[0].replace(".", "_")

print(username)   # -> giuseppe_dicrisci


# COMMAND ----------

# pick an existing volume (here: training.default.sample_data)
base = f"/Volumes/training/dev_{username}_raw/sample_data"
display(base)

# COMMAND ----------

# create a demo folder
test_dir = f"{base}/dbutils_demo"
dbutils.fs.mkdirs(test_dir)

# COMMAND ----------

# Verify the directory was created
display(dbutils.fs.ls(base))

# COMMAND ----------

# Check directory
test_dir = f"{base}/dbutils_demo"
files = dbutils.fs.ls(test_dir)
if files:
    display(files)
else:
    print(f"No files found in {test_dir} or directory does not exist.")

# COMMAND ----------

# delete directory again
#dbutils.fs.rm(demo_dir, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Creating and Writing Files

# COMMAND ----------

# Create a sample text file
sample_file = f"{test_dir}/sample.txt"
dbutils.fs.put(sample_file, "Hello, Databricks!\nThis is a sample file created with dbutils.fs.put()", True)
print(f"Created file: {sample_file}")

# COMMAND ----------

# Create a CSV file
csv_content = """name,age,city
Alice,25,New York
Bob,30,San Francisco
Charlie,35,Chicago"""

csv_file = f"{test_dir}/sample.csv"
dbutils.fs.put(csv_file, csv_content, True)
print(f"Created CSV file: {csv_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Reading Files

# COMMAND ----------

# Read the text file
file_content = dbutils.fs.head(sample_file)
print("File content:")
print(file_content)

# COMMAND ----------

# Read first few lines with limit
file_content_limited = dbutils.fs.head(sample_file, max_bytes=20)
print("First 20 bytes:")
print(file_content_limited)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 File Operations

# COMMAND ----------

# Copy a file
copied_file = f"{test_dir}/sample_copy.txt"
dbutils.fs.cp(sample_file, copied_file)
print(f"Copied {sample_file} to {copied_file}")

# COMMAND ----------

# Move/rename a file
moved_file = f"{test_dir}/sample_renamed.txt"
dbutils.fs.mv(copied_file, moved_file)
print(f"Moved {copied_file} to {moved_file}")

# COMMAND ----------

# List all files in our test directory
display(dbutils.fs.ls(test_dir))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 File Information and Utilities

# COMMAND ----------

# Get file information
import pprint
file_info = dbutils.fs.ls(sample_file)[0]
print("File information:")
pprint.pprint(file_info._asdict())

# COMMAND ----------

# Check if a file exists (using try-catch as there's no direct exists method)
def file_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except:
        return False

print(f"Does {sample_file} exist? {file_exists(sample_file)}")
print(f"Does {test_dir}/nonexistent.txt exist? {file_exists(test_dir + '/nonexistent.txt')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Notebook Workflow Operations (dbutils.notebook) {#notebook}
# MAGIC
# MAGIC Notebook utilities allow you to chain notebooks together and create complex workflows.

# COMMAND ----------

# List available notebook commands
dbutils.notebook.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Getting Notebook Context

# COMMAND ----------

# Get current notebook path
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(f"Current notebook path: {notebook_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Exiting Notebooks with Values

# COMMAND ----------

# Create a simple child notebook for demonstration
child_notebook_path = f"/tmp/child_notebook_demo"
child_notebook_content = '''# Databricks notebook source
%md
# Child Notebook Demo
This is a demo child notebook that returns a value'''

# COMMAND ----------

# Some processing
result = {"status": "success", "processed_records": 100, "timestamp": "2025-01-01"}

# Return the result
dbutils.notebook.exit(result)
'''

# Create the child notebook (Note: In practice, you'd create this through the UI or API)
print("Child notebook would contain logic to process data and return results")
print("Example exit command: dbutils.notebook.exit({'status': 'completed', 'count': 42})")'''

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Running Child Notebooks

# COMMAND ----------

# Example of how you would run a child notebook (commented out as we don't have a real child notebook)
# result = dbutils.notebook.run("/path/to/child/notebook", timeout_seconds=60, arguments={"param1": "value1"})
# print(f"Child notebook returned: {result}")

print("Example of running a child notebook:")
print("result = dbutils.notebook.run('/path/to/child/notebook', 60, {'param1': 'value1'})")
print("This would execute the child notebook and return its exit value")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Secrets Management (dbutils.secrets) {#secrets}
# MAGIC
# MAGIC Databricks secrets allow you to securely store and access sensitive information like API keys, passwords, and tokens.

# COMMAND ----------

# List available secrets commands
dbutils.secrets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Working with Secret Scopes

# COMMAND ----------

# List all available secret scopes
try:
    scopes = dbutils.secrets.listScopes()
    if not scopes:
        print("No secret scopes found or access denied")
    else:
        print("Available secret scopes:")
        for scope in scopes:
            print(f"- {scope.name}")
except Exception as e:
    print("No secret scopes found or access denied")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Accessing Secrets (Example)

# COMMAND ----------

# Example of how to access secrets (commented out as it requires actual secrets)
print("Examples of accessing secrets:")
print("# Get a secret value")
print("api_key = dbutils.secrets.get(scope='my-secrets', key='api-key')")
print("")
print("# List secrets in a scope")
print("secrets = dbutils.secrets.list('my-secrets')")
print("for secret in secrets:")
print("    print(secret.key)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Widgets for Parameterization (dbutils.widgets) {#widgets}
# MAGIC
# MAGIC Widgets allow you to create interactive parameters for your notebooks, making them more flexible and reusable.

# COMMAND ----------

# List available widget commands
dbutils.widgets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Creating Different Types of Widgets

# COMMAND ----------

# Create a text widget
dbutils.widgets.text("sample_text", "default_value", "Sample Text Input")

# COMMAND ----------

# Create a dropdown widget
dbutils.widgets.dropdown("sample_dropdown", "Option1", ["Option1", "Option2", "Option3"], "Sample Dropdown")

# COMMAND ----------

# Create a combobox widget (allows free text + dropdown options)
dbutils.widgets.combobox("sample_combo", "Value1", ["Value1", "Value2", "Value3"], "Sample Combobox")

# COMMAND ----------

# Create a multiselect widget
dbutils.widgets.multiselect("sample_multiselect", "A", ["A", "B", "C", "D"], "Sample Multiselect")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Getting Widget Values

# COMMAND ----------

# Get values from widgets
text_value = dbutils.widgets.get("sample_text")
dropdown_value = dbutils.widgets.get("sample_dropdown")
combo_value = dbutils.widgets.get("sample_combo")
multiselect_value = dbutils.widgets.get("sample_multiselect")

print(f"Text widget value: {text_value}")
print(f"Dropdown widget value: {dropdown_value}")
print(f"Combobox widget value: {combo_value}")
print(f"Multiselect widget value: {multiselect_value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Using Widget Values in Processing

# COMMAND ----------

# Example of using widget values in data processing
if dropdown_value == "Option1":
    processing_mode = "fast"
elif dropdown_value == "Option2":
    processing_mode = "thorough"
else:
    processing_mode = "balanced"

print(f"Processing mode selected: {processing_mode}")

# Parse multiselect values (they come as comma-separated string)
selected_options = multiselect_value.split(",") if multiselect_value else []
print(f"Selected options: {selected_options}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Managing Widgets

# COMMAND ----------

# Remove a specific widget
# dbutils.widgets.remove("sample_text")  # Commented out to keep the example

# Remove all widgets (commented out to keep examples visible)
# dbutils.widgets.removeAll()

print("Widget management commands:")
print("dbutils.widgets.remove('widget_name')  # Remove specific widget")
print("dbutils.widgets.removeAll()  # Remove all widgets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Library Management  {#library}  -> dbutils.library deprecated
# MAGIC
# MAGIC Library utilities allow you to install Python packages and JARs dynamically in your cluster.

# COMMAND ----------

# Install libraries
%pip install requests
%pip install pandas


# COMMAND ----------

# List available Python libraries
%pip list

# COMMAND ----------

# List available Python libraries - another method
import importlib.metadata as md
rows = sorted([(d.metadata['Name'], d.version) for d in md.distributions()])
spark.createDataFrame(rows, ['package','version']).display()


# COMMAND ----------

# List available Python libraries - with filter
%pip list | grep requests


# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Managing Library Installation

# COMMAND ----------

# Restart Python to make installed libraries available
# dbutils.library.restartPython()  # Commented out as it would restart the Python session

print("Restart Python to load newly installed libraries:")
print("dbutils.library.restartPython()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Job and Cluster Information (dbutils.jobs) {#jobs}
# MAGIC
# MAGIC Get information about the current job context (if running as part of a job).

# COMMAND ----------

# List available job commands
try:
    dbutils.jobs.help()
except:
    print("dbutils.jobs may not be available in all Databricks versions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Getting Job Information

# COMMAND ----------

# Get task information (if running in a job)
try:
    task_values = dbutils.jobs.taskValues.get(
        taskKey="upstream_task",
        key="result",
        debugValue="default_value")
    print(f"Task value: {task_values}")
except:
    print("Not running in a job context or task values not available")

# Set task values for downstream tasks
try:
    dbutils.jobs.taskValues.set(
        key="my_result",
        value="processed_successfully")
    print("Set task value for downstream tasks")
except:
    print("Not running in a job context")

# COMMAND ----------

# Very simple example 

# Task A: sets the value (only effective inside real jobs)
row_count = 1234
dbutils.jobs.taskValues.set("row_count", row_count)
print("Task A has set the value.")

# Task B: retrieves the value
count = dbutils.jobs.taskValues.get(
    taskKey="TaskA",        # Name of the upstream task
    key="row_count", 
    debugValue=1234         # Fallback if not running inside a job
)
print(f"Task A delivered {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Data Operations (dbutils.data) {#data}
# MAGIC
# MAGIC Data utilities for summarizing DataFrames and working with data.

# COMMAND ----------

# List available data commands (may not be available in all versions)
try:
    dbutils.data.help()
except AttributeError:
    print("dbutils.data is not available in this Databricks version")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Data Summarization Example

# COMMAND ----------

# Create a sample DataFrame to demonstrate data operations
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create sample data
data = [
    ("Alice", 25, "Engineering", 75000),
    ("Bob", 30, "Sales", 65000),
    ("Charlie", 35, "Engineering", 80000),
    ("Diana", 28, "Marketing", 60000),
    ("Eve", 32, "Sales", 70000)
]

columns = ["name", "age", "department", "salary"]
df = spark.createDataFrame(data, columns)

display(df)

# COMMAND ----------

# Example of data summarization (if dbutils.data is available)
try:
    summary = dbutils.data.summarize(df)
    print("Data summary:")
    print(summary)
except:
    print("dbutils.data.summarize not available. Using standard Spark methods:")
    print("\nDataFrame Info:")
    print(f"Row count: {df.count()}")
    print(f"Column count: {len(df.columns)}")
    df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Credential Operations (dbutils.credentials) {#credentials}
# MAGIC
# MAGIC Handle credentials for accessing external services.

# COMMAND ----------

# List available credential commands (may not be available in all versions)
try:
    dbutils.credentials.help()
except AttributeError:
    print("dbutils.credentials is not available in this Databricks version")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Best Practices and Tips {#best-practices}
# MAGIC
# MAGIC Here are some best practices when working with dbutils:

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.1 Error Handling Best Practices

# COMMAND ----------

# Always use try-catch blocks when working with file operations
def safe_file_operation():
    try:
        # Attempt to read a file
        content = dbutils.fs.head("/path/that/might/not/exist.txt")
        return content
    except Exception as e:
        print(f"File operation failed: {e}")
        return None

result = safe_file_operation()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.2 Working with Different File Systems

# COMMAND ----------

# Examples of different file system paths
print("Different file system path examples:")
print("DBFS: dbfs:/path/to/file") # --> Don't use this anymore, if you don't have an urgent case! Or you have a special purpose.
print("S3: s3a://bucket-name/path/to/file")
print("Azure Data Lake Gen2: abfss://container@account.dfs.core.windows.net/path")
print("Azure Blob Storage: wasbs://container@account.blob.core.windows.net/path")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.3 Useful Helper Functions

# COMMAND ----------

def get_file_size(path):
    """Get file size in bytes"""
    try:
        file_info = dbutils.fs.ls(path)[0]
        return file_info.size
    except:
        return None

def list_files_recursively(path, file_extension=None):
    """List all files recursively with optional extension filter"""
    all_files = []
    
    def explore_directory(current_path):
        try:
            items = dbutils.fs.ls(current_path)
            for item in items:
                if item.isDir():
                    explore_directory(item.path)
                else:
                    if file_extension is None or item.name.endswith(file_extension):
                        all_files.append(item)
        except:
            pass
    
    explore_directory(path)
    return all_files

# Example usage
print("Helper functions defined:")
print("- get_file_size(path): Returns file size in bytes")
print("- list_files_recursively(path, extension): Lists all files recursively")

# COMMAND ----------

# get file size of sample.txt file
# Giusi ToDo: Convert/Calculate Size in MB
get_file_size(csv_file)

# COMMAND ----------

# get through sample data directory
list_files_recursively(base, ".txt")

# COMMAND ----------

# Giusi ToDo: 
# get the total size of a directory

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.4 Performance Tips

# COMMAND ----------

print("Performance Tips:")
print("1. Use display() for DataFrames instead of show() for better performance")
print("2. Cache frequently accessed files in DBFS for faster access")
print("3. Use appropriate file formats (Parquet, Delta) for large datasets")
print("4. Leverage widgets for parameterization instead of hardcoded values")
print("5. Use dbutils.fs.ls() with patterns to filter files efficiently")
print("6. Handle secrets properly - never log or print secret values")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Let's clean up the test files we created during this demonstration:

# COMMAND ----------

# Remove test directory and all its contents
try:
    dbutils.fs.rm(test_dir, recurse=True)
    print(f"Cleaned up test directory: {test_dir}")
except:
    print("Test directory cleanup completed or directory didn't exist")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook covered all major aspects of `dbutils`:
# MAGIC
# MAGIC 1. **File System Operations**: Creating, reading, writing, and managing files across different storage systems
# MAGIC 2. **Notebook Workflows**: Running child notebooks and managing notebook execution flow
# MAGIC 3. **Secrets Management**: Securely accessing sensitive information
# MAGIC 4. **Widgets**: Creating interactive parameters for notebook flexibility
# MAGIC 5. **Library Management**: Installing and managing Python packages and JARs
# MAGIC 6. **Job Context**: Working with job-related information and task values
# MAGIC 7. **Data Operations**: Summarizing and working with DataFrames
# MAGIC 8. **Credentials**: Managing external service credentials
# MAGIC 9. **Best Practices**: Error handling, performance tips, and helper functions
# MAGIC
# MAGIC ### Key Takeaways:
# MAGIC - `dbutils` is your Swiss Army knife for Databricks operations
# MAGIC - Always handle errors gracefully when working with file systems
# MAGIC - Use widgets to make notebooks parameterizable and reusable
# MAGIC - Leverage secrets for secure credential management
# MAGIC - Combine `dbutils` functions with Spark operations for powerful data processing workflows
# MAGIC
# MAGIC ### Next Steps:
# MAGIC - Practice using these utilities in your own data processing workflows
# MAGIC - Experiment with different file systems and storage options
# MAGIC - Create parameterized notebooks using widgets
# MAGIC - Build notebook workflows using `dbutils.notebook.run()`
