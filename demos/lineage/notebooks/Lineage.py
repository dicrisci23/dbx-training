# Databricks notebook source
# MAGIC %md
# MAGIC # Lineage

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook has some transformation whcih can be used for exploration of the system tables. It's not intended to be used in Production. It should be replaced in the future with a Databricks App.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation

# COMMAND ----------

# MAGIC %pip install networkx kaleido

# COMMAND ----------

pip install -U kaleido

# COMMAND ----------

from pyspark.sql.functions import col
from functools import reduce

# COMMAND ----------

# MAGIC %md
# MAGIC ## get System Tables Info

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED system.access.table_lineage

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM system.access.table_lineage

# COMMAND ----------

target_table_full_name = 'pilatus_hub_dev.gold_saphana.dim_material'
spark.sql(f"""
WITH latest_event AS (
  SELECT MAX(event_date) AS max_event_date
  FROM system.access.table_lineage
  WHERE target_table_full_name = '{target_table_full_name}'
)
SELECT *
FROM system.access.table_lineage
WHERE 1 = 1
    AND target_table_full_name = '{target_table_full_name}'
    AND event_date = (SELECT max_event_date FROM latest_event)
    AND entity_type = 'PIPELINE';
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## create Dataframe

# COMMAND ----------


def get_lineage_info(target_table_full_name):
    query = f"""
    WITH latest_event AS (
      SELECT MAX(event_date) AS max_event_date
      FROM system.access.table_lineage
      WHERE target_table_full_name = '{target_table_full_name}'
    )
    SELECT *
    FROM system.access.table_lineage
    WHERE 1 = 1
        AND target_table_full_name = '{target_table_full_name}'
        AND event_date = (SELECT max_event_date FROM latest_event)
        AND entity_type = 'PIPELINE';
    """
    df = spark.sql(query)
    return df

# COMMAND ----------

target_table_full_name = 'pilatus_hub_dev.gold_saphana.dim_material'

df = get_lineage_info(target_table_full_name)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, max as max_
from functools import reduce

def get_recursive_lineage(
    start_table_full_name: str, 
    max_depth: int = 10,
    date_filter: str = None
):
    # Load lineage data from Unity Catalog system table
    lineage_df = spark.read.table("system.access.table_lineage")

    # If no date_filter is provided, get the latest event_date from the table
    if date_filter is None:
        latest_date = lineage_df.select(max_("event_date")).first()[0]
        if latest_date is None:
            raise ValueError("No event_date found in system.access.table_lineage.")
        date_filter = latest_date

    # Filter lineage data by the provided or inferred date
    lineage_df = lineage_df.filter(col("event_date") == date_filter)

    # Exclude self-references where source and target are the same table
    lineage_df = lineage_df.filter(
        col("source_table_full_name") != col("target_table_full_name")
    )

    visited = set()
    result = []

    def recurse(table_name: str, depth: int):
        # Stop recursion if maximum depth is reached or table already visited
        if depth > max_depth or table_name in visited:
            return
        visited.add(table_name)

        # Get direct lineage edges for the given table
        direct_lineage = (
            lineage_df
            .filter(col("target_table_full_name") == table_name)
            .filter(col("source_table_full_name").isNotNull())
            .filter(~col("source_table_full_name").like("%__materialization_mat_%"))  # Exclude internal materialization tables
            .filter(col("source_table_full_name") != col("target_table_full_name"))  # Redundant but ensures safety
            .select("source_table_full_name", "target_table_full_name", "event_time")
            .distinct()
        )

        result.append(direct_lineage)

        # Collect source tables and recurse for each of them
        source_tables = [
            row["source_table_full_name"]
            for row in direct_lineage.collect()
        ]

        for src in source_tables:
            recurse(src, depth + 1)

    # Start recursive traversal from the given table
    recurse(start_table_full_name, 0)

    # Combine results into one DataFrame or return empty if no lineage found
    if result:
        combined = reduce(lambda df1, df2: df1.unionByName(df2), result)
        return combined
    else:
        return spark.createDataFrame([], schema="source_table_full_name string, target_table_full_name string, event_time timestamp")

# COMMAND ----------

df = get_recursive_lineage(
    start_table_full_name="pilatus_hub_dev.gold_saphana.dim_material",
    max_depth=10
)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plotly Sankey

# COMMAND ----------

import pandas as pd
import plotly.graph_objects as go

def plot_lineage_sankey(table_name: str):
    """
    Create a lineage Sankey diagram for the given table.
    
    Parameters:
    - df: Spark DataFrame containing lineage information (must have 'source_table_full_name', 'target_table_full_name')
    - main_table_name: The name of the main table (used in the title)
    """

    # get lineage info from System tables
    df = get_recursive_lineage(
    start_table_full_name=table_name,
    max_depth=10
)

    # Convert Spark DataFrame to Pandas
    pdf = df.toPandas()

    # Filter and clean data
    pdf = pdf[['source_table_full_name', 'target_table_full_name']].dropna()

    # Aggregate counts of source → target relationships
    agg_df = pdf.groupby(['source_table_full_name', 'target_table_full_name']).size().reset_index(name='count')

    # Create a unique list of all table names (source and target)
    full_labels = list(pd.unique(agg_df[['source_table_full_name', 'target_table_full_name']].values.ravel()))
    label_index = {label: i for i, label in enumerate(full_labels)}

    # Short labels (only table name without catalog.schema)
    short_labels = [label.split('.')[-1] for label in full_labels]

    # Define node colors based on naming
    def get_layer_color(table_full_name):
        if 'bronze' in table_full_name.lower():
            return '#cd7f32'  # bronze
        elif 'silver' in table_full_name.lower():
            return '#c0c0c0'  # silver
        elif 'gold' in table_full_name.lower():
            return '#ffd700'  # gold
        else:
            return '#a9a9a9'  # default gray

    node_colors = [get_layer_color(label) for label in full_labels]

    # Map source and target labels to index
    sources = agg_df['source_table_full_name'].map(label_index)
    targets = agg_df['target_table_full_name'].map(label_index)
    values = agg_df['count']

    # Create Plotly Sankey diagram
    title = f"Lineage Visualization: {table_name}"

    fig = go.Figure(go.Sankey(
        node=dict(
            pad=20,
            thickness=20,
            label=short_labels,
            customdata=full_labels,
            hovertemplate='Full Name: %{customdata}<extra></extra>',
            color=node_colors,
            line=dict(color="black", width=0.3)
        ),
        link=dict(source=sources, target=targets, value=values)
    ))

    # Layout with spacing and layer headings
    fig.update_layout(
        title=dict(
            text=f"{title}",
            x=0.5,
            xanchor='center',
            font=dict(size=18)
        ),
        font_size=12,
        height=600,
        margin=dict(l=20, r=20, t=140, b=20),

        annotations=[
            dict(x=0.0, y=1.16, text="🥉 Bronze", showarrow=False, font=dict(size=14), xref='paper', yref='paper'),
            dict(x=0.5,  y=1.16, text="🥈 Silver", showarrow=False, font=dict(size=14), xref='paper', yref='paper'),
            dict(x=0.999, y=1.16, text="🥇 Gold",   showarrow=False, font=dict(size=14), xref='paper', yref='paper'),
        ]
    )

    fig.show()


plot_lineage_sankey('pilatus_hub_dev.gold_saphana.dim_material')

# COMMAND ----------



# COMMAND ----------

import pandas as pd
import plotly.graph_objects as go

def plot_lineage_sankey(table_name: str, export_html: bool = True, html_path: str = None):
    """
    Create a lineage Sankey diagram for the given table and optionally export to HTML.
    
    Parameters:
    - table_name: Full name of the main table (e.g. 'catalog.schema.table')
    - export_html: If True, saves the figure to an HTML file
    - html_path: Optional custom path for the HTML file
    """

    # Get lineage info from system table
    df = get_recursive_lineage(
        start_table_full_name=table_name,
        max_depth=10
    )

    # Convert Spark DataFrame to Pandas
    pdf = df.toPandas()

    # Clean and filter relevant columns
    pdf = pdf[['source_table_full_name', 'target_table_full_name']].dropna()

    # Aggregate relationship counts
    agg_df = pdf.groupby(['source_table_full_name', 'target_table_full_name']).size().reset_index(name='count')

    # Get all table names
    full_labels = list(pd.unique(agg_df[['source_table_full_name', 'target_table_full_name']].values.ravel()))
    label_index = {label: i for i, label in enumerate(full_labels)}
    short_labels = [label.split('.')[-1] for label in full_labels]

    # Layer-based coloring
    def get_layer_color(table_full_name):
        if 'bronze' in table_full_name.lower():
            return '#cd7f32'
        elif 'silver' in table_full_name.lower():
            return '#c0c0c0'
        elif 'gold' in table_full_name.lower():
            return '#ffd700'
        else:
            return '#a9a9a9'

    node_colors = [get_layer_color(label) for label in full_labels]

    # Map relationships to Sankey indices
    sources = agg_df['source_table_full_name'].map(label_index)
    targets = agg_df['target_table_full_name'].map(label_index)
    values = agg_df['count']

    # Create Sankey figure
    title = f"Lineage Visualization: {table_name}"
    fig = go.Figure(go.Sankey(
        node=dict(
            pad=20,
            thickness=20,
            label=short_labels,
            customdata=full_labels,
            hovertemplate='Full Name: %{customdata}<extra></extra>',
            color=node_colors,
            line=dict(color="black", width=0.3)
        ),
        link=dict(source=sources, target=targets, value=values)
    ))

    # Layout with annotations
    fig.update_layout(
        title=dict(
            text=title,
            x=0.5,
            xanchor='center',
            font=dict(size=18)
        ),
        font_size=12,
        height=600,
        margin=dict(l=20, r=20, t=140, b=20),
        annotations=[
            dict(x=0.0, y=1.16, text="🥉 Bronze", showarrow=False, font=dict(size=14), xref='paper', yref='paper'),
            dict(x=0.5, y=1.16, text="🥈 Silver", showarrow=False, font=dict(size=14), xref='paper', yref='paper'),
            dict(x=0.999, y=1.16, text="🥇 Gold", showarrow=False, font=dict(size=14), xref='paper', yref='paper'),
        ]
    )

    # Export as HTML if requested
    if export_html:
        if html_path is None:
            safe_name = table_name.replace('.', '_')
            html_path = f"lineage_{safe_name}.html"
        fig.write_html(html_path, auto_open=False)
        print(f"✅ HTML file saved at: {html_path}")

    # Optional: show plot inline (still works in Notebook)
    fig.show()

table_name = 'pilatus_hub_dev.gold_saphana.dim_material'
path = '/Volumes/sandbox/stefan/files/plotly'
plot_lineage_sankey(table_name, html_path=f"{path}/lineage_view_{table_name}.html")

# COMMAND ----------

# MAGIC %md
# MAGIC # create Table

# COMMAND ----------

def get_lineage_paths(table_name, max_depth=5):
    df = get_recursive_lineage(table_name, max_depth=max_depth)
    df = df.dropna().filter("source_table_full_name != target_table_full_name")

    paths = [(table_name, [table_name])]

    results = []

    for _ in range(max_depth):
        new_paths = []
        for current_node, path in paths:
            parents = (
                df.filter(df["target_table_full_name"] == current_node)
                .select("source_table_full_name")
                .distinct()
                .rdd.flatMap(lambda x: x)
                .collect()
            )
            if not parents:
                results.append(list(reversed(path)))
            else:
                for p in parents:
                    if p not in path:  # avoid loops
                        new_paths.append((p, path + [p]))
        paths = new_paths

    # Wenn Pfade zu kurz: auffüllen mit None
    padded = []
    for path in results:
        while len(path) < max_depth + 1:
            path.append(None)
        padded.append(path)

    # Konvertiere zu Pandas DataFrame mit Spalten stage_1 .. stage_N
    pdf = pd.DataFrame(padded, columns=[f"stage_{i+1}" for i in range(max_depth+1)])
    pdf["value"] = 1  # optional: du könntest hier auch Frequenz zählen
    return spark.createDataFrame(pdf)

# COMMAND ----------

get_lineage_paths("pilatus_hub_dev.gold_saphana.dim_material").createOrReplaceTempView("lineage_paths")

# COMMAND ----------

from pyspark.sql.functions import col, lit
import pandas as pd

def get_lineage_paths(table_name: str, max_depth: int = 5):
    df = get_recursive_lineage(start_table_full_name=table_name, max_depth=max_depth)

    # Filter out invalid/self references
    df = (
        df.filter(col("source_table_full_name").isNotNull())
          .filter(col("source_table_full_name") != col("target_table_full_name"))
          .select("source_table_full_name", "target_table_full_name")
    )

    # Build parent → child map
    edges = df.distinct()

    # Initialize paths DataFrame with root node
    paths = spark.createDataFrame([(table_name,)], ["stage_{}".format(max_depth)])

    for depth in reversed(range(max_depth)):
        paths = (
            paths.join(edges, paths[f"stage_{depth+1}"] == edges["target_table_full_name"], how="left")
                 .drop("target_table_full_name")
                 .withColumnRenamed("source_table_full_name", f"stage_{depth}")
        )

    # Fill missing stages with null
    for d in range(max_depth + 1):
        if f"stage_{d}" not in paths.columns:
            paths = paths.withColumn(f"stage_{d}", lit(None))

    # Add value column
    paths = paths.withColumn("value", lit(1))
    return paths

df_paths = get_lineage_paths("pilatus_hub_dev.gold_saphana.dim_material", max_depth=4)
display(df_paths)

# COMMAND ----------

from pyspark.sql.functions import col, lit, max as max_, split
from pyspark.sql import DataFrame
from functools import reduce

def get_all_lineage_paths_latest_event_filtered(catalog_filter: str, max_depth: int = 5) -> DataFrame:
    """
    Builds recursive lineage paths for all distinct target tables (latest event only),
    filtered by a specific catalog, and includes catalog/schema columns.
    Serverless-compatible (no .rdd, no collect, no mapInPandas).
    """
    lineage_base = (
        spark.read.table("system.access.table_lineage")
        .filter(col("target_table_full_name").isNotNull())
        .filter(col("source_table_full_name").isNotNull())
        .filter(col("source_table_full_name") != col("target_table_full_name"))
        .filter(col("target_table_full_name").startswith(catalog_filter))
        .select("source_table_full_name", "target_table_full_name", "event_date")
        .distinct()
    )

    latest_events = (
        lineage_base.groupBy("target_table_full_name")
        .agg(max_("event_date").alias("event_date"))
    )

    # Only keep latest event rows per table
    lineage_latest = (
        lineage_base.alias("l")
        .join(latest_events.alias("e"),
              (col("l.target_table_full_name") == col("e.target_table_full_name")) &
              (col("l.event_date") == col("e.event_date")),
              how="inner")
        .select("l.source_table_full_name", "l.target_table_full_name")
        .distinct()
    )

    # Create a DataFrame with unique target tables (without collect)
    unique_targets_df = (
        lineage_latest.select("target_table_full_name")
        .distinct()
        .withColumnRenamed("target_table_full_name", "table_name")
    )

    # Prepare to collect all paths here
    all_paths = []

    # Loop over tables manually (use toLocalIterator for serverless-safe iteration)
    for row in unique_targets_df.toLocalIterator():
        table_name = row["table_name"]
        print(f"🔄 Processing: {table_name}")

        # Subset of lineage only relevant for recursion
        edges = lineage_latest

        # Start with last stage = current table
        paths = spark.createDataFrame([(table_name,)], [f"stage_{max_depth}"])

        for depth in reversed(range(max_depth)):
            paths = (
                paths.join(edges,
                           paths[f"stage_{depth+1}"] == edges["target_table_full_name"],
                           how="left")
                     .drop("target_table_full_name")
                     .withColumnRenamed("source_table_full_name", f"stage_{depth}")
            )

        # Fill missing columns
        for d in range(max_depth + 1):
            if f"stage_{d}" not in paths.columns:
                paths = paths.withColumn(f"stage_{d}", lit(None))

        # Add metadata columns
        paths = (
            paths.withColumn("value", lit(1))
                 .withColumn("target_table", lit(table_name))
                 .withColumn("target_catalog", split(lit(table_name), "\\.").getItem(0))
                 .withColumn("target_schema", split(lit(table_name), "\\.").getItem(1))
        )

        all_paths.append(paths)

    # Combine all paths
    if all_paths:
        full_df = reduce(DataFrame.unionByName, all_paths)
        full_df.createOrReplaceTempView("all_lineage_paths")
        return full_df
    else:
        print("❌ No lineage paths generated.")
        return None

# COMMAND ----------

df_lineage = get_all_lineage_paths_latest_event_filtered("pilatus_hub_dev", max_depth=4)

# COMMAND ----------

display(df_lineage)

# COMMAND ----------

df_paths.write.mode("overwrite").saveAsTable("sandbox.stefan.lineage_paths1")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sandbox.stefan.lineage_paths1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabelle mit dem jeweils neuesten event_date pro Ziel-Tabelle
# MAGIC WITH latest_events AS (
# MAGIC   SELECT
# MAGIC     target_table_full_name,
# MAGIC     MAX(event_date) AS max_event_date
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE target_table_full_name LIKE 'pilatus_hub_dev.%'
# MAGIC   GROUP BY target_table_full_name
# MAGIC ),
# MAGIC
# MAGIC -- Nur neueste Einträge pro Tabelle
# MAGIC latest_lineage AS (
# MAGIC   SELECT DISTINCT l.source_table_full_name, l.target_table_full_name
# MAGIC   FROM system.access.table_lineage l
# MAGIC   JOIN latest_events e
# MAGIC     ON l.target_table_full_name = e.target_table_full_name AND l.event_date = e.max_event_date
# MAGIC   WHERE l.source_table_full_name IS NOT NULL
# MAGIC     AND l.source_table_full_name != l.target_table_full_name
# MAGIC ),
# MAGIC
# MAGIC -- Rekursion "von Hand" (4 Ebenen)
# MAGIC stage_4 AS (
# MAGIC   SELECT DISTINCT target_table_full_name AS stage_4 FROM latest_lineage
# MAGIC ),
# MAGIC
# MAGIC stage_3 AS (
# MAGIC   SELECT l.source_table_full_name AS stage_3, s4.stage_4
# MAGIC   FROM latest_lineage l
# MAGIC   JOIN stage_4 s4 ON l.target_table_full_name = s4.stage_4
# MAGIC ),
# MAGIC
# MAGIC stage_2 AS (
# MAGIC   SELECT l.source_table_full_name AS stage_2, s3.stage_3, s3.stage_4
# MAGIC   FROM latest_lineage l
# MAGIC   JOIN stage_3 s3 ON l.target_table_full_name = s3.stage_3
# MAGIC ),
# MAGIC
# MAGIC stage_1 AS (
# MAGIC   SELECT l.source_table_full_name AS stage_1, s2.stage_2, s2.stage_3, s2.stage_4
# MAGIC   FROM latest_lineage l
# MAGIC   JOIN stage_2 s2 ON l.target_table_full_name = s2.stage_2
# MAGIC ),
# MAGIC
# MAGIC stage_0 AS (
# MAGIC   SELECT l.source_table_full_name AS stage_0, s1.*
# MAGIC   FROM latest_lineage l
# MAGIC   JOIN stage_1 s1 ON l.target_table_full_name = s1.stage_1
# MAGIC )
# MAGIC
# MAGIC -- Finales Ergebnis mit Metadaten-Spalten
# MAGIC SELECT
# MAGIC   stage_0, stage_1, stage_2, stage_3, stage_4,
# MAGIC   1 AS value,
# MAGIC   stage_4 AS target_table,
# MAGIC   split(stage_4, '\\.')[0] AS target_catalog,
# MAGIC   split(stage_4, '\\.')[1] AS target_schema
# MAGIC FROM stage_0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM (
# MAGIC   SELECT DISTINCT l.source_table_full_name, l.target_table_full_name
# MAGIC   FROM system.access.table_lineage l
# MAGIC   JOIN (
# MAGIC     SELECT target_table_full_name, MAX(event_date) AS max_event_date
# MAGIC     FROM system.access.table_lineage
# MAGIC     WHERE target_table_full_name LIKE 'pilatus_hub_dev.%'
# MAGIC     GROUP BY target_table_full_name
# MAGIC   ) e
# MAGIC   ON l.target_table_full_name = e.target_table_full_name
# MAGIC   AND l.event_date = e.max_event_date
# MAGIC   WHERE l.source_table_full_name IS NOT NULL
# MAGIC     AND l.source_table_full_name != l.target_table_full_name
# MAGIC ) latest_lineage;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_events AS (
# MAGIC   SELECT
# MAGIC     target_table_full_name,
# MAGIC     MAX(event_date) AS max_event_date
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE target_table_full_name LIKE 'pilatus_hub_dev.%'
# MAGIC   GROUP BY target_table_full_name
# MAGIC ),
# MAGIC latest_lineage AS (
# MAGIC   SELECT DISTINCT l.source_table_full_name, l.target_table_full_name
# MAGIC   FROM system.access.table_lineage l
# MAGIC   JOIN latest_events e
# MAGIC     ON l.target_table_full_name = e.target_table_full_name AND l.event_date = e.max_event_date
# MAGIC   WHERE l.source_table_full_name IS NOT NULL
# MAGIC     AND l.source_table_full_name != l.target_table_full_name
# MAGIC ),
# MAGIC stage_4 AS (
# MAGIC   SELECT DISTINCT target_table_full_name AS stage_4 FROM latest_lineage
# MAGIC ),
# MAGIC stage_3 AS (
# MAGIC   SELECT l.source_table_full_name AS stage_3, s4.stage_4
# MAGIC   FROM latest_lineage l
# MAGIC   JOIN stage_4 s4 ON l.target_table_full_name = s4.stage_4
# MAGIC )
# MAGIC
# MAGIC SELECT * FROM stage_3

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_events AS (
# MAGIC   SELECT
# MAGIC     target_table_full_name,
# MAGIC     MAX(event_date) AS max_event_date
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE target_table_full_name LIKE 'pilatus_hub_dev.%'
# MAGIC   GROUP BY target_table_full_name
# MAGIC ),
# MAGIC
# MAGIC latest_lineage AS (
# MAGIC   SELECT DISTINCT
# MAGIC     l.source_table_full_name,
# MAGIC     l.target_table_full_name
# MAGIC   FROM system.access.table_lineage l
# MAGIC   JOIN latest_events e
# MAGIC     ON l.target_table_full_name = e.target_table_full_name
# MAGIC    AND l.event_date = e.max_event_date
# MAGIC   WHERE l.source_table_full_name IS NOT NULL
# MAGIC     AND l.source_table_full_name != l.target_table_full_name
# MAGIC ),
# MAGIC
# MAGIC stage_4 AS (
# MAGIC   SELECT DISTINCT target_table_full_name AS stage_4 FROM latest_lineage
# MAGIC ),
# MAGIC
# MAGIC stage_3 AS (
# MAGIC   SELECT l.source_table_full_name AS stage_3, s4.stage_4
# MAGIC   FROM latest_lineage l
# MAGIC   JOIN stage_4 s4 ON l.target_table_full_name = s4.stage_4
# MAGIC ),
# MAGIC
# MAGIC stage_2 AS (
# MAGIC   SELECT l.source_table_full_name AS stage_2, s3.stage_3, s3.stage_4
# MAGIC   FROM latest_lineage l
# MAGIC   JOIN stage_3 s3 ON l.target_table_full_name = s3.stage_3
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   stage_2,
# MAGIC   stage_3,
# MAGIC   stage_4,
# MAGIC
# MAGIC   -- Ziel-Infos aus stage_4
# MAGIC   split(stage_4, '\\.')[0] AS target_catalog,
# MAGIC   split(stage_4, '\\.')[1] AS target_schema,
# MAGIC   split(stage_4, '\\.')[2] AS target_table
# MAGIC FROM stage_2

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_events AS (
# MAGIC   SELECT
# MAGIC     target_table_full_name,
# MAGIC     MAX(event_date) AS max_event_date
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE target_table_full_name LIKE 'pilatus_hub_dev.%'
# MAGIC   GROUP BY target_table_full_name
# MAGIC ),
# MAGIC
# MAGIC latest_lineage AS (
# MAGIC   SELECT DISTINCT
# MAGIC     l.source_table_full_name,
# MAGIC     l.target_table_full_name
# MAGIC   FROM system.access.table_lineage l
# MAGIC   JOIN latest_events e
# MAGIC     ON l.target_table_full_name = e.target_table_full_name
# MAGIC    AND l.event_date = e.max_event_date
# MAGIC   WHERE l.source_table_full_name IS NOT NULL
# MAGIC     AND l.source_table_full_name != l.target_table_full_name
# MAGIC ),
# MAGIC
# MAGIC stage_0 AS (
# MAGIC   SELECT DISTINCT target_table_full_name AS stage_0 FROM latest_lineage
# MAGIC ),
# MAGIC
# MAGIC stage_1 AS (
# MAGIC   SELECT l.source_table_full_name AS stage_1, s0.stage_0
# MAGIC   FROM latest_lineage l
# MAGIC   JOIN stage_0 s0 ON l.target_table_full_name = s0.stage_0
# MAGIC ),
# MAGIC
# MAGIC stage_2 AS (
# MAGIC   SELECT l.source_table_full_name AS stage_2, s1.stage_1, s1.stage_0
# MAGIC   FROM latest_lineage l
# MAGIC   JOIN stage_1 s1 ON l.target_table_full_name = s1.stage_1
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   stage_2,
# MAGIC   stage_1,
# MAGIC   stage_0,
# MAGIC
# MAGIC   -- Ziel-Infos aus stage_0
# MAGIC   split(stage_0, '\\.')[0] AS target_catalog,
# MAGIC   split(stage_0, '\\.')[1] AS target_schema,
# MAGIC   split(stage_0, '\\.')[2] AS target_table
# MAGIC FROM stage_2

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_events AS (
# MAGIC   SELECT
# MAGIC     target_table_full_name,
# MAGIC     MAX(event_date) AS max_event_date
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE target_table_full_name LIKE 'pilatus_hub_dev.%'
# MAGIC   GROUP BY target_table_full_name
# MAGIC ),
# MAGIC
# MAGIC latest_lineage AS (
# MAGIC   SELECT DISTINCT
# MAGIC     l.source_table_full_name,
# MAGIC     l.target_table_full_name
# MAGIC   FROM system.access.table_lineage l
# MAGIC   JOIN latest_events e
# MAGIC     ON l.target_table_full_name = e.target_table_full_name
# MAGIC    AND l.event_date = e.max_event_date
# MAGIC   WHERE l.source_table_full_name IS NOT NULL
# MAGIC     AND l.source_table_full_name != l.target_table_full_name
# MAGIC     AND NOT split(l.source_table_full_name, '\\.')[2] LIKE '__materialization_%'
# MAGIC ),
# MAGIC
# MAGIC stage_0 AS (
# MAGIC   SELECT DISTINCT target_table_full_name AS stage_0 FROM latest_lineage
# MAGIC ),
# MAGIC
# MAGIC stage_1 AS (
# MAGIC   SELECT l.source_table_full_name AS stage_1, s0.stage_0
# MAGIC   FROM latest_lineage l
# MAGIC   JOIN stage_0 s0 ON l.target_table_full_name = s0.stage_0
# MAGIC ),
# MAGIC
# MAGIC stage_2 AS (
# MAGIC   SELECT l.source_table_full_name AS stage_2, s1.stage_1, s1.stage_0
# MAGIC   FROM latest_lineage l
# MAGIC   JOIN stage_1 s1 ON l.target_table_full_name = s1.stage_1
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   stage_2,
# MAGIC   stage_1,
# MAGIC   stage_0,
# MAGIC
# MAGIC   -- Ziel-Infos aus stage_0
# MAGIC   split(stage_0, '\\.')[0] AS target_catalog,
# MAGIC   split(stage_0, '\\.')[1] AS target_schema,
# MAGIC   split(stage_0, '\\.')[2] AS target_table
# MAGIC FROM stage_2

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE system.access.column_lineage;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pilatus_hub_dev.gold_saphana.fact_fehlmaterial

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_events AS (
# MAGIC   SELECT
# MAGIC     target_table_full_name,
# MAGIC     MAX(event_date) AS max_event_date
# MAGIC   FROM system.access.column_lineage
# MAGIC   GROUP BY target_table_full_name
# MAGIC ),
# MAGIC
# MAGIC latest_lineage AS (
# MAGIC   SELECT DISTINCT
# MAGIC     l.source_table_full_name,
# MAGIC     l.source_column_name,
# MAGIC     l.target_table_full_name,
# MAGIC     l.target_column_name
# MAGIC   FROM system.access.column_lineage l
# MAGIC   JOIN latest_events e
# MAGIC     ON l.target_table_full_name = e.target_table_full_name
# MAGIC    AND l.event_date = e.max_event_date
# MAGIC   WHERE l.source_column_name IS NOT NULL
# MAGIC     AND l.target_column_name IS NOT NULL
# MAGIC     AND l.source_table_full_name != l.target_table_full_name
# MAGIC     AND NOT split(l.source_table_full_name, '\\.')[2] LIKE '__materialization_%'
# MAGIC ),
# MAGIC
# MAGIC stage_0 AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     target_table_full_name AS stage_0_table,
# MAGIC     target_column_name     AS stage_0_column
# MAGIC   FROM latest_lineage
# MAGIC ),
# MAGIC
# MAGIC stage_1 AS (
# MAGIC   SELECT 
# MAGIC     l.source_table_full_name AS stage_1_table,
# MAGIC     l.source_column_name     AS stage_1_column,
# MAGIC     s0.stage_0_table,
# MAGIC     s0.stage_0_column
# MAGIC   FROM latest_lineage l
# MAGIC   JOIN stage_0 s0
# MAGIC     ON l.target_table_full_name = s0.stage_0_table
# MAGIC    AND l.target_column_name     = s0.stage_0_column
# MAGIC ),
# MAGIC
# MAGIC stage_2 AS (
# MAGIC   SELECT 
# MAGIC     l.source_table_full_name AS stage_2_table,
# MAGIC     l.source_column_name     AS stage_2_column,
# MAGIC     s1.stage_1_table,
# MAGIC     s1.stage_1_column,
# MAGIC     s1.stage_0_table,
# MAGIC     s1.stage_0_column
# MAGIC   FROM latest_lineage l
# MAGIC   JOIN stage_1 s1
# MAGIC     ON l.target_table_full_name = s1.stage_1_table
# MAGIC    AND l.target_column_name     = s1.stage_1_column
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   -- Columns per Stage
# MAGIC   stage_2_table,
# MAGIC   stage_2_column,
# MAGIC   stage_1_table,
# MAGIC   stage_1_column,
# MAGIC   stage_0_table,
# MAGIC   stage_0_column,
# MAGIC
# MAGIC   -- Catalog, Schema, Table from stages
# MAGIC   split(stage_0_table, '\\.')[0] AS 0_catalog,
# MAGIC   split(stage_0_table, '\\.')[1] AS 0_schema,
# MAGIC   split(stage_0_table, '\\.')[2] AS 0_table,
# MAGIC
# MAGIC   split(stage_1_table, '\\.')[0] AS 1_catalog,
# MAGIC   split(stage_1_table, '\\.')[1] AS 1_schema,
# MAGIC   split(stage_1_table, '\\.')[2] AS 1_table,
# MAGIC
# MAGIC   split(stage_2_table, '\\.')[0] AS 2_catalog,
# MAGIC   split(stage_2_table, '\\.')[1] AS 2_schema,
# MAGIC   split(stage_2_table, '\\.')[2] AS 2_table
# MAGIC
# MAGIC FROM stage_2
# MAGIC WHERE split(stage_0_table, '\\.')[2]  = 'fact_fehlmaterial'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE pilatus_hub_dev.gold_saphana.fact_fehlmaterial

# COMMAND ----------


