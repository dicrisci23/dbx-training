# Databricks notebook source
# MAGIC %md
# MAGIC # Integrated Notebook Debugger
# MAGIC You can debug your code within the Notebook

# COMMAND ----------

import random

# Perform initial calculations
input_value = random.randint(1, 1000)
intermediate_result = input_value * 2
threshold = 100

# Apply conditions and transformations
if intermediate_result > threshold:
    transformed_value = intermediate_result + 10
else:
    transformed_value = intermediate_result - 10

# Perform iterative processing
iteration_count = 5
final_result = transformed_value

for i in range(iteration_count):
    final_result += i

# Generate output
print("The final result is: {}".format(final_result))
