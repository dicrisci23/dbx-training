# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Dummy Data with Faker

# COMMAND ----------

# MAGIC %md
# MAGIC https://github.com/databrickslabs/dbldatagen   
# MAGIC

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

import re
import uuid
from pyspark.sql.functions import current_timestamp, sha2, concat, lit, col, udf
from pyspark.sql.types import StringType

from faker import Faker
fake = Faker()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Unity Catalog

# COMMAND ----------

def transform_email(email):
    match = re.match(r'^([^@]+)@', email)
    if match:
        username = match.group(1)
        username = re.sub(r'[._]', '_', username)
        return username
    else:
        return None

# COMMAND ----------

user_id = spark.sql('select current_user() as user').collect()[0]['user']
user_catalog_name = transform_email(user_id)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {user_catalog_name}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {user_catalog_name}.person")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Customer Data

# COMMAND ----------

# Create a list of fake person data
data = []
for i in range(10000):
    # Generate a unique primary key
    primary_key = str(uuid.uuid4())
    
    name = fake.first_name()
    surname = fake.last_name()
    dob = fake.date_of_birth()
    nationality = fake.country()
    address = fake.address()
    phone_number = fake.phone_number()
    email = fake.email()
    religion = fake.random_element(['Christianity', 'Islam', 'Hinduism', 'Buddhism', 'Judaism'])
    age = fake.random_int(min=18, max=99)
    education = fake.random_element(['High School', 'Bachelor', 'Master', 'PhD'])
    occupation = list(fake.random_elements(['Engineer', 'Teacher', 'Doctor', 'Lawyer', 'Artist', 'Chef', 'Scientist', 'Writer', 'Athlete', 'Musician', 'Architect', 'Designer', 'Pilot', 'Actor', 'Police Officer', 'Firefighter', 'Nurse', 'Accountant', 'Entrepreneur', 'Psychologist', 'Journalist', 'Salesperson', 'Veterinarian', 'Electrician', 'Plumber', 'Mechanic', 'Chef', 'Waiter', 'Baker', 'Barista', 'Hairdresser', 'Construction Worker', 'Carpenter', 'Librarian', 'Pharmacist', 'Dentist', 'Singer', 'Photographer', 'Flight Attendant'], length=1, unique=True))[0]
    income = fake.random_int(min=10000, max=100000)   
    marital_status = fake.random_element(['Single', 'Married', 'Divorced'])
    sex = fake.random_element(['Male', 'Female', 'Not Sure'])
    hobby = fake.random_element(['Reading', 'Sports', 'Painting', 'Cooking', 'Traveling'])

    data.append((primary_key, name, surname, dob, nationality, address, phone_number, email, religion, age, education, occupation, income, marital_status, sex, hobby))

# Create a DataFrame
df_source = spark.createDataFrame(data, ['primary_key', 'name', 'surname', 'date_of_birth', 'nationality', 'address', 'phone_number', 'email', 'religion', 'age', 'education', 'occupation', 'income', 'marital_status', 'sex', 'hobby'])

# Display the DataFrame
display(df_source)

# COMMAND ----------

# Save the DataFrame as a table in the new schema
df_source.write.mode("append").saveAsTable(f"{user_catalog_name}.person.customer")

 # Display the data
display(spark.sql(f"SELECT * FROM {user_catalog_name}.person.customer"))

# COMMAND ----------


