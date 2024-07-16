## Introduction
Data set context: Sakila contains data about movie rental and payment transactions. In addition, it also contains information about movies and customers.

**Goal:** Build a data warehouse with Apache Hive, extract, transform, and store (ETL) in Dimension and Fact tables. Serves for analyzing operations, helping to improve business strategies to bring profits to businesses

### 1. Customer Segmentation

Customer segmentation: aims to group customers according to certain behavioral characteristics, forming data clusters. From there, marketing campaigns or incentives will easily reach the appropriate customer groups
- Use clustering model according to `KMean` algorithm: important attributes in this model are `recency`, `frequency` and `monetary`
- The performance measures of the model are `SSE`, sum of squared distances to cluster centers and `Silhouette Coefficient` method. From there, you can choose the best number of clusters so that the data is clustered more clearly

### 2. Film Inventory Analysis

The goal is to track the quantity of inventory and revenue of each film in the film warehouse. So that, there will be strategies to adjust the process of importing and exporting inventory accordingly

## Integration

```python
from src.hiveconnect import hiveapp

username='***' # your window username

# Create dimension tables
hiveapp.CreateTableDimRental(username=username)
hiveapp.CreateTableDimCustomer(username=username)
DimInventory=hiveapp.CreateDimInventory(username)
DimDate=hiveapp.CreateTableDimDate(username)

# Load source data to stages (csv) in preprocessing folder
# Load csv stages to dimension tables
hiveapp.LoadData('dimRental.txt', 'dim_rental', username=username)
hiveapp.LoadData('dimCustomer.csv', 'dim_customer', username=username)
Load_data_to_DimInventory=hiveapp.LoadData("dimInventory.csv","DimInventory", username)
Load_data_to_DimDate=hiveapp.LoadData("dimDate.csv","DimDate", username)

# Create and Integrate fact table
hiveapp.CreateTableFactSegment(username=username)
hiveapp.IntegrateFactSegment(username=username)

# Create and Integrate data to Fact Inventory Film
Fact_Inventory_Analysis_TextFile=hiveapp.CreateTableFact_Inventory_Analysis_TextFile(username)
Load_data_to_Fact_Inventory_TextFile=hiveapp.LoadData("Fact_Inventory_Analysis.csv",'Fact_Inventory_Analysis_TextFile',username)
Fact_Inventory_Analysis_ORC=hiveapp.CreateTableFact_Inventory_Analysis_ORC(username)
```

## Connect Apache Hive on Python
> **Note:** Edit the core-site.xml file in Hadoop, add the proxy configuration section for the user and close the file
```xml
<property>
    <name>hadoop.proxyuser.<username>.hosts</name>
    <value>*</value>
</property>
<property>
    <name>hadoop.proxyuser.<username>.groups</name>
    <value>*</value>
</property>
```
> **Note:** start hiveserver2 before connect
```
hive --service hiveserver2 start
```

## Schemas

* Work-flow:

![Screenshot 2024-07-16 165738](https://github.com/user-attachments/assets/8e6a0bda-f377-4d0a-a6a7-464c1b6f8466)


* Galaxy Schema of Data Warehouse:

![Screenshot 2024-07-16 165751](https://github.com/user-attachments/assets/6341aadd-3316-4448-8739-c711fd309cf6)


