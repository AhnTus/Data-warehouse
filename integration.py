from src.hiveconnect import hiveapp

username='admin'

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



