from pyhive import hive
import pyodbc 
import pandas as pd 
import logging
import os

# logging basic file config:-
logging.basicConfig(filename="log.txt",level=logging.DEBUG,
                    filemode='a',
                    format= '%(asctime)s - %(message)s',
                    datefmt= '%d-%b-%y %H:%M:%S') 

# to create database
def create_database(dbname, username):
      
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='default')
        cur = connection.cursor()
        cur.execute(f"CREATE DATABASE {dbname}")
        
        print(f"\n Database:- {dbname} is created successfully \n")
        logging.info(f"Database:- {dbname}is created successfully")
        
    except Exception as e:
        logging.error(e)   


"""Duc Nhan and Thien Nhan: Customer Segmentation task"""
# create Rental Dimension
def CreateTableDimRental(username):
    
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database = 'sakila_dwh')
        cur = connection.cursor()
        
        cur.execute(f'''CREATE TABLE Dim_Rental (
                            rental_key INT,
                            rental_id INT,
                            rental_date DATE,
                            inventory_id INT,
                            customer_id INT,
                            return_date DATE,
                            staff_id INT,
                            amount FLOAT,
                            payment_date DATE
                        )
                        PARTITIONED BY (rental_month INT)
                        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                        STORED AS TEXTFILE''')
        
        print(f"DimRental is created successfully \n")
        logging.info('Table:- DimRental is created successfully')
        
    except Exception as e:
        logging.error(e)
        
def CreateTableDimCustomer(username):
    
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='sakila_dwh')
        cur = connection.cursor()
        
        cur.execute(f'''CREATE TABLE Dim_Customer (
                            customer_key INT,
                            customer_id INT,
                            store_id INT,
                            address_id INT,
                            active INT,
                            full_name string,
                            city string,
                            country string
                        )
                        CLUSTERED BY (customer_id) INTO 6 BUCKETS
                        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                        STORED AS TEXTFILE''')
        
        print(f"DimCustomer is created successfully \n")
        logging.info('Table:- DimCustomer is created successfully')
        
    except Exception as e:
        logging.error(e) 

def CreateTableFactSegment(username):
    
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='sakila_dwh')
        cur = connection.cursor()
        
        cur.execute(f'''CREATE TABLE Fact_Segment (
                            customer_id INT,
                            city string,
                            country string,
                            active TINYINT,
                            full_name string,
                            rental_id INT,
                            amount FLOAT,
                            rental_date DATE,
                            first_date DATE,
                            recency INT,
                            monetary FLOAT,
                            frequency INT
                        )
                        CLUSTERED BY (customer_id) INTO 8 BUCKETS
                        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                        STORED AS TEXTFILE''')
        
        print(f"FactSegment is created successfully \n")
        logging.info('Table:- FactSegment is created successfully')
        
    except Exception as e:
        logging.error(e)

def IntegrateFactSegment(username):
    
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='sakila_dwh')
        cur = connection.cursor()
        
        cur.execute(f'''INSERT INTO TABLE Fact_Segment
                        SELECT
                            c.customer_id, c.city, c.country, c.active, c.full_name,
                            r.rental_id, r.amount, r.rental_date,
                            (SELECT MIN(rental_date) FROM Dim_Rental sub_r WHERE sub_r.customer_id = c.customer_id) AS first_date,
                            DATEDIFF(
                                (SELECT MAX(rental_date) FROM Dim_Rental sub_r WHERE sub_r.customer_id = c.customer_id),
                                (SELECT MIN(rental_date) FROM Dim_Rental sub_r WHERE sub_r.customer_id = c.customer_id)
                            ) AS recency,
                            (SELECT SUM(amount) FROM dim_rental sub_r WHERE sub_r.customer_id = c.customer_id) AS monetary,
                            (SELECT COUNT(rental_id) FROM dim_rental sub_r WHERE sub_r.customer_id = c.customer_id) AS frequency
                        FROM dim_customer c
                        JOIN dim_rental r ON r.customer_id = c.customer_id''')
        
        print(f"FactSegment is integrated successfully \n")
        logging.info('Table:- FactSegment is integrated successfully')
        
    except Exception as e:
        logging.error(e)

# to load data into csv table
def LoadData(csv_file_path, tablename, username):
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='sakila_dwh')
        absolute_csv_file_path = os.path.abspath('data').replace("\\", "/")
        temp_path=os.path.join(absolute_csv_file_path,"tables").replace("\\","/")
        final_path=os.path.join(temp_path,csv_file_path).replace("\\", "/")
        
        load_data_sql = f""" 
                        LOAD DATA LOCAL INPATH '/{final_path}'
                        OVERWRITE INTO TABLE {tablename}
                        """
        # Execute SQL statement
        cursor = connection.cursor()
        cursor.execute(load_data_sql)
        cursor.close()
        print(f"Data loaded into {tablename} successfully.")
        
    except Exception as e:
        logging.error(e)
        return None
    
    
# drop table temp table;
def drop_table(table_name, username):
    
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='sakila_dwh')
        cur = connection.cursor()
        
        cur.execute(f"DROP TABLE {table_name}")
        
        print(f'\n Table: {table_name} is deleted successfully \n')        
        logging.info(f'Table: {table_name} is deleted successfully')
        
    except Exception as e:
        logging.error(e) 

#store the row details into python list of tuples
def ExtractRows(query, username): 
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='sakila_dwh')
        
        df = pd.read_sql(query, connection)

        records = df.to_records(index=False)
        result = list(records)
        print('\n converting the rows_data into python list of tuples \n')
        print('converted suscessfully the rows_data into python list of tuples')
    
    except Exception as e:
        logging.error(e)
        return None


"""Quang Huy and Trieu Phi: Inventory Analysis task
"""

def CreateTableFact_Inventory_Analysis_TextFile(username):
    try:
        connection=hive.Connection(host='127.0.0.1',port='10000',username = username,database='sakila_dwh')
        create_table_sql="""CREATE TABLE Fact_Inventory_Analysis_TextFile (
                                        inventory_key INT,
                                        rental_key INT,
                                        rental_date_key int,
                                        remaining INT,
                                        Total_Rental_Amount FLOAT 
                                              )
                                    ROW FORMAT DELIMITED
                                    FIELDS TERMINATED BY ','
                                    STORED AS TEXTFILE
                                    TBLPROPERTIES ('skip.header.line.count'='1')
                                """
        cursor=connection.cursor()
        cursor.execute(create_table_sql)
        cursor.close()
        print(f"Table Fact_Inventory_Analysis_TextFile created successfully")
    except Exception as e:
            logging.error(e)
            return None 
        

def CreateTableFact_Inventory_Analysis_ORC(username):
    try:
        connection=hive.Connection(host='127.0.0.1',port='10000',username = username,database='sakila_dwh')
        create_table_sql="""
                            CREATE TABLE Fact_Inventory_Analysis_ORC STORED AS ORC
                            AS SELECT * FROM Fact_Inventory_Analysis_TextFile
                         """
        cursor=connection.cursor()
        cursor.execute(create_table_sql)
        cursor.close()
        print(f"Table Fact_Inventory_Analysic_ORC created successfully")
    except Exception as e:
            logging.error(e)
            return None 
        

def CreateTableDimDate(username):
    try:
        # lệnh kết nối
        connection=hive.Connection(host='127.0.0.1',port="10000",username=username,
                                    database='sakila_dwh')
        create_table_sql="""CREATE TABLE IF NOT EXISTS DimDate (
                                    date_key int,
                                    full_date STRING,
                                    day_of_week INT,
                                    day_num_in_month INT,
                                    day_num_overall INT,
                                    day_name STRING,
                                    day_abbrev STRING,
                                    weekday_flag STRING,
                                    week_num_in_year INT,
                                    week_num_overall INT,
                                    week_begin_date STRING,
                                    week_begin_date_key STRING,
                                    month STRING,
                                    month_num_overall INT,
                                    month_name STRING,
                                    month_abbrev STRING,
                                    quarter INT,
                                    year INT,
                                    year_month STRING,
                                    fiscal_month INT,
                                    fiscal_quarter INT,
                                    fiscal_year INT,
                                    month_end_flag STRING
                                )
                                ROW FORMAT DELIMITED
                                FIELDS TERMINATED BY ','
                                STORED AS TEXTFILE
                                TBLPROPERTIES ('skip.header.line.count'='1')
                                """
        cursor=connection.cursor()
        cursor.execute(create_table_sql)
        cursor.close()
        print(f"Table DimDate created successfully")
    except Exception as e:
            logging.error(e)
            return None  


#store the row details into python list of tuples
def CreateDimInventory(username): 
    try:
        # Kết nối đến Hive server
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='sakila_dwh')
        # Lệnh SQL để tạo bảng
        create_table_sql = """
        CREATE TABLE DimInventory (
            inventory_key INT,
            inventory_id INT,
            title STRING,
            description STRING,
            release_year INT,
            language STRING,
            rental_duration INT,
            rental_rate DECIMAL(5,2),
            length INT,
            replacement_cost DECIMAL(5,2),
            rating STRING,
            special_features STRING,
            catalogy_name STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        TBLPROPERTIES ('skip.header.line.count'='1')
        """
        # Thực thi lệnh SQL
        cursor = connection.cursor()
        cursor.execute(create_table_sql)
        cursor.close()
        print(f"Table DimInventory created successfully.")
    except Exception as e:
        logging.error(e)
        return None    
 
def df_rows_details(query, username): 
    try:
        connection = hive.Connection(host="127.0.0.1", port="10000", username=username, database='sakila_dwh')
        set_join = f"""
        set hive.auto.convert.join=false
        """
        # Thực thi lệnh SQL
        cursor = connection.cursor()
        cursor.execute(set_join)
        df = pd.read_sql(query, connection)
        print('\n converting the rows_data into DataFrame \n')
        print('converted successfully the rows_data into DataFrame')
        print(df)  # In ra dữ liệu của DataFrame
        
    except Exception as e:
        logging.error(e)
        return None    
    
    return df

    