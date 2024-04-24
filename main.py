import psycopg2
import random

def connect_db():
    try:
        return psycopg2.connect(
            host="localhost",
            dbname="fraud_test",
            user="postgres",
            password="2001",
            port=5432
        )
    except psycopg2.Error as e:
        print("Error connecting to the database: ", e)
        return None

def setup_database():
    conn = connect_db()
    if conn is None:
        print("Failed to connect to the database.")
        return None
    
    cur = conn.cursor()
    try:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS City (
            city_id SERIAL PRIMARY KEY,
            city VARCHAR(255),
            state VARCHAR(255),
            zip VARCHAR(10),
            city_pop INT
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS Customer (
            customer_id SERIAL PRIMARY KEY,
            cc_num BIGINT UNIQUE,
            first VARCHAR(255),
            last VARCHAR(255),
            gender CHAR(1),
            dob DATE,
            job VARCHAR(255),
            street VARCHAR(255),
            city_id INT,
            lat DECIMAL(10, 7),
            long DECIMAL(10, 7),
            FOREIGN KEY (city_id) REFERENCES City(city_id)
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS Merchant (
            merchant_id SERIAL PRIMARY KEY,
            merchant_name VARCHAR(255),
            category VARCHAR(255),
            merch_lat DECIMAL(10, 7),
            merch_long DECIMAL(10, 7)
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS Transaction (
            trans_num VARCHAR(255) PRIMARY KEY,
            trans_date DATE,
            amt DECIMAL(10, 2),
            unix_time BIGINT,
            is_fraud SMALLINT,
            cc_num BIGINT,
            merchant_id INT,
            FOREIGN KEY (cc_num) REFERENCES Customer(cc_num),
            FOREIGN KEY (merchant_id) REFERENCES Merchant(merchant_id)
        );
        """)
        conn.commit()
    except psycopg2.Error as e:
        print("An error occurred while setting up the database:", e)
        conn.rollback()
        cur.close()
        conn.close()
        return None
    cur.close()
    return conn

def prompt_insert_data(conn):
    print("Choose a table to insert data:")
    print("1. City")
    print("2. Customer")
    print("3. Merchant")
    print("4. Transaction")
    choice = input("Enter your choice: ")

    if choice == '1':
        insert_city(conn)
    elif choice == '2':
        insert_customer(conn)
    elif choice == '3':
        insert_merchant(conn)
    elif choice == '4':
        insert_transaction(conn)
    else:
        print("Invalid choice. Please try again.")

def prompt_delete_data(conn):
    print("Choose a table to delete data from:")
    print("1. City")
    print("2. Customer")
    print("3. Merchant")
    print("4. Transaction")
    choice = input("Enter your choice: ")

    if choice == '1':
        delete_city(conn)
    elif choice == '2':
        delete_customer(conn)
    elif choice == '3':
        delete_merchant(conn)
    elif choice == '4':
        delete_transaction(conn)
    else:
        print("Invalid choice. Please try again.")

def prompt_update_data(conn):
    print("Choose a table to update data:")
    print("1. City")
    print("2. Customer")
    print("3. Merchant")
    print("4. Transaction")
    choice = input("Enter your choice: ")

    if choice == '1':
        update_city(conn)
    elif choice == '2':
        update_customer(conn)
    elif choice == '3':
        update_merchant(conn)
    elif choice == '4':
        update_transaction(conn)
    else:
        print("Invalid choice. Please try again.")

def prompt_search_data(conn):
    print("Choose a table to search data from:")
    print("1. City")
    print("2. Customer")
    print("3. Merchant")
    print("4. Transaction")
    choice = input("Enter your choice: ")

    if choice == '1':
        search_city(conn)
    elif choice == '2':
        search_customer(conn)
    elif choice == '3':
        search_merchant(conn)
    elif choice == '4':
        search_transaction(conn)
    else:
        print("Invalid choice. Please try again.")

def insert_city(conn):
    city = input("Enter city name: ")
    state = input("Enter state: ")
    zip_code = input("Enter ZIP code: ")
    city_pop = int(input("Enter city population: "))
    query = "INSERT INTO City (city, state, zip, city_pop) VALUES (%s, %s, %s, %s)"
    execute_query(conn, query, (city, state, zip_code, city_pop))

def insert_customer(conn):
    first = input("Enter first name: ")
    last = input("Enter last name: ")
    gender = input("Enter gender (M/F): ")
    dob = input("Enter date of birth (YYYY-MM-DD): ")
    job = input("Enter job: ")
    street = input("Enter street: ")
    city_id = int(input("Enter city ID: "))
    lat = float(input("Enter latitude: "))
    long = float(input("Enter longitude: "))
    cc_num = random.randint(1000000000000000, 9999999999999999)  # Auto-generate cc_num
    query = "INSERT INTO Customer (cc_num, first, last, gender, dob, job, street, city_id, lat, long) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    execute_query(conn, query, (cc_num, first, last, gender, dob, job, street, city_id, lat, long))

def insert_merchant(conn):
    merchant_name = input("Enter merchant name: ")
    category = input("Enter category: ")
    merch_lat = float(input("Enter merchant latitude: "))
    merch_long = float(input("Enter merchant longitude: "))
    query = "INSERT INTO Merchant (merchant_name, category, merch_lat, merch_long) VALUES (%s, %s, %s, %s)"
    execute_query(conn, query, (merchant_name, category, merch_lat, merch_long))

def insert_transaction(conn):
    trans_num = input("Enter transaction number: ")
    trans_date = input("Enter transaction date (YYYY-MM-DD): ")
    amt = float(input("Enter amount: "))
    unix_time = int(input("Enter unix time: "))
    is_fraud = int(input("Enter is fraud (0 for no, 1 for yes): "))
    cc_num = int(input("Enter customer's cc_num: "))
    merchant_id = int(input("Enter merchant ID: "))
    query = "INSERT INTO Transaction (trans_num, trans_date, amt, unix_time, is_fraud, cc_num, merchant_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    execute_query(conn, query, (trans_num, trans_date, amt, unix_time, is_fraud, cc_num, merchant_id))

def delete_city(conn):
    city_id = int(input("Enter the city ID to delete: "))
    query = "DELETE FROM City WHERE city_id = %s"
    execute_query(conn, query, (city_id,))

def delete_customer(conn):
    customer_id = int(input("Enter the customer ID to delete: "))
    query = "DELETE FROM Customer WHERE customer_id = %s"
    execute_query(conn, query, (customer_id,))

def delete_merchant(conn):
    merchant_id = int(input("Enter the merchant ID to delete: "))
    query = "DELETE FROM Merchant WHERE merchant_id = %s"
    execute_query(conn, query, (merchant_id,))

def delete_transaction(conn):
    trans_num = input("Enter the transaction number to delete: ")
    query = "DELETE FROM Transaction WHERE trans_num = %s"
    execute_query(conn, query, (trans_num,))

def update_city(conn):
    city_id = int(input("Enter the city ID to update: "))
    new_city = input("Enter new city name: ")
    new_state = input("Enter new state: ")
    new_zip = input("Enter new ZIP code: ")
    new_city_pop = int(input("Enter new city population: "))
    query = "UPDATE City SET city = %s, state = %s, zip = %s, city_pop = %s WHERE city_id = %s"
    execute_query(conn, query, (new_city, new_state, new_zip, new_city_pop, city_id))

def update_customer(conn):
    customer_id = int(input("Enter the customer ID to update: "))
    new_first = input("Enter new first name: ")
    new_last = input("Enter new last name: ")
    new_gender = input("Enter new gender (M/F): ")
    new_dob = input("Enter new date of birth (YYYY-MM-DD): ")
    new_job = input("Enter new job: ")
    new_street = input("Enter new street: ")
    new_city_id = int(input("Enter new city ID: "))
    new_lat = float(input("Enter new latitude: "))
    new_long = float(input("Enter new longitude: "))
    query = """
    UPDATE Customer 
    SET first = %s, last = %s, gender = %s, dob = %s, job = %s, street = %s, city_id = %s, lat = %s, long = %s 
    WHERE customer_id = %s
    """
    params = (new_first, new_last, new_gender, new_dob, new_job, new_street, new_city_id, new_lat, new_long, customer_id)
    execute_query(conn, query, params)


def update_merchant(conn):
    merchant_id = int(input("Enter the merchant ID to update: "))
    new_merchant_name = input("Enter new merchant name: ")
    new_category = input("Enter new category: ")
    new_merch_lat = float(input("Enter new merchant latitude: "))
    new_merch_long = float(input("Enter new merchant longitude: "))
    query = "UPDATE Merchant SET merchant_name = %s, category = %s, merch_lat = %s, merch_long = %s WHERE merchant_id = %s"
    execute_query(conn, query, (new_merchant_name, new_category, new_merch_lat, new_merch_long, merchant_id))

def update_transaction(conn):
    trans_num = input("Enter the transaction number to update: ")
    new_trans_date = input("Enter new transaction date (YYYY-MM-DD): ")
    new_amt = float(input("Enter new amount: "))
    new_unix_time = int(input("Enter new unix time: "))
    new_is_fraud = int(input("Enter new fraud status (0 for no, 1 for yes): "))
    new_cc_num = int(input("Enter new customer's cc_num: "))
    new_merchant_id = int(input("Enter new merchant ID: "))
    query = "UPDATE Transaction SET trans_date = %s, amt = %s, unix_time = %s, is_fraud = %s, cc_num = %s, merchant_id = %s WHERE trans_num = %s"
    execute_query(conn, query, (new_trans_date, new_amt, new_unix_time, new_is_fraud, new_cc_num, new_merchant_id, trans_num))

def search_city(conn):
    condition = input("Enter search condition (e.g., 'city_pop > 10000'): ")
    query = f"SELECT * FROM City WHERE {condition}"
    execute_query(conn, query)

def search_customer(conn):
    condition = input("Enter search condition (e.g., 'gender = \'M\' and last = \'Smith\''): ")
    query = f"SELECT * FROM Customer WHERE {condition}"
    execute_query(conn, query)

def search_merchant(conn):
    condition = input("Enter search condition (e.g., 'category = \'Retail\''): ")
    query = f"SELECT * FROM Merchant WHERE {condition}"
    execute_query(conn, query)

def search_transaction(conn):
    condition = input("Enter search condition (e.g., 'amt > 100.00 and is_fraud = 1'): ")
    query = f"SELECT * FROM Transaction WHERE {condition}"
    execute_query(conn, query)

def execute_query(conn, query, params=None, transaction=False):
    with conn.cursor() as cur:
        try:
            if transaction:
                cur.execute("BEGIN;")  # Start a transaction explicitly
            
            cur.execute(query, params or ())
            if cur.description:  # If the query returns a result set
                records = cur.fetchall()
                if records:
                    # Fetch the column headers
                    headers = [desc[0] for desc in cur.description]
                    # Print headers
                    print("\t".join(headers))
                    # Print each row
                    for record in records:
                        print("\t".join(str(item) for item in record))
                else:
                    print("No records found.")
            else:
                print("Affected rows:", cur.rowcount)

            if transaction:
                if "commit" in query.lower() or "rollback" in query.lower():
                    conn.commit()  # Commit or rollback within the same cursor to maintain transaction integrity
                else:
                    print("Transaction pending: COMMIT or ROLLBACK to complete.")
            else:
                conn.commit()
                
        except psycopg2.Error as e:
            print(f"An error occurred: {e}")
            if transaction:
                cur.execute("ROLLBACK;")
                print("Transaction rolled back due to error.")
            conn.rollback()
        finally:
            print("Query executed successfully.")




def aggregate_operations(conn, table):
    column = input(f"Enter the column name in {table} to perform calculations: ")
    function = input("Enter aggregate function to use (SUM, AVG, COUNT, MIN, MAX): ")
    query = f"SELECT {function}({column}) FROM {table}"
    execute_query(conn, query)

def prompt_aggregate_functions(conn):
    print("Choose a table to perform aggregate functions:")
    print("1. City")
    print("2. Customer")
    print("3. Merchant")
    print("4. Transaction")
    choice = input("Enter your choice: ")

    if choice == '1':
        aggregate_operations(conn, "City")
    elif choice == '2':
        aggregate_operations(conn, "Customer")
    elif choice == '3':
        aggregate_operations(conn, "Merchant")
    elif choice == '4':
        aggregate_operations(conn, "Transaction")
    else:
        print("Invalid choice. Please try again.")

def sort_operations(conn, table):
    column = input(f"Enter the column name in {table} to sort by: ")
    order = input("Enter sort order (ASC for ascending or DESC for descending): ")
    query = f"SELECT * FROM {table} ORDER BY {column} {order}"
    execute_query(conn, query)

def prompt_sort_data(conn):
    print("Choose a table to sort data from:")
    print("1. City")
    print("2. Customer")
    print("3. Merchant")
    print("4. Transaction")
    choice = input("Enter your choice: ")

    if choice == '1':
        sort_operations(conn, "City")
    elif choice == '2':
        sort_operations(conn, "Customer")
    elif choice == '3':
        sort_operations(conn, "Merchant")
    elif choice == '4':
        sort_operations(conn, "Transaction")
    else:
        print("Invalid choice. Please try again.")




def join_operations(conn):
    first_table = input("Enter the first table name: ")
    second_table = input("Enter the second table name: ")
    join_type = input("Enter the type of join (INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN): ")
    join_condition = input("Enter the join condition (e.g., 'Table1.Key = Table2.Key'): ")
    query = f"SELECT * FROM {first_table} {join_type} {second_table} ON {join_condition}"
    execute_query(conn, query)

def prompt_join_data(conn):
    print("Data joining operation:")
    join_operations(conn)

def handle_transaction_operations(conn):
    print("Starting transaction mode. Enter queries to execute or 'COMMIT' to commit or 'ROLLBACK' to abort.")
    while True:
        query = input("Enter SQL query or 'COMMIT'/'ROLLBACK' to control transaction: ")
        if query.lower() == 'commit' or query.lower() == 'rollback':
            execute_query(conn, query, transaction=True)  
            break
        else:
            execute_query(conn, query, transaction=True)  



def group_operations(conn, table):
    grouping_columns = input("Enter column(s) to group by (comma-separated if multiple): ")
    aggregate_query = input("Enter aggregate function and column (e.g., 'SUM(amount)'): ")
    query = f"SELECT {grouping_columns}, {aggregate_query} FROM {table} GROUP BY {grouping_columns}"
    execute_query(conn, query)

def prompt_group_data(conn):
    print("Choose a table to group data from:")
    print("1. City")
    print("2. Customer")
    print("3. Merchant")
    print("4. Transaction")
    choice = input("Enter your choice: ")

    table_dict = {
        '1': 'City',
        '2': 'Customer',
        '3': 'Merchant',
        '4': 'Transaction'
    }
    
    if choice in table_dict:
        group_operations(conn, table_dict[choice])
    else:
        print("Invalid choice. Please try again.")

def subquery_operations(conn):
    main_query = input("Enter the main SQL query (e.g., SELECT * FROM Customer WHERE city_id IN): ")
    subquery = input("Enter the subquery (e.g., SELECT city_id FROM City WHERE city_pop > 100000): ")
    full_query = f"{main_query} ({subquery})"
    execute_query(conn, full_query)

def prompt_subquery_data(conn):
    print("Perform operations involving subqueries:")
    subquery_operations(conn)

def cli():
    conn = setup_database()
    if conn is None:
        return

    actions = {
        "1": prompt_insert_data,
        "2": prompt_delete_data,
        "3": prompt_update_data,
        "4": prompt_search_data,
        "5": prompt_aggregate_functions,
        "6": prompt_sort_data,
        "7": prompt_join_data,
        "8": handle_transaction_operations,
        "9": prompt_group_data,
        "10": prompt_subquery_data  # Added subquery functionality
    }

    while True:
        print("\nDatabase Management CLI")
        print("1. Insert Data")
        print("2. Delete Data")
        print("3. Update Data")
        print("4. Search Data")
        print("5. Aggregate Functions")
        print("6. Sort Data")
        print("7. Join Data")
        print("8. Handle Transactions")
        print("9. Group Data")
        print("10. Subquery Operations")  # New menu option for subqueries
        print("11. Exit")

        choice = input("Enter your choice: ")
        if choice == '11':
            print("Exiting program.")

            break
        if choice in actions:
            actions[choice](conn)
        else:
            print("Invalid choice. Please try again.")

    conn.close()

if __name__ == "__main__":
    cli()