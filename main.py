import mysql.connector
from mysql.connector import Error


def create_connection(host_name, user_name, user_password, database_name=None):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=database_name,
        )
        print("Подключение к базе данных MySQL прошло успешно")
    except Error as e:
        print(f"Произошла ошибка: {e}")
    return connection


def create_database(connection):
    try:
        cursor = connection.cursor()
        database_name = "Auto_lab6"
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"База данных '{database_name}' создана успешно")
    except Error as e:
        print(f"Произошла ошибка: {e}")


def create_tables(connection, ):
    try:
        database_name = "Auto_lab6"
        cursor = connection.cursor()
        cursor.execute(f"USE {database_name}")

        # Создание таблицы "Clients"
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS Clients (
                ClientCode INT AUTO_INCREMENT PRIMARY KEY,
                CompanyName VARCHAR(255),
                BankAccount VARCHAR(20),
                Phone VARCHAR(15),
                ContactPerson VARCHAR(255),
                Address VARCHAR(255)
            )
        ''')

        # Создание таблицы "Cars"
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Cars (
                CarID INT AUTO_INCREMENT PRIMARY KEY,
                CarBrand ENUM('fiesta', 'focus', 'fusion', 'mondeo'),
                NewCarPrice DECIMAL(10, 2),
                ClientCode INT,
                FOREIGN KEY (ClientCode) REFERENCES Clients(ClientCode)
            )
        ''')

        # Создание таблицы "Repairs"
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Repairs (
                RepairCode INT AUTO_INCREMENT PRIMARY KEY,
                RepairStartDate DATE,
                CarCode INT,
                RepairType ENUM('гарантійний', 'плановий', 'капітальний'),
                HourlyRepairCost DECIMAL(10, 2),
                Discount INT CHECK (Discount >= 0 AND Discount <= 10),
                HoursRequired INT,
                FOREIGN KEY (CarCode) REFERENCES Cars(CarID)
            )
        ''')

        connection.commit()
        print("Таблицы созданы успешно")
    except Error as e:
        print(f"Произошла ошибка: {e}")


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Запрос выполнен успешно.")
    except Error as e:
        print(f"Произошла ошибка при выполнении запроса: {e}")


def execute_query_print(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()  # Отримати всі рядки результату запиту
        for row in result:
            print(row)  # Вивести кожен рядок
        print("Запит виконано успішно.")
    except Error as e:
        print(f"Произошла ошибка при выполнении запроса: {e}")


def display(conn):
    connect = conn
    # Відобразити інформацію про всі гарантійні ремонти та відсортувати назви клієнтів за алфавітом:
    query = ("""
    SELECT R.*, C.CompanyName
    FROM Repairs R
    JOIN Cars Ca ON R.CarCode = Ca.CarID
    JOIN Clients C ON Ca.ClientCode = C.ClientCode
    WHERE R.RepairType = 'гарантійний'
    ORDER BY C.CompanyName;
    """)
    execute_query_print(connect, query)

    # Порахувати вартість ремонту, та вартість з урахуванням знижки, для кожного автомобіля (запит з обчислювальним полем):
    query = ("""
    SELECT
        c.CarID,
        c.CarBrand,
        r.RepairCode,
        r.HourlyRepairCost,
        r.Discount,
        r.HoursRequired,
        (r.HourlyRepairCost * r.HoursRequired) AS TotalCost,
        (r.HourlyRepairCost * r.HoursRequired) * (1 - r.Discount / 100) AS TotalCostWithDiscount
    FROM
        Cars c
    JOIN
        Repairs r ON c.CarID = r.CarCode;

        """)
    execute_query_print(connect, query)

    print("Відобразити інформацію по ремонту для всіх авто заданої марки (запит з параметром):")
    print('fiesta', 'focus', 'fusion', 'mondeo')
    car_brand = input()  # Замініть на бажану марку автомобіля
    query = (f"""
    SELECT R.*, Ca.CarBrand
    FROM Repairs R
    JOIN Cars Ca ON R.CarCode = Ca.CarID
    WHERE Ca.CarBrand = '{car_brand}';
    """)
    execute_query_print(connect, query)

    # Порахувати загальну суму, яку сплатив кожен клієнт (підсумковий запит):
    query = ("""
    SELECT C.ClientCode, C.CompanyName, SUM(R.HourlyRepairCost * R.HoursRequired * (1 - R.Discount/100)) AS TotalCostWithDiscount
    FROM Clients C
    LEFT JOIN Cars Ca ON C.ClientCode = Ca.ClientCode
    LEFT JOIN Repairs R ON Ca.CarID = R.CarCode
    GROUP BY C.ClientCode, C.CompanyName;
    """)
    execute_query_print(connect, query)

    # Порахувати кількість кожного типу ремонтів для кожного клієнта (перехресний запит):

    query = ("""
    SELECT C.ClientCode, C.CompanyName,
           SUM(CASE WHEN R.RepairType = 'гарантійний' THEN 1 ELSE 0 END) AS WarrantyRepairs,
           SUM(CASE WHEN R.RepairType = 'плановий' THEN 1 ELSE 0 END) AS PlannedRepairs,
           SUM(CASE WHEN R.RepairType = 'капітальний' THEN 1 ELSE 0 END) AS CapitalRepairs
    FROM Clients C
    LEFT JOIN Cars Ca ON C.ClientCode = Ca.ClientCode
    LEFT JOIN Repairs R ON Ca.CarID = R.CarCode
    GROUP BY C.ClientCode, C.CompanyName;
    """)
    execute_query_print(connect, query)

    # Порахувати кількість ремонтів для кожної марки автомобіля:

    query = ("""
    SELECT Ca.CarBrand,
           SUM(CASE WHEN R.RepairType = 'гарантійний' THEN 1 ELSE 0 END) AS WarrantyRepairs,
           SUM(CASE WHEN R.RepairType = 'плановий' THEN 1 ELSE 0 END) AS PlannedRepairs,
           SUM(CASE WHEN R.RepairType = 'капітальний' THEN 1 ELSE 0 END) AS CapitalRepairs
    FROM Cars Ca
    LEFT JOIN Repairs R ON Ca.CarID = R.CarCode
    GROUP BY Ca.CarBrand;
    """)
    execute_query_print(connect, query)


def insert_tables(conn):
    conn = conn
    query = (f"""
    INSERT INTO Clients (CompanyName, BankAccount, Phone, ContactPerson, Address)
    VALUES
        ('Company A', '1112345678', '0123456789', 'John Doe', 'City A'),
        ('Company B', '98765432', '9876543210', 'Jane Smith', 'City B'),
        ('Company C', '56789012', '5678901234', 'Robert Johnson', 'City C'),
        ('Company D', '11223344', '1122334455', 'Lisa Davis', 'City D'),
        ('Company E', '99887766', '9988776655', 'Michael Wilson', 'City E'),
        ('Company F', '33221100', '3322110099', 'Sarah Anderson', 'City F')
        """)
    execute_query(conn, query)

    query = (f"""INSERT INTO Cars (CarBrand, NewCarPrice, ClientCode)
        VALUES
        ('fiesta', 15000, 1),
        ('focus', 20000, 2),
        ('fusion', 150000, 3),
        ('mondeo', 200000, 4)
            """)
    execute_query(conn, query)

    query = ("""
        INSERT INTO Repairs (RepairStartDate, CarCode, RepairType, HourlyRepairCost, Discount, HoursRequired)
        VALUES
        ('2023-10-15', 1, 'гарантійний', 50, 5, 5),
        ('2023-11-01', 2, 'капітальний', 70, 0, 10),
        ('2023-11-15', 1, 'плановий', 30, 2, 8),
        ('2023-12-01', 2, 'гарантійний', 45, 6, 3),
        ('2023-12-15', 3, 'капітальний', 80, 0, 12),
        ('2024-01-01', 4, 'плановий', 40, 1, 9),
        ('2024-01-15', 1, 'гарантійний', 55, 4, 7),
        ('2024-02-01', 2, 'капітальний', 75, 0, 11),
        ('2024-02-15', 3, 'плановий', 35, 3, 6),
        ('2024-03-01', 4, 'гарантійний', 60, 7, 4),
        ('2024-03-15', 1, 'капітальний', 85, 0, 14),
        ('2024-04-01', 2, 'плановий', 55, 2, 10),
        ('2024-04-15', 3, 'гарантійний', 70, 5, 8),
        ('2024-05-01', 4, 'капітальний', 90, 0, 13),
        ('2024-05-15', 1, 'плановий', 50, 4, 11)
    """)
    execute_query(conn, query)
    conn.close()


if __name__ == "__main__":
    config = {
        'host_name': '127.0.0.1',
        'user_name': 'root',
        'user_password': 'root',
    }
    config1 = {
        'host_name': '127.0.0.1',
        'user_name': 'root',
        'user_password': 'root',
        'database_name': 'Auto_lab6',
    }
    # Подключение к серверу MySQL
    conn = create_connection(**config)

    # Создание базы данных
    create_database(conn)

    # Создание таблиц
    create_tables(conn)
    # Insert Tables
    conn = create_connection(**config1)

    # insert_tables(conn)
    display(conn)

    conn.close()

    print("База данных и таблицы созданы успешно.")
