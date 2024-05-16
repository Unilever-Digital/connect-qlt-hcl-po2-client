import datetime
import time
from datetime import datetime as dt
from app.controls.control import *

def queryOptimizationCheckweigher(table):
    """
    counter bottles server
    """
    connection = connectToSqlServer('DESKTOP-M7H8BIL', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Po2",table)
    
    result = collection.delete_many({})
    startdate = datetime.datetime(2023, 1, 1, 0, 0, 0)
    query = f"""
    SELECT CONVERT(date, DateTime) AS date,
        DAY(DateTime) AS day,
        MONTH(DateTime) AS month,
        YEAR(DateTime) AS year,
        CASE
            WHEN DATEPART(hh, DateTime) < 6 THEN 1
            WHEN DATEPART(hh, DateTime) < 14 THEN 2
            ELSE 3
        END AS shift,
		FGsCode AS sku,
		Line As line,
        COUNT(*) AS count,
        SUM(CASE WHEN Status = 'Good' THEN 1 ELSE 0 END) AS countGood,
        SUM(CASE WHEN Status = 'Not Good' THEN 1 ELSE 0 END) AS countNotgood
    FROM Table_ResultCap
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), Line, FGsCode,
            CASE
                WHEN DATEPART(hh, DateTime) < 6 THEN 1
                WHEN DATEPART(hh, DateTime) < 14 THEN 2
                ELSE 3
            END
    ORDER BY date, shift;
    """
    cursor.execute(query)
    data = cursor.fetchall()
    data_insert = []
    for row in data:
        new_row = {
            "date": row[0],
            "day": row [1],
            "month": row[2],
            "year": row[3],
            "shift": row[4],
            "sku":  row[5],
            "line": row[6],
            "count": row[7],
            "countPass": row[8],
            "countNotgood": row[9],
        }
        data_insert.append(new_row)
    print(data_insert)

    collection.insert_many(data_insert)
    connection.close()

def queryOptimizationImageFail(table):
    """
    counter bottles server
    """
    connection = connectToSqlServer('DESKTOP-M7H8BIL', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Po2", table)
    
    result = collection.delete_many({})
    
    startdate = datetime.datetime(2023, 1, 1, 0, 0, 0)
    pipeline = f"""
    SELECT CONVERT(date, DateTime) AS date,
        DAY(DateTime) AS day,
        MONTH(DateTime) AS month,
        YEAR(DateTime) AS year,
        CASE
            WHEN DATEPART(hh, DateTime) < 6 THEN 1
            WHEN DATEPART(hh, DateTime) < 14 THEN 2
            ELSE 3
        END AS shift,
		SKUID AS sku,
		Line As line,
        COUNT(*) AS count,
        SUM(CASE WHEN Status = 'Good' THEN 1 ELSE 0 END) AS countGood,
        SUM(CASE WHEN Status = 'WrongCode' THEN 1 ELSE 0 END) AS countWrongCode
    FROM Table_ResultCarton
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), Line, SKUID,
            CASE
                WHEN DATEPART(hh, DateTime) < 6 THEN 1
                WHEN DATEPART(hh, DateTime) < 14 THEN 2
                ELSE 3
            END
    ORDER BY date, shift;
    """
    cursor.execute(pipeline)
    group_data = cursor.fetchall()
    data_insert = []
    for row in group_data:
        new_row = {
            "date": row[0],
            "day": row [1],
            "month": row[2],
            "year": row[3],
            "shift": row[4],
            "sku":  row[5],
            "line": row[6],
            "count": row[7],
            "countGood": row[8],
            "countWrongCode": row[9],
        }
        data_insert.append(new_row)

    collection.insert_many(data_insert)
    connection.close()

def queryOptimizationProduct(table):
    """
    counter bottles server
    """
    connection = connectToSqlServer('DESKTOP-M7H8BIL', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Po2", table)
    
    result = collection.delete_many({})
    print(result)

    startdate = datetime.datetime(2023, 1, 1, 0, 0, 0)
    # Define the pipeline string using f-strings for cleaner formatting
    pipeline = f"""
    SELECT CONVERT(date, DateTime) AS date,
        DAY(DateTime) AS day,
        MONTH(DateTime) AS month,
        YEAR(DateTime) AS year,
        CASE
            WHEN DATEPART(hh, DateTime) < 6 THEN 1
            WHEN DATEPART(hh, DateTime) < 14 THEN 2
            ELSE 3
        END AS shift,
        FGsCode AS sku,
        COUNT(*) AS count,
        SUM(CASE WHEN Status = 'Good' THEN 1 ELSE 0 END) AS countPass,
        SUM(CASE WHEN Status = 'NotGood' THEN 1 ELSE 0 END) AS countReject
    FROM Table_ResultCounterBottles
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), FGsCode,
        CASE
            WHEN DATEPART(hh, DateTime) < 6 THEN 1
            WHEN DATEPART(hh, DateTime) < 14 THEN 2
            ELSE 3
        END
    ORDER BY date, shift;
    """
    cursor.execute(pipeline)
    group_data = cursor.fetchall()

    data_insert = []
    for row in group_data:
        new_row = {
            "date": row[0],
            "day": row [1],
            "month": row[2],
            "year": row[3],
            "shift": row[4],
            "sku":  row[5],
            "count": row[6],
            "countGood": row[7],
            "countNotGood": row[8],
        }
        #for key, value in new_row.items():
            #if key == "date":
                #new_row[key] = dt.strftime(value, "%Y-%m-%d")
            #else:
                #new_row[key] = value if value != float('nan') else ""
        data_insert.append(new_row)

    collection.insert_many(data_insert)
    connection.close()

def queryOptimizationResultCarton(table):
    """
    result carton
    """
    connection = connectToSqlServer('DESKTOP-M7H8BIL', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Po2", table)
    
    result = collection.delete_many({})
    startdate = datetime.datetime(2023, 1, 1, 0, 0, 0)
    query = f"""
    SELECT CONVERT(date, DateTime) AS date,
        DAY(DateTime) AS day,
        MONTH(DateTime) AS month,
        YEAR(DateTime) AS year,
        Shift AS shift,
		FGsCode AS sku,
        ProductName AS productName,
		Line As line,
        COUNT(*) AS count,
        SUM(CASE WHEN Status = 'Good' THEN 1 ELSE 0 END) AS countGood,
        SUM(CASE WHEN Status = 'Not Good' THEN 1 ELSE 0 END) AS countNotgood
    FROM Table_ResultCap
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), Line, FGsCode,
        CASE
            WHEN DATEPART(hh, DateTime) < 6 THEN 1
            WHEN DATEPART(hh, DateTime) < 14 THEN 2
            ELSE 3
        END
    ORDER BY date, shift;
    """
    cursor.execute(query)
    data = cursor.fetchall()
    data_insert = []
    for row in data:
        new_row = {
            "date": row[0],
            "day": row[1],
            "month": row[2],
            "year": row[3],
            "shift": row[4],
            "sku":  row[5],
            "line": row[6],
            "count": row[7],
            "countPass": row[8],
            "countNotgood": row[9],
        }
        data_insert.append(new_row)
    print(data_insert)

    collection.insert_many(data_insert)
    connection.close()

def queryOptimizationResultDataman(table):
    """
    result dataman
    """
    connection = connectToSqlServer('DESKTOP-M7H8BIL', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Po2", table)
    
    result = collection.delete_many({})
    startdate = datetime.datetime(2023, 1, 1, 0, 0, 0)
    query = f"""
    SELECT CONVERT(date, DateTime) AS date,
        DAY(DateTime) AS day,
        MONTH(DateTime) AS month,
        YEAR(DateTime) AS year,
        Shift AS shift,
		FGsCode AS sku,
        ProductName AS productName,
		Line As line,
        COUNT(*) AS count,
        SUM(CASE WHEN Status = 'Good' THEN 1 ELSE 0 END) AS countGood,
        SUM(CASE WHEN Status = 'Not Good' THEN 1 ELSE 0 END) AS countNotgood
    FROM Table_ResultCap
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), Line, FGsCode,
        CASE
            WHEN DATEPART(hh, DateTime) < 6 THEN 1
            WHEN DATEPART(hh, DateTime) < 14 THEN 2
            ELSE 3
        END
    ORDER BY date, shift;
    """
    cursor.execute(query)
    data = cursor.fetchall()
    data_insert = []
    for row in data:
        new_row = {
            "date": row[0],
            "day": row[1],
            "month": row[2],
            "year": row[3],
            "shift": row[4],
            "sku":  row[5],
            "line": row[6],
            "count": row[7],
            "countPass": row[8],
            "countNotgood": row[9],
        }
        data_insert.append(new_row)
    print(data_insert)

    collection.insert_many(data_insert)
    connection.close()
    
    

def querySqlServer():
    queryOptimizationCheckweigher("Table_Checkweigher")
    queryOptimizationImageFail("Table_ImageFail")
    queryOptimizationProduct("Table_Product")
    queryOptimizationResultCarton("Table_ResultCarton")
    queryOptimizationResultDataman("Table_ResultDataman")
