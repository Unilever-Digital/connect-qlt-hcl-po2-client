import datetime
import time
from datetime import datetime as dt
from app.controls.control import *

def optimizationQueryCheckweigher(table):
    """
    counter bottles server
    """
    connection = connectToSqlServer('DESKTOP-M7H8BIL', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Po2",table)
    
    result = collection.delete_many({})
    startdate = datetime.datetime(2023, 1, 1, 0, 0, 0)
    query = f"""
    SELECT 
        CONVERT(date, Datetime) AS date,
        DAY(Datetime) AS day, 
        MONTH(Datetime) AS month,
        YEAR(Datetime) AS year,
        Shift AS shift,
        FgsCode AS sku,
        Line AS line,
        Weight_Under AS under,
        Weight_Target AS target,
        Weight_Over AS overweight,
        COUNT(*) AS count,
        SUM(CASE WHEN Status = 'Under' THEN 1 ELSE 0 END) AS under_count,  -- Renamed for clarity
        SUM(CASE WHEN Status = 'Accept' THEN 1 ELSE 0 END) AS accept_count, -- Renamed for clarity
	    CASE
            WHEN DATEPART(hour, Datetime) < 6 THEN 1  -- Use DATEPART(hour) instead of hh
            WHEN DATEPART(hour, Datetime) < 14 THEN 2
		    ELSE 3
	    END AS shift_category
	FROM {table}
	GROUP BY CONVERT(date, Datetime), DAY(Datetime), MONTH(Datetime), YEAR(Datetime), Line, FGsCode, Shift, Weight_Under, Weight_Target, Weight_Over,
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
            "under": row[7],
            "target": row[8],
            "over": row[9],
            "count": row[10],
            "under": row[11],
            "accept": row[12],
        }
        data_insert.append(new_row)
    collection.insert_many(data_insert)
    connection.close()

def optimizationQueryImageFail(table):
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
		FGsCode AS sku,
		Line As line,
        Type AS type,
        BarcodeTarget AS target,
        BarcodeCurrent AS current_,
        COUNT(*) AS count
    FROM {table}
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), Line, Type, FGsCode, BarcodeTarget, BarcodeCurrent,
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
            "type": row[7],
            "target": row[8],
            "current": row[9],
            "count": row[10]
        }
        data_insert.append(new_row)

    collection.insert_many(data_insert)
    connection.close()

def optimizationQueryProduct(table):
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
        SKUID AS sku,
        COUNT(*) AS count,
        SUM(CASE WHEN Messenger = 'Export' THEN 1 ELSE 0 END) AS Export,
        SUM(CASE WHEN Messenger = 'Local' THEN 1 ELSE 0 END) AS Local,
    FROM {table}
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), SKUID,
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
        data_insert.append(new_row)

    collection.insert_many(data_insert)
    connection.close()

def optimizationQueryResultCarton(table):
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
		FgsCode AS sku,
        ProductName AS productName,
		Line AS line,
        COUNT(*) AS count,
        SUM(CASE WHEN Status = 'Good' THEN 1 ELSE 0 END) AS good,
        SUM(CASE WHEN Status = 'WrongCode' THEN 1 ELSE 0 END) AS wrongcode,
		SUM(CASE WHEN Status = 'No Read' THEN 1 ELSE 0 END) AS no_read
    FROM Table_ResultCarton
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), Line, FgsCode, ProductName, Shift,
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
            "name": row[6],
            "line": row[7],
            "count": row[8],
            "countGood": row[9],
            "countNotgood": row[10],
        }
        data_insert.append(new_row)
    print(data_insert)

    collection.insert_many(data_insert)
    connection.close()

def optimizationQueryResultDataman(table):
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
        SUM(CASE WHEN Status = 'Not Good' THEN 1 ELSE 0 END) AS countNotgood,
        SUM(CASE WHEN Status = 'No Read' THEN 1 ELSE 0 END) AS no_read
    FROM {table}
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), Line, FGsCode, ProductName, Shift,
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
            "name": row[6],
            "line": row[7],
            "count": row[8],
            "countPass": row[9],
            "countNotgood": row[10],
        }
        data_insert.append(new_row)

    collection.insert_many(data_insert)
    connection.close()
    
def querySqlServer():
    try:
        optimizationQueryCheckweigher("Table_Checkweigher") ## done
        optimizationQueryImageFail("Table_ImageFail") ## done
        #optimizationQueryProduct("Table_Product")
        optimizationQueryResultCarton("Table_ResultCarton") ## done
        optimizationQueryResultDataman("Table_ResultDataman")
    except Exception as e:
        print(e)

