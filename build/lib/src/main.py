import decimal
import json
import pyodbc
from flask import Flask, request, jsonify, url_for
import pandas as pd
from convertGateVepari import generateGatePassVepari
from convertGateBook import generateGateBook
import os
from datetime import datetime, date
from itertools import groupby
from dotenv import dotenv_values
import math
import schedule
import decimal
from flask_cors import CORS
import threading
import time

app = Flask(__name__)
CORS(app)

env_vars = dotenv_values('.env')

# Define the database connection details
server = env_vars['server']
database = env_vars['database']
username = env_vars['username']
password = env_vars['password']

# Create the database connection
conn = pyodbc.connect(
    'DRIVER={SQL Server};'  # Specify the ODBC driver name here
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password};'
)


# Custom JSON encoder class to handle datetime serialization
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()  # Convert datetime to ISO format string
        return super().default(obj)
    
# Custom JSON encoder to handle Decimal values
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)  # Convert Decimal to string representation
        return super().default(o)

def delete_files():
    folder_path = r'D:\myProject\api-python-pdf\static\reports\pdf\gateBook'  # Replace with the actual path to the folder containing the files

    # Iterate over the files in the folder and delete them
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"Deleted file: {filename}")

    folder_path = r'D:\myProject\api-python-pdf\static\reports\pdf\gatePass-vepari'  # Replace with the actual path to the folder containing the files

    # Iterate over the files in the folder and delete them
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"Deleted file: {filename}")

# Schedule the file deletion task to run at 12:00 AM every day
def schedule_file_deletion():
    schedule.every().day.at('00:00').do(delete_files)


    while True:
        schedule.run_pending()
        time.sleep(1)

# Start the file deletion task in a background thread
file_deletion_thread = threading.Thread(target=schedule_file_deletion)
file_deletion_thread.start()

env_vars = dotenv_values('.env')

# Define the database connection details
server = env_vars['server']
database = env_vars['database']
username = env_vars['username']
password = env_vars['password']

# Create the database connection
conn = pyodbc.connect(
    'DRIVER={SQL Server};'  # Specify the ODBC driver name here
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password};'
)


# Custom JSON encoder class to handle datetime serialization
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()  # Convert datetime to ISO format string
        return super().default(obj)
    
# Custom JSON encoder to handle Decimal values
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)  # Convert Decimal to string representation
        return super().default(o)

@app.route('/api/createPdf', methods=['POST'])
def handle_endpoint():
    # Access the request data using the `request` object
    data = request.json  # Assuming the request data is in JSON format

    # Extract the parameters from the request data
    params = data.get('params')
    stored_procedure_number = data.get('sp')
    webClientId = data.get('webClientId')

    
    Fromdate = params.get('Fromdate')
    date_obj1 = datetime.strptime(Fromdate, "%d-%b-%Y")
    FromdateNew = date_obj1.strftime("%d/%m/%Y")
    Todate = params.get('Todate')
    date_obj = datetime.strptime(Todate, "%d-%b-%Y")
    TodateNew = date_obj.strftime("%d/%m/%Y")

    # Construct the stored procedure query with the parameters
    if(stored_procedure_number == 1):
        # Extract the individual parameters from the 'params' dictionary
        Mcode = params.get('Mcode')
        Stallno1 = params.get('Stallno1')
        Stallno2 = params.get('Stallno2')
        StallTypeId = params.get('StallTypeId')
        sp_query = f"EXEC GateRegisterNew @Mcode='{Mcode}', @Stallno1={Stallno1}, @Stallno2={Stallno2}, @StallTypeId={StallTypeId}, @Fromdate='{Fromdate}', @Todate='{Todate}'"

        print(sp_query)
        # Create a cursor to execute SQL queries
        cursor = conn.cursor()

        try:
            # Execute the stored procedure query
            cursor.execute(sp_query)
            # Stored procedure executed successfully
            results = cursor.fetchall()
            # Convert the query results to JSON

            json_results = []
            for row in results:
                result_dict = {}
                for i, column in enumerate(cursor.description):
                    result_dict[column[0]] = row[i] if column[0] != 'Occupier' else row[i].strip()
                json_results.append(result_dict)

            # Group the results based on 'stalltype', 'StallNo', 'Occupier', and 'StallAL'
            df = pd.DataFrame(json_results)
            grouped_data = df.groupby(['stalltype', 'StallNo', 'Occupier', 'StallAL']).apply(lambda x: x.to_dict('records')).to_dict()

            # Convert tuple keys to strings in grouped_data and adjust the dictionary structure
            formatted_data = []
            market_fee_total = 0
            total = 0
            for key, value in grouped_data.items():
                stalltype, StallNo, Occupier, StallAL = key
                stalltype = stalltype.strip()
                if int(StallAL) > 0:
                    StallAL = "/ " + str(StallAL)
                    stall_data = {
                        'stalltype': stalltype,
                        'StallNo': StallNo,
                        'Occupier': Occupier,
                        'StallAL': StallAL,
                        'obj': value
                    }
                else:
                    stall_data = {
                        'stalltype': stalltype,
                        'StallNo': StallNo,
                        'Occupier': Occupier,
                        'obj': value
                    }

                # Remove the ".0" from GI_ESTWGHT in each obj
                for obj in value:
                    obj['GI_ESTWGHT'] = str(int(obj['GI_ESTWGHT']))

                # Calculate the total of GI_MARKETAMT and GI_ESTWGHT
                market_amt_total = sum(float(obj['GI_MARKETAMT']) for obj in value)
                est_weight_total = sum(float(obj['GI_ESTWGHT']) for obj in value)

                # Format market_amt_total, est_weight_total, and total_anaj
                market_amt_total_str = "{:.2f}".format(market_amt_total)
                est_weight_total_str = "{:.2f}".format(est_weight_total)

                stall_data['marketFeeTotal'] = market_amt_total_str
                stall_data['total'] = est_weight_total_str

                market_fee_total += market_amt_total
                total += est_weight_total

                formatted_data.append(stall_data)

            # Calculate the total of all stalls
            total_anaj = sum(float(data['total']) for data in formatted_data)
            total_anaj_str = "{:.2f}".format(total_anaj)

            # Add totalAnaj, FromDate, and ToDate to the response data
            response_data = [{
                'obj': formatted_data,
                'totalAnaj': total_anaj_str,
                'FromDate': FromdateNew,
                'ToDate': TodateNew
            }]

            # Create a JSON file
            with open('formatted_data_gatevepari.json', 'w') as file:
                json.dump(response_data, file, cls=DecimalEncoder, indent=2)

        except Exception as e:
            print(f"Error executing stored procedure: {e}")

        print("Query Executed")
        
        # Get the URL of the generated PDF file
        pdf_filename =  generateGatePassVepari()
        pdf_url = url_for('static', filename=f'reports/pdf/gatePass-vepari/{pdf_filename}', _external=True)

    # For GateBook
    if(stored_procedure_number == 2):
        sp_query = f"EXEC Report_GateBookNew @Fromdate='{Fromdate}', @Todate='{Todate}'"
        
        print(sp_query)
        # Create a cursor to execute SQL queries
        cursor = conn.cursor()


        try:
            # Execute the stored procedure query
            cursor.execute(sp_query)

            # Stored procedure executed successfully
            results = cursor.fetchall()

            # Count the total number of unique GP_GPASSNO
            unique_gpassno = set(row[0] for row in results)

            json_results = []
            for row in results:
                result_dict = {}
                for i, column in enumerate(cursor.description):
                    result_dict[column[0]] = row[i]
                json_results.append(result_dict)

            # Sort the JSON data as per GP_GPASSNO
            sorted_results = sorted(json_results, key=lambda x: x['GP_GPASSNO'])

            # Group the results based on 'stalltype', 'StallNo', and 'Occupier'
            df = pd.DataFrame(sorted_results)
            df['GP_GPDATE'] = pd.to_datetime(df['GP_GPDATE']).dt.strftime('%d-%b-%Y')  # Convert GP_GPDATE to string format
            grouped_data = df.groupby('GP_GPDATE').apply(lambda x: x.to_dict('records')).to_dict()

            # Convert tuple keys to strings in grouped_data and adjust the dictionary structure
            formatted_data = []
            market_fee_total = 0
            total_net_quantity = 0

            for key, value in grouped_data.items():
                for obj in value:
                    obj['NETWEIGHT'] = int(obj['NETWEIGHT'])
                GP_GPDATE = key

                # Calculate the total NETWEIGHT, GI_MARKETAMT, and GI_Auctionamt for each GP_GPDATE
                net_weight_total = sum(record['NETWEIGHT'] for record in value)
                market_fee_total = sum(record['GI_MARKETAMT'] for record in value)
                net_amount_total = sum(record['GI_Auctionamt'] for record in value)

                # Calculate the GrandTotalNetQuantity
                net_quantity_total = math.floor(net_weight_total / 100)

                stall_data = {
                    'GP_GPDATE': GP_GPDATE,
                    'obj': value,
                }

                formatted_data.append(stall_data)

            # Add totalAnaj, FromDate, and ToDate to the response data
            response_data = [{
                'obj': formatted_data,
                'FromDate': FromdateNew,
                'ToDate': TodateNew,
                'GrandTotalNetWeight': net_weight_total,
                'GrandTotalMarketFee': market_fee_total,
                'GrandTotalNetAmount': net_amount_total,
                'GrandTotalNetQuantity': net_quantity_total,
                'TotalGatePass': len(unique_gpassno),  # Add the total unique count to the response data
            }]
            # Create a JSON file
            with open('formatted_data_gatebook.json', 'w') as file:
                json.dump(response_data, file, cls=DecimalEncoder, indent=2)

        except Exception as e:
            print(f"Error executing stored procedure: {e}")
                    
        print("Query Executed")
        
        pdf_filename = generateGateBook()

        # Get the URL of the generated PDF file
        pdf_url = url_for('static', filename=f'reports/pdf/gateBook/{pdf_filename}', _external=True)

    return jsonify({
        'status': 200,
        'message': 'PDF created successfully',
        'pdf_file_url': pdf_url,
        'webClientId': webClientId
    })

@app.route('/', methods=['GET'])

def index():
    return "HELLO TO APMC PYTHON BACKEND"


app.run()


