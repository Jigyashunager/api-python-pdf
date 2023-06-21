import json
import math
import os
import time
from io import BytesIO
from datetime import datetime

import pdfkit
from jinja2 import Environment, FileSystemLoader
from PyPDF2 import PdfMerger, PdfReader


def generateGateBook():

    print('Running PDF generator')
    current_directory = os.getcwd()
    output_pdf_directory = os.path.join(current_directory, 'static', 'reports', 'pdf', 'gateBook')
    json_file_path = os.path.join(current_directory, 'formatted_data_gatebook.json')
    template_directory = os.path.join(current_directory, 'gatebook', 'templates')
    single_json_file_path = os.path.join(current_directory, 'json-files', 'single-data_gatebook.json')



    wkhtmltopdf_path = os.path.join(current_directory, 'wkhtmltopdf', 'bin', 'wkhtmltopdf.exe')
 
    # Import JSON data from file

    # Load JSON data into a Python object
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    env = Environment(loader=FileSystemLoader(template_directory))
    total_pages = 0

    # Create separate HTML files for each object
    start_time = time.time()
    # print(f'PDF file creation starts: {start_time // 60} minutes')

    pdf_files = []  # List to store PDF file paths
    now = datetime.today()
    formatted_date = now.strftime("%d/%m/%Y, %I:%M %p")

    single_data = {
        'FromDate': '',
        'ToDate': '',
        'GrandTotalNetWeight': 0,
        'GrandTotalMarketFee': 0,
        'GrandTotalNetAmount': 0,
        'GrandTotalNetQuantity': 0,
        'TotalGatePass': 0
    }

    for item in data:
        obj_list = item['obj']
        obj_count = len(obj_list)
        # print(f"Total Objects : {obj_count}")

        single_data['FromDate'] = item['FromDate']
        single_data['ToDate'] = item['ToDate']
        single_data['GrandTotalNetWeight'] = float(item['GrandTotalNetWeight'])
        single_data['GrandTotalMarketFee'] = float(item['GrandTotalMarketFee'])
        single_data['GrandTotalNetAmount'] = float(item['GrandTotalNetAmount'])
        single_data['GrandTotalNetQuantity'] = item['GrandTotalNetQuantity']
        single_data['TotalGatePass'] = item['TotalGatePass']

        template_name = 'gatebookData.html' 

        template = env.get_template(template_name)

        # Render the template with obj and single_json_data
        html_output = template.render(obj=obj_list, single_json=single_data)

        # Configure options for PDF generation (including orientation)
        options = {
            'encoding': 'UTF-8',
            'margin-top': '10px',
            'margin-right': '40px',
            'margin-bottom': '30px',
            'margin-left': '40px',
            'footer-left': formatted_date,
            'footer-font-size': "9",
            'orientation': 'Landscape',
            'page-size': 'A4',
            'footer-right': "Page [page] of [topage]",
        }

        print(f'Pdf pages created: {total_pages}')

        # Convert HTML to PDF as byte array with specified options
        pdf_data = pdfkit.from_string(html_output, False, options=options)

        # Get the number of pages in the generated PDF
        reader = PdfReader(BytesIO(pdf_data))
        num_pages = len(reader.pages)

        # Append PDF data to the list
        pdf_files.append(BytesIO(pdf_data))
        total_pages += num_pages

    print(f'Total pages created: {total_pages}')

    # Merge PDF files
    merger = PdfMerger()
    for pdf_data in pdf_files:
        merger.append(pdf_data)

    pdf_filename = f'gatebook_{int(time.time())}.pdf'
    merged_pdf_file = os.path.join(output_pdf_directory, pdf_filename)
    merger.write(merged_pdf_file)

    print(f'Merged PDF file with page numbers created: {(time.time() - start_time) // 60} minutes')
    time.sleep(5)

    # Remove all output files
    for file in pdf_files:
        file.close()

    return pdf_filename