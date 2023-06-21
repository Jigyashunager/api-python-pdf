import json
import math
import os
import time
from io import BytesIO
from datetime import datetime

import pdfkit
from jinja2 import Environment, FileSystemLoader
from PyPDF2 import PdfMerger, PdfReader

def generateGatePassVepari():

    # Specify the path
    current_directory = os.getcwd()
    output_pdf_directory = os.path.join(current_directory, 'static', 'reports', 'pdf', 'gatePass-vepari')
    json_file_path = os.path.join(current_directory, 'formatted_data_gatevepari.json')
    single_json_file_path = os.path.join(current_directory, 'json-files', 'single-data_gatevepari.json')
    template_directory = os.path.join(current_directory, 'gatevepari', 'templates')

    print('Running PDF generator')
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

    single_json_data = {
        'FromDate': '',
        'ToDate': '',
        'totalAnaj': ''
    }
    
    for item in data:
        obj_list = item['obj']
        obj_count = len(obj_list)
        # print(f"Total Objects : {obj_count}")

        single_json_data = {
            'totalAnaj': item['totalAnaj'],
            'FromDate': item['FromDate'],
            'ToDate': item['ToDate']
        }

        for i, obj in enumerate(obj_list, start=1):
            template_name = 'gate-vepari.html' if i == 1 else 'gate-vepari-table.html'
            if i == obj_count:
                template_name = 'gate-vepari-footer.html'

            template = env.get_template(template_name)

            # Render the template with obj and single_json_data
            html_output = template.render(obj=obj, single_json=single_json_data)

            # Calculate the total number of lines in the HTML content
            total_lines = len(html_output.split('\n'))
            # print(f'Total lines: {total_lines}')

            # Define the number of lines per chunk
            lines_per_chunk = total_lines  # Adjust this value as per your requirement

            # Calculate the number of chunks based on the lines per chunk
            total_chunks = math.ceil(total_lines / lines_per_chunk)
            # print(f'Total chunks: {total_chunks}')

            for j in range(total_chunks):
                # Calculate the starting and ending line numbers for the current chunk
                start_line = j * lines_per_chunk + 1
                end_line = min((j + 1) * lines_per_chunk, total_lines)

                # Extract the chunk from the HTML content
                chunk = html_output.split('\n')[start_line - 1:end_line]
                chunk_html = '\n'.join(chunk)

                # Configure options for PDF generation (including orientation)
                if i == 1:
                    options = {
                        'encoding': 'UTF-8',
                        'margin-top': '10px',
                        'margin-right': '40px',
                        'margin-bottom': '30px',
                        'margin-left': '40px',
                        'footer-left': formatted_date,
                        'footer-font-size': "9",
                        'orientation': 'Portrait',
                        'page-size': 'A4',
                        'footer-right': f'Page no. [page]',

                    }
                else:
                    options = {
                        'encoding': 'UTF-8',
                        'margin-top': '10px',
                        'margin-right': '40px',
                        'margin-bottom': '30px',
                        'margin-left': '40px',
                        'footer-left': formatted_date,
                        'footer-font-size': "9",
                        'orientation': 'Portrait',
                        'page-size': 'A4',
                        'footer-right': f'Page no. [page]',
                        'page-offset': total_pages  # Set the starting page number (10 - 1)

                    }
                    print(f'Pdf pages created: {total_pages}')

                # Convert HTML chunk to PDF as byte array with specified options
                pdf_data = pdfkit.from_string(chunk_html, False, options=options)

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

    pdf_filename = f'gatevepari_{int(time.time())}..pdf'
    merged_pdf_file = os.path.join(output_pdf_directory, pdf_filename)
    merger.write(merged_pdf_file)

    print(f'Merged PDF file with page numbers created: {(time.time() - start_time) // 60} minutes')
    time.sleep(5)

    # Remove all output files
    for file in pdf_files:
        file.close()

    return pdf_filename