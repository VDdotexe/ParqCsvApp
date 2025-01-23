from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import tempfile
import zipfile
from io import BytesIO

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your_secret_key'

# Store temporary files in a global dictionary
temp_files = {}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    if 'files[]' not in request.files:
        flash('No file part')
        return redirect(request.url)
    files = request.files.getlist('files[]')
    uploaded_files = []
    for file in files:
        if file:
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1])
            file.save(temp_file.name)
            uploaded_files.append((temp_file.name, file.filename))
    temp_files['uploaded'] = uploaded_files
    flash('Files successfully uploaded')
    return redirect(url_for('index'))

@app.route('/convert_csv_to_parquet', methods=['POST'])
def convert_csv_to_parquet():
    if 'uploaded' not in temp_files:
        flash('No files uploaded for conversion')
        return redirect(url_for('index'))

    converted_files = []
    for temp_file, original_filename in temp_files['uploaded']:
        if temp_file.endswith('.csv'):
            df = pd.read_csv(temp_file)
            table = pa.Table.from_pandas(df)
            parquet_file_path = temp_file.replace('.csv', '.parquet')
            pq.write_table(table, parquet_file_path)
            converted_files.append((parquet_file_path, original_filename.replace('.csv', '.parquet')))
    temp_files['converted'] = converted_files
    flash('CSV files successfully converted to Parquet')
    return redirect(url_for('download_converted_files'))

@app.route('/convert_parquet_to_csv', methods=['POST'])
def convert_parquet_to_csv():
    if 'uploaded' not in temp_files:
        flash('No files uploaded for conversion')
        return redirect(url_for('index'))

    converted_files = []
    for temp_file, original_filename in temp_files['uploaded']:
        if temp_file.endswith('.parquet'):
            table = pq.read_table(temp_file)
            df = table.to_pandas()
            csv_file_path = temp_file.replace('.parquet', '.csv')
            df.to_csv(csv_file_path, index=False)
            converted_files.append((csv_file_path, original_filename.replace('.parquet', '.csv')))
    temp_files['converted'] = converted_files
    flash('Parquet files successfully converted to CSV')
    return redirect(url_for('download_converted_files'))

@app.route('/download_converted_files')
def download_converted_files():
    if 'converted' not in temp_files:
        flash('No files converted')
        return redirect(url_for('index'))

    # Create a ZIP file containing all converted files
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        for converted_file, original_filename in temp_files['converted']:
            zip_file.write(converted_file, original_filename)
    zip_buffer.seek(0)

    # Clear the converted files after creating the ZIP
    del temp_files['converted']

    return send_file(zip_buffer, as_attachment=True, download_name='converted_files.zip')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6007, debug=True)