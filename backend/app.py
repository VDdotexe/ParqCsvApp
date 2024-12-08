from flask import Flask, request, send_file, render_template
import pandas as pd
import os

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return "No file part"
    file = request.files["file"]
    if file.filename == "":
        return "No selected file"
    file_path = os.path.join(app.config["UPLOAD_FOLDER"], file.filename)
    file.save(file_path)

    # Convert Parquet to CSV
    df = pd.read_parquet(file_path)
    csv_path = file_path.replace(".parquet", ".csv")
    df.to_csv(csv_path, index=False)

    return send_file(csv_path, as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True)
