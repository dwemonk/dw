from flask import Flask, Response
import pandas as pd
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import google.auth
import os

app = Flask(__name__)

@app.route("/", methods=["GET"])
def procesar_ventas():
    bucket_name = "data-dev-test-processed"
    source_file = "raw/ventas/actual/facturas/ventas_2025.csv"
    destination_blob = "ventas/facturas/actual/ventas_2025.parquet"

    # Autenticación y acceso a Google Drive
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/drive.readonly"])
    drive_service = build("drive", "v3", credentials=credentials)

    # Buscar el archivo en Drive
    results = drive_service.files().list(
        q="name='ventas_2025.csv' and 'FOLDER_ID' in parents",
        fields="files(id, name)"
    ).execute()
    files = results.get("files", [])
    if not files:
        return Response("Archivo no encontrado", status=404)

    # Descargar el archivo
    file_id = files[0]["id"]
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()

    # Leer el CSV con Pandas
    fh.seek(0)
    df = pd.read_csv(fh)

    # Transformaciones de limpieza
    # 1. Eliminar duplicados basados en 'id'
    df = df.drop_duplicates(subset=['id'], keep='first')

    # 2. Validar y corregir valores
    df = df[df['precio'] > 0]  # Filtrar precios <= 0
    df = df[df['cantidad'] > 0]  # Filtrar cantidades <= 0
    df['producto'] = df['producto'].fillna('Desconocido')  # Rellenar nulos

    # 3. Estandarizar formatos
    df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')  # Convertir a datetime
    df['producto'] = df['producto'].str.upper()  # Convertir a mayúsculas

    # Escribir a Parquet
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    df.to_parquet("/tmp/ventas_2025.parquet", index=False)
    blob.upload_from_filename("/tmp/ventas_2025.parquet")

    return Response("Procesado exitosamente", status=200)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
