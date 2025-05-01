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

    try:
        # Autenticación y acceso a Google Drive
        credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/drive.readonly"])
        drive_service = build("drive", "v3", credentials=credentials)

        # Buscar el archivo en Drive
        results = drive_service.files().list(
            q="name='ventas_2025.csv' and '17CFr15ijnQOKPZy2PjnRxy66Ji3Vngbr' in parents",
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

        # Mapear columnas según los nombres reales (ajusta según tu archivo)
        column_mapping = {
            'Id_Venta': 'id',
            'Producto': 'producto',
            'Categoria': 'categoria',
            'Region': 'region',
            'Cliente': 'cliente',
            'Vendedor': 'vendedor',
            'Cantidad': 'cantidad',
            'Precio_Unitario': 'precio_unitario',
            'Fecha_Venta': 'fecha'
        }
        df = df.rename(columns=column_mapping)

        # Validar columnas esperadas
        expected_columns = ['id', 'producto', 'categoria', 'region', 'cliente', 'vendedor', 'cantidad', 'precio_unitario', 'fecha']
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            return Response(f"Error: Columnas faltantes en el archivo CSV: {missing_columns}", status=500)

        # Transformaciones de limpieza
        # 1. Eliminar duplicados basados en 'id'
        df = df.drop_duplicates(subset=['id'], keep='first')

        # 2. Validar y corregir valores
        df['precio_unitario'] = pd.to_numeric(df['precio_unitario'], errors='coerce')
        df['cantidad'] = pd.to_numeric(df['cantidad'], errors='coerce')
        df = df[df['precio_unitario'] > 0]  # Filtrar precios <= 0
        df = df[df['cantidad'] > 0]  # Filtrar cantidades <= 0
        df['producto'] = df['producto'].fillna('Desconocido')
        df['categoria'] = df['categoria'].fillna('Desconocido')
        df['region'] = df['region'].fillna('Desconocido')
        df['cliente'] = df['cliente'].fillna('Desconocido')
        df['vendedor'] = df['vendedor'].fillna('Desconocido')

        # 3. Estandarizar formatos
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')  # Convertir a datetime, ignorar errores
        df['producto'] = df['producto'].str.upper()
        df['categoria'] = df['categoria'].str.upper()
        df['region'] = df['region'].str.upper()
        df['cliente'] = df['cliente'].str.upper()
        df['vendedor'] = df['vendedor'].str.upper()

        # Escribir a Parquet
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob)
        df.to_parquet("/tmp/ventas_2025.parquet", index=False)
        blob.upload_from_filename("/tmp/ventas_2025.parquet")

        return Response("Procesado exitosamente", status=200)

    except Exception as e:
        return Response(f"Error interno: {str(e)}", status=500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
