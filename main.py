from flask import Flask, Response
import pandas as pd
from google.cloud import storage, bigquery
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import google.auth
import os

app = Flask(__name__)

@app.route("/", methods=["GET"])
def procesar_ventas():
    project_id = "data-dev-test"
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
        df['precio_unitario'] = pd.to_numeric(df['precio_unitario'], errors='coerce').astype('float64')  # Forzar float64
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

        # Cargar a BigQuery
        bq_client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.ventas_landing.raw_facturas_2025"
        schema = [
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("producto", "STRING"),
            bigquery.SchemaField("categoria", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("cliente", "STRING"),
            bigquery.SchemaField("vendedor", "STRING"),
            bigquery.SchemaField("cantidad", "INTEGER"),
            bigquery.SchemaField("precio_unitario", "FLOAT"),
            bigquery.SchemaField("fecha", "DATETIME"),
        ]
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=False
        )
        uri = f"gs://{bucket_name}/{destination_blob}"
        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()  # Espera a que la carga se complete

        # Crear tabla transformada fact_ventas_2025
        query = """
        CREATE OR REPLACE TABLE `ventas_analytics.fact_ventas_2025`
        PARTITION BY DATE(fecha)
        CLUSTER BY region
        AS
        SELECT
            id,
            producto,
            categoria,
            region,
            cliente,
            vendedor,
            cantidad,
            precio_unitario,
            fecha,
            (precio_unitario * cantidad) AS total,
            EXTRACT(MONTH FROM fecha) AS mes,
            CASE
                WHEN (precio_unitario * cantidad) > 1000 THEN 'Alta'
                ELSE 'Baja'
            END AS tipo_venta
        FROM `ventas_landing.raw_facturas_2025`;
        """
        query_job = bq_client.query(query)
        query_job.result()  # Espera a que la query se complete

        return Response("Procesado exitosamente y cargado a BigQuery", status=200)

    except Exception as e:
        return Response(f"Error interno: {str(e)}", status=500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
