import streamlit as st
import pandas as pd
import os
import json
import tempfile
import gc
from google.cloud import storage, bigquery

# --- KONFIGURASI HALAMAN ---
st.set_page_config(page_title="Dbase Uploader", page_icon="📦")
st.title("📦 DBASE UPLOADER")
st.caption("Mode: Qty, Value, Nett = Desimal (Float)")

# --- AUTHENTICATION ---
try:
    if "gcp_service_account" in st.secrets:
        service_account_info = st.secrets["gcp_service_account"]
        with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".json") as tfile:
            json.dump(dict(service_account_info), tfile)
            tfile.flush()
            KEY_PATH = tfile.name
    else:
        KEY_PATH = "gcp-key.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH
except Exception as e:
    st.error(f"❌ Masalah Kunci: {e}")
    st.stop()

BUCKET_NAME = "transaksi-upload" 
DATASET_ID = "pma"
TABLE_ID = "berjalan"

# --- MAPPING ---
EXCEL_TO_BQ_MAP = {
    "TGL": "tgl", "NO FAKTUR": "no_faktur", "KODE OUTLET": "kode_outlet",
    "NAMA OUTLET": "nama_outlet", "CHANNEL": "channel", "FC": "fc",
    "RUTE": "rute", "PMA": "pma", "KODE SALESMAN": "kode_salesman",
    "KD_BRG": "kd_brg", "NM_BRG": "nm_brg", "BU": "bu", "MARK": "mark",
    "KODE BARANG": "kode_barang", "DESCRIPTION": "description", "QTY": "qty",
    "VALUE": "value", "VALUE NETT": "value_nett", "BLN": "bln",
    "KD SLS2": "kd_sls2", "DIV": "div"
}

# --- SCHEMA DEFINITION (ALL FLOAT) ---
BQ_SCHEMA = [
    bigquery.SchemaField("tgl", "DATE"),
    bigquery.SchemaField("no_faktur", "STRING"),
    bigquery.SchemaField("kode_outlet", "STRING"),
    bigquery.SchemaField("nama_outlet", "STRING"),
    bigquery.SchemaField("channel", "STRING"),
    bigquery.SchemaField("fc", "STRING"),
    bigquery.SchemaField("rute", "STRING"),
    bigquery.SchemaField("pma", "STRING"),
    bigquery.SchemaField("kode_salesman", "STRING"),
    bigquery.SchemaField("kd_brg", "STRING"),
    bigquery.SchemaField("nm_brg", "STRING"),
    bigquery.SchemaField("bu", "STRING"),
    bigquery.SchemaField("mark", "STRING"),
    bigquery.SchemaField("kode_barang", "STRING"),
    bigquery.SchemaField("description", "STRING"),
    
    # KEMBALI KE FLOAT SEMUA (SESUAI REQUEST)
    bigquery.SchemaField("qty", "FLOAT"), 
    bigquery.SchemaField("value", "FLOAT"), 
    bigquery.SchemaField("value_nett", "FLOAT"),
    
    bigquery.SchemaField("bln", "STRING"),
    bigquery.SchemaField("kd_sls2", "STRING"),
    bigquery.SchemaField("div", "STRING"),
]

st.info("💡 Semua kolom angka (Qty, Value, Nett) akan disimpan sebagai FLOAT.")
uploaded_files = st.file_uploader("Upload File", type=['xlsx', 'xls'], accept_multiple_files=True)

if uploaded_files:
    if st.button("MULAI PROSES"):
        progress_bar = st.progress(0)
        status = st.empty()
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        
        # Bersihkan Bucket
        blobs = bucket.list_blobs(prefix="upload/")
        for blob in blobs: blob.delete()
        
        success_count = 0
        total_files = len(uploaded_files)

        for i, file in enumerate(uploaded_files):
            try:
                status.text(f"⏳ Processing {i+1}/{total_files}: {file.name}")
                
                # Baca Excel
                df = pd.read_excel(file, dtype=object, engine='openpyxl')
                
                # Mapping Header
                df.columns = df.columns.str.strip().str.upper()
                df.rename(columns=EXCEL_TO_BQ_MAP, inplace=True)
                
                # Filter Kolom
                valid_cols = [f.name for f in BQ_SCHEMA if f.name in df.columns]
                df = df[valid_cols]

                # --- CLEANING & FORMATTING (ALL FLOAT) ---
                
                # 1. DATE
                if 'tgl' in df.columns:
                    df['tgl'] = pd.to_datetime(df['tgl'], errors='coerce').dt.date

                # 2. NUMERIC (FORCE FLOAT SEMUA)
                # Qty, Value, Value Nett dipaksa jadi Desimal (100.0)
                numeric_cols = ['qty', 'value', 'value_nett']
                for nc in numeric_cols:
                    if nc in df.columns:
                        # Ubah ke numeric (handle string '1,000')
                        df[nc] = pd.to_numeric(df[nc], errors='coerce')
                        # Isi kosong dengan 0.0
                        df[nc] = df[nc].fillna(0.0)
                        # Paksa jadi FLOAT (Desimal)
                        df[nc] = df[nc].astype(float) 

                # 3. STRING (SISANYA)
                non_string = ['tgl'] + numeric_cols
                str_cols = [c for c in df.columns if c not in non_string]
                
                for sc in str_cols:
                    # Bersihkan .0 di belakang string text (misal BLN "10.0" -> "10")
                    df[sc] = df[sc].astype(str).str.strip().replace('nan', '').str.replace(r'\.0$', '', regex=True)
                    if sc in ['pma', 'channel', 'kode_outlet', 'no_faktur', 'fc']: 
                        df[sc] = df[sc].str.upper()

                # Save Parquet
                temp_filename = f"part_{i}.parquet"
                df.to_parquet(temp_filename, index=False)
                
                blob = bucket.blob(f"upload/{temp_filename}")
                blob.upload_from_filename(temp_filename)
                
                os.remove(temp_filename)
                del df
                gc.collect()
                
                success_count += 1
                progress_bar.progress(int((i+1) / total_files * 80))
                
            except Exception as e:
                st.error(f"Gagal {file.name}: {e}")

        if success_count > 0:
            status.text("Upload ke BigQuery...")
            bq_client = bigquery.Client()
            
            # --- RESET TABEL (PENTING) ---
            # Hapus tabel lama agar schema berubah dari INTEGER kembali ke FLOAT
            try:
                table_ref = f"{DATASET_ID}.{TABLE_ID}"
                bq_client.delete_table(table_ref, not_found_ok=True)
                status.text("Tabel lama di-reset untuk schema FLOAT...")
            except Exception:
                pass

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_TRUNCATE",
                schema=BQ_SCHEMA, # Schema sudah FLOAT semua
                autodetect=False 
            )
            
            load_job = bq_client.load_table_from_uri(
                f"gs://{BUCKET_NAME}/upload/*.parquet", 
                f"{DATASET_ID}.{TABLE_ID}", 
                job_config=job_config
            )
            
            try:
                load_job.result()
                progress_bar.progress(100)
                st.success("✅ SUKSES! Data Masuk (Format FLOAT).")
                st.balloons()
            except Exception as e:
                st.error("BigQuery Error Details:")
                if hasattr(e, 'errors'): st.json(e.errors)
                else: st.write(e)
            
            # Cleanup
            blobs = bucket.list_blobs(prefix="upload/")
            for blob in blobs: blob.delete()
