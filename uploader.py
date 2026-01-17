import streamlit as st
import pandas as pd
import os
import json
import tempfile
import gc # Garbage Collector (Tukang Sampah Memori)
from google.cloud import storage, bigquery

# --- KONFIGURASI HALAMAN ---
st.set_page_config(page_title="Dbase Uploader", page_icon="📦")
st.title("📦 DBASE UPLOADER ")
st.caption("Mode: Estafet (Hemat RAM) + Schema Permanen")

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

# --- KONFIGURASI ---
BUCKET_NAME = "transaksi-upload" 
DATASET_ID = "pma"
TABLE_ID = "berjalan"

# --- MAPPING & SCHEMA ---
EXCEL_TO_BQ_MAP = {
    "TGL": "tgl", "NO FAKTUR": "no_faktur", "KODE OUTLET": "kode_outlet",
    "NAMA OUTLET": "nama_outlet", "CHANNEL": "channel", "FC": "fc",
    "RUTE": "rute", "PMA": "pma", "KODE SALESMAN": "kode_salesman",
    "KD_BRG": "kd_brg", "NM_BRG": "nm_brg", "BU": "bu", "MARK": "mark",
    "KODE BARANG": "kode_barang", "DESCRIPTION": "description", "QTY": "qty",
    "VALUE": "value", "VALUE NETT": "value_nett", "BLN": "bln",
    "KD SLS2": "kd_sls2", "DIV": "div"
}

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
    bigquery.SchemaField("qty", "FLOAT"), 
    bigquery.SchemaField("value", "FLOAT"), 
    bigquery.SchemaField("value_nett", "FLOAT"),
    bigquery.SchemaField("bln", "STRING"),
    bigquery.SchemaField("kd_sls2", "STRING"),
    bigquery.SchemaField("div", "STRING"),
]

# --- UI UPLOAD ---
st.info("💡 Tips: Aman untuk upload puluhan file sekaligus.")
uploaded_files = st.file_uploader("Upload File Transaksi", type=['xlsx', 'xls'], accept_multiple_files=True)

if uploaded_files:
    st.write(f"📂 Mendeteksi {len(uploaded_files)} file.")
    
    if st.button("MULAI PROSES ESTAFET 🚀"):
        
        # 1. PERSIAPAN
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        
        # Bersihkan Bucket Dulu
        status_text.text("Membersihkan sisa file lama di Bucket...")
        blobs = bucket.list_blobs(prefix="upload/")
        for blob in blobs: blob.delete()
        
        # 2. LOOPING ESTAFET (BACA -> BERSIHKAN -> UPLOAD -> LUPAKAN)
        success_count = 0
        total_files = len(uploaded_files)

        for i, file in enumerate(uploaded_files):
            try:
                status_text.text(f"Sedang memproses file {i+1} dari {total_files}: {file.name}")
                
                # A. Baca File
                df = pd.read_excel(file, dtype=object)
                
                # B. Cleaning & Mapping
                df.columns = df.columns.str.strip().str.upper()
                df.rename(columns=EXCEL_TO_BQ_MAP, inplace=True)
                
                # Filter Kolom Valid
                valid_cols = [field.name for field in BQ_SCHEMA if field.name in df.columns]
                df = df[valid_cols]

                # Format Data
                for col in df.columns:
                    if col == 'tgl':
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                    elif col in ['qty', 'value', 'value_nett']:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    else:
                        df[col] = df[col].astype(str).str.strip().replace('nan', '').str.replace(r'\.0$', '', regex=True)
                        if col in ['pma', 'channel']:
                            df[col] = df[col].str.upper()

                # C. Save Partial Parquet
                temp_filename = f"part_{i}.parquet"
                df.to_parquet(temp_filename, index=False)
                
                # D. Upload Partial ke Bucket
                blob = bucket.blob(f"upload/{temp_filename}")
                blob.upload_from_filename(temp_filename)
                
                # E. Hapus Jejak Memori (PENTING!)
                os.remove(temp_filename) # Hapus file fisik di laptop/server
                del df                   # Hapus variabel dataframe
                gc.collect()             # Panggil Tukang Sampah RAM
                
                success_count += 1
                
                # Update Progress
                progress_bar.progress(int((i+1) / total_files * 80))
                
            except Exception as e:
                st.error(f"Gagal di file {file.name}: {e}")

        # 3. LOAD KE BIGQUERY (SEKALI JALAN)
        if success_count > 0:
            status_text.text("Memasukkan semua data ke BigQuery...")
            bq_client = bigquery.Client()
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_TRUNCATE", # Timpa isi tabel lama
                schema=BQ_SCHEMA,                   # Paksa Schema Permanen
                autodetect=False 
            )
            
            # BigQuery pintar, dia bisa baca "semua file *.parquet" sekaligus
            load_job = bq_client.load_table_from_uri(
                f"gs://{BUCKET_NAME}/upload/*.parquet", 
                f"{DATASET_ID}.{TABLE_ID}", 
                job_config=job_config
            )
            load_job.result()
            
            progress_bar.progress(90)
            
            # 4. FINAL CLEANUP
            status_text.text("Membersihkan Bucket...")
            blobs = bucket.list_blobs(prefix="upload/")
            for blob in blobs: blob.delete()
            
            progress_bar.progress(100)
            status_text.text("Selesai!")
            st.success(f"🎉 SUKSES! {success_count} File berhasil diproses secara Estafet.")
            st.balloons()
        else:
            st.error("Tidak ada file yang berhasil diproses.")


