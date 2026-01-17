import streamlit as st
import pandas as pd
import os
import json
import tempfile
import gc
from google.cloud import storage, bigquery

# --- CONFIG ---
st.set_page_config(page_title="SFA Uploader", page_icon="📦")
st.title("📦 SFA UPLOADER (BATCH MODE)")
st.caption("Logika: Reset Tabel > Upload Semua > Load Sekali")

# --- AUTH ---
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

# --- SCHEMA (ALL FLOAT) ---
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

uploaded_files = st.file_uploader("Upload File", type=['xlsx', 'xls'], accept_multiple_files=True)

if uploaded_files:
    st.info(f"📂 Terdeteksi {len(uploaded_files)} file siap proses.")
    
    if st.button("MULAI PROSES BATCH 🚀"):
        progress_bar = st.progress(0)
        status = st.empty()
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        bq_client = bigquery.Client()
        table_ref = f"{DATASET_ID}.{TABLE_ID}"

        # ==========================================
        # PHASE 1: PREPARATION (RESET DI AWAL)
        # ==========================================
        status.text("🛑 Menghapus Tabel Lama & Membersihkan Bucket...")
        
        # 1. Hapus Tabel BigQuery Dulu (Supaya bersih total)
        bq_client.delete_table(table_ref, not_found_ok=True)
        
        # 2. Hapus File Sampah di Bucket GCS
        blobs = bucket.list_blobs(prefix="upload/")
        for blob in blobs: blob.delete()
        
        progress_bar.progress(10)

        # ==========================================
        # PHASE 2: LOOPING UPLOAD (ESTAFET)
        # ==========================================
        success_count = 0
        total_files = len(uploaded_files)

        for i, file in enumerate(uploaded_files):
            try:
                status.text(f"📤 Mengirim File {i+1} dari {total_files}: {file.name}")
                
                # Baca Excel
                df = pd.read_excel(file, dtype=object, engine='openpyxl')
                
                # Mapping & Filter
                df.columns = df.columns.str.strip().str.upper()
                df.rename(columns=EXCEL_TO_BQ_MAP, inplace=True)
                valid_cols = [f.name for f in BQ_SCHEMA if f.name in df.columns]
                df = df[valid_cols]

                # --- CLEANING (ALL FLOAT) ---
                if 'tgl' in df.columns:
                    df['tgl'] = pd.to_datetime(df['tgl'], errors='coerce').dt.date

                numeric_cols = ['qty', 'value', 'value_nett']
                for nc in numeric_cols:
                    if nc in df.columns:
                        df[nc] = pd.to_numeric(df[nc], errors='coerce').fillna(0.0).astype(float)

                non_string = ['tgl'] + numeric_cols
                str_cols = [c for c in df.columns if c not in non_string]
                for sc in str_cols:
                    df[sc] = df[sc].astype(str).str.strip().replace('nan', '').str.replace(r'\.0$', '', regex=True)
                    if sc in ['pma', 'channel', 'kode_outlet', 'no_faktur', 'fc']: 
                        df[sc] = df[sc].str.upper()

                # Save Partial Parquet
                temp_filename = f"part_{i}.parquet"
                df.to_parquet(temp_filename, index=False)
                
                # Upload ke GCS
                blob = bucket.blob(f"upload/{temp_filename}")
                blob.upload_from_filename(temp_filename)
                
                # Hapus Memori
                os.remove(temp_filename)
                del df
                gc.collect()
                
                success_count += 1
                
                # Update Progress Bar (10% - 90%)
                current_progress = 10 + int((i+1) / total_files * 80)
                progress_bar.progress(current_progress)
                
            except Exception as e:
                st.error(f"❌ Gagal di file {file.name}: {e}")

        # ==========================================
        # PHASE 3: FINAL LOAD (HANYA SEKALI DI AKHIR)
        # ==========================================
        if success_count > 0:
            status.text("📥 Memasukkan SEMUA Data ke BigQuery...")
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_TRUNCATE", # Aman karena tabel sudah kosong/baru
                schema=BQ_SCHEMA,
                autodetect=False 
            )
            
            # Perintah ini mengambil SEMUA file *.parquet yang tadi kita upload
            load_job = bq_client.load_table_from_uri(
                f"gs://{BUCKET_NAME}/upload/*.parquet", 
                table_ref, 
                job_config=job_config
            )
            
            try:
                load_job.result() # Tunggu sampai selesai
                
                # Cleanup Akhir
                status.text("🧹 Membersihkan sisa file...")
                blobs = bucket.list_blobs(prefix="upload/")
                for blob in blobs: blob.delete()
                
                progress_bar.progress(100)
                st.success(f"🎉 SUKSES TOTAL! {success_count} File berhasil digabung dan masuk ke BigQuery.")
                st.balloons()
                
            except Exception as e:
                st.error("BigQuery Error:")
                if hasattr(e, 'errors'): st.json(e.errors)
                else: st.write(e)
        else:
            st.error("Tidak ada file yang berhasil diproses.")
