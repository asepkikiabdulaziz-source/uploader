import streamlit as st
import pandas as pd
import os
import json
import tempfile
from google.cloud import storage, bigquery

# --- KONFIGURASI HALAMAN ---
st.set_page_config(page_title="DBASE Uploader", page_icon="📦")
st.title("📦 DBASE UPLOADER")
st.caption("Support: 60+ File Sekaligus | Auto-Merge")

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

# --- KONFIGURASI BUCKET ---
BUCKET_NAME = "transaksi-upload" 
DATASET_ID = "pma"
TABLE_ID = "berjalan"

# --- MAPPING ---
EXCEL_TO_BQ_MAP = {
    "TGL": "tgl", "NO_FAKTUR": "no_faktur", "KODE OUTLET": "kode_outlet",
    "NAMA OUTLET": "nama_outlet", "CHANNEL": "channel", "FC": "fc",
    "RUTE": "rute", "PMA": "pma", "KODE SALESMAN": "kode_salesman",
    "KD_BRG": "kd_brg", "NM_BRG": "nm_brg", "BU": "bu", "MARK": "mark",
    "KODE BARANG": "kode_barang", "DESCRIPTION": "description", "QTY": "qty",
    "VALUE": "value", "VALUE_NETT": "value_nett", "BLN": "bln",
    "KD_SLS2": "kd_sls2", "DIV": "div"
}

# --- UI UPLOAD (CHANGE: ACCEPT MULTIPLE FILES) ---
st.info("💡 Tips: Anda bisa blok 60 file sekaligus lalu tarik ke sini.")
uploaded_files = st.file_uploader("Upload File Transaksi Harian", type=['xlsx', 'xls'], accept_multiple_files=True)

if uploaded_files:
    try:
        # 1. LOOPING BACA & GABUNG FILE
        st.write(f"📂 Mendeteksi {len(uploaded_files)} file. Sedang menggabungkan...")
        
        all_dfs = []
        progress_text = st.empty()
        my_bar = st.progress(0)
        
        for i, file in enumerate(uploaded_files):
            # Update progress bar biar admin tau robot lagi kerja
            percent = int(((i+1) / len(uploaded_files)) * 20) # Max 20% untuk proses baca
            my_bar.progress(percent)
            
            # Baca file
            df_temp = pd.read_excel(file, dtype=object)
            all_dfs.append(df_temp)
        
        # JURUS GABUNG (CONCAT)
        df = pd.concat(all_dfs, ignore_index=True)
        
        st.success(f"✅ SUKSES GABUNG! Total Data: {len(df):,} Baris dari {len(uploaded_files)} File.")
        st.dataframe(df.head(3))
        
        if st.button("PROSES & KIRIM KE CLOUD"):
            progress = st.progress(20)
            status = st.empty()
            
            # ==========================================
            # 2. MAPPING & CLEANING (Sama seperti sebelumnya)
            # ==========================================
            status.text("Sedang membersihkan data...")
            
            df.columns = df.columns.str.strip().str.upper()
            df.rename(columns=EXCEL_TO_BQ_MAP, inplace=True)
            valid_cols = [c for c in df.columns if c in EXCEL_TO_BQ_MAP.values()]
            df = df[valid_cols]

            # A. UPPERCASE KHUSUS PMA & CHANNEL
            cols_to_upper = ['pma', 'channel']
            for col in cols_to_upper:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip().str.upper().replace('NAN', '')

            # B. STANDARD CLEANING LAINNYA
            cols_normal = ['nama_outlet', 'rute', 'kode_salesman', 'description', 'nm_brg']
            for col in cols_normal:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip().replace('nan', '')

            if 'tgl' in df.columns:
                df['tgl'] = pd.to_datetime(df['tgl'], errors='coerce').dt.date

            for num in ['qty', 'value', 'value_nett']:
                if num in df.columns: 
                    df[num] = pd.to_numeric(df[num], errors='coerce').fillna(0)

            progress.progress(50)
            
            # ==========================================
            # 3. UPLOAD GCS
            # ==========================================
            status.text("Mengirim Paket Data ke Cloud...")
            storage_client = storage.Client()
            bucket = storage_client.bucket(BUCKET_NAME)
            
            # Pre-Clean
            blobs = bucket.list_blobs(prefix="upload/")
            for blob in blobs: blob.delete()
            
            pq_name = "bulk_upload.parquet"
            df.to_parquet(pq_name, index=False)
            blob = bucket.blob(f"upload/{pq_name}")
            blob.upload_from_filename(pq_name)
            progress.progress(75)
            
            # ==========================================
            # 4. BIGQUERY LOAD
            # ==========================================
            status.text("Memasukkan ke BigQuery...")
            bq_client = bigquery.Client()
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_TRUNCATE",
                autodetect=True
            )
            load_job = bq_client.load_table_from_uri(
                f"gs://{BUCKET_NAME}/upload/*.parquet", 
                f"{DATASET_ID}.{TABLE_ID}", 
                job_config=job_config
            )
            load_job.result()
            progress.progress(90)
            
            # ==========================================
            # 5. CLEANUP
            # ==========================================
            status.text("Sapu Jagat (Membersihkan Bucket)...")
            blobs = bucket.list_blobs(prefix="upload/")
            for blob in blobs: blob.delete()
            
            progress.progress(100)
            st.success(f"🎉 SELESAI! {len(uploaded_files)} File berhasil diproses menjadi 1 Tabel Staging.")
            st.balloons()
            
    except Exception as e:
        st.error(f"Error: {e}")


