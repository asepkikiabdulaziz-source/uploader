import streamlit as st
import pandas as pd
import os
import json
import tempfile
from google.cloud import storage, bigquery

# --- KONFIGURASI HALAMAN ---
st.set_page_config(page_title="DBASE Uploader", page_icon="📦")
st.title("📦 DBASE UPLOADER")
st.caption("Mode: Strict Numeric (Hanya Value & Qty)")

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

# --- MAPPING KOLOM ---
EXCEL_TO_BQ_MAP = {
    "TGL": "tgl", "NO FAKTUR": "no_faktur", "KODE OUTLET": "kode_outlet",
    "NAMA OUTLET": "nama_outlet", "CHANNEL": "channel", "FC": "fc",
    "RUTE": "rute", "PMA": "pma", "KODE SALESMAN": "kode_salesman",
    "KD_BRG": "kd_brg", "NM_BRG": "nm_brg", "BU": "bu", "MARK": "mark",
    "KODE BARANG": "kode_barang", "DESCRIPTION": "description", "QTY": "qty",
    "VALUE": "value", "VALUE NETT": "value_nett", "BLN": "bln",
    "KD SLS2": "kd_sls2", "DIV": "div"
}

# --- ATURAN KETAT TIPE DATA ---
# 1. HANYA INI YANG BOLEH ANGKA
ALLOWED_NUMERIC = ['qty', 'value', 'value_nett']

# 2. HANYA INI YANG TANGGAL
ALLOWED_DATE = ['tgl']

# 3. SISANYA = WAJIB STRING/TEXT
# (fc, bln, div, mark, dll akan masuk sini otomatis)

# --- UI UPLOAD ---
st.info("💡 Tips: Blok semua file Excel, lalu tarik ke sini.")
uploaded_files = st.file_uploader("Upload File Transaksi", type=['xlsx', 'xls'], accept_multiple_files=True)

if uploaded_files:
    try:
        st.write(f"📂 Mendeteksi {len(uploaded_files)} file. Menggabungkan...")
        
        all_dfs = []
        my_bar = st.progress(0)
        
        for i, file in enumerate(uploaded_files):
            percent = int(((i+1) / len(uploaded_files)) * 20)
            my_bar.progress(percent)
            
            # Baca sebagai OBJECT (String) dulu biar aman
            df_temp = pd.read_excel(file, dtype=object)
            all_dfs.append(df_temp)
        
        # Gabung Data
        df = pd.concat(all_dfs, ignore_index=True)
        
        st.success(f"✅ Total Data: {len(df):,} Baris.")
        st.dataframe(df.head(3))
        
        if st.button("PROSES & KIRIM"):
            progress = st.progress(20)
            status = st.empty()
            
            # ==========================================
            # 1. MAPPING HEADER
            # ==========================================
            status.text("Mapping Header...")
            df.columns = df.columns.str.strip().str.upper()
            df.rename(columns=EXCEL_TO_BQ_MAP, inplace=True)
            
            # Ambil kolom valid saja
            valid_cols = [c for c in df.columns if c in EXCEL_TO_BQ_MAP.values()]
            df = df[valid_cols]

            # ==========================================
            # 2. KONVERSI TIPE DATA (STRICT MODE)
            # ==========================================
            status.text("Membersihkan Data & Format Angka...")
            
            for col in df.columns:
                
                # A. JIKA KOLOM TANGGAL
                if col in ALLOWED_DATE:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                
                # B. JIKA KOLOM ANGKA (Hanya Qty, Value, Value Nett)
                elif col in ALLOWED_NUMERIC:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
                # C. SISANYA -> PAKSA JADI TEXT/STRING
                else:
                    # FC, BLN, DIV, MARK, KODE... masuk sini.
                    # 1. Ubah ke String
                    # 2. Buang Spasi
                    # 3. Ganti 'nan' jadi kosong
                    # 4. Hapus akhiran '.0' (misal: '10.0' jadi '10')
                    df[col] = df[col].astype(str).str.strip().replace('nan', '').str.replace(r'\.0$', '', regex=True)
                    
                    # Khusus PMA & CHANNEL -> HURUF BESAR
                    if col in ['pma', 'channel']:
                        df[col] = df[col].str.upper()

            progress.progress(50)
            
            # ==========================================
            # 3. UPLOAD KE BUCKET
            # ==========================================
            status.text("Upload ke Cloud Storage...")
            storage_client = storage.Client()
            bucket = storage_client.bucket(BUCKET_NAME)
            
            # Bersihkan sampah lama
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
            status.text("Sapu Jagat...")
            blobs = bucket.list_blobs(prefix="upload/")
            for blob in blobs: blob.delete()
            
            progress.progress(100)
            st.success(f"🎉 SELESAI! {len(uploaded_files)} File Masuk.")
            st.balloons()
            
    except Exception as e:
        st.error(f"Error: {e}")


