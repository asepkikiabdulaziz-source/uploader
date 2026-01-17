import streamlit as st
import pandas as pd
import os
import json
import tempfile
from google.cloud import storage, bigquery

# --- KONFIGURASI HALAMAN ---
st.set_page_config(page_title="SFA Uploader", page_icon="🚀")
st.title("🚀 PORTAL SFA CLOUD")
st.caption("Status: Online | Mapping Original | Upper Data Value")

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

# --- KONFIGURASI BUCKET & BQ ---
BUCKET_NAME = "transaksi-upload" 
DATASET_ID = "pma"
TABLE_ID = "berjalan"

# --- MAPPING ASLI (SESUAI REQUEST) ---
# Kiri: Header Excel (Huruf Besar), Kanan: Kolom BigQuery (Huruf Kecil)
EXCEL_TO_BQ_MAP = {
    "TGL": "tgl", 
    "NO_FAKTUR": "no_faktur", 
    "KODE OUTLET": "kode_outlet",
    "NAMA OUTLET": "nama_outlet", 
    "CHANNEL": "channel", 
    "FC": "fc",
    "RUTE": "rute", 
    "PMA": "pma", 
    "KODE SALESMAN": "kode_salesman",
    "KD_BRG": "kd_brg", 
    "NM_BRG": "nm_brg", 
    "BU": "bu", 
    "MARK": "mark",
    "KODE BARANG": "kode_barang", 
    "DESCRIPTION": "description", 
    "QTY": "qty",
    "VALUE": "value", 
    "VALUE_NETT": "value_nett", 
    "BLN": "bln",
    "KD_SLS2": "kd_sls2", 
    "DIV": "div"
}

# --- UI UPLOAD ---
uploaded_file = st.file_uploader("Upload Excel Transaksi", type=['xlsx', 'xls'])

if uploaded_file:
    try:
        # Baca Excel sebagai Object (agar angka 0 di depan kode tidak hilang)
        df = pd.read_excel(uploaded_file, dtype=object)
        
        st.success(f"✅ Data Terbaca: {len(df)} Baris")
        st.dataframe(df.head(3))
        
        if st.button("KIRIM DATA"):
            progress = st.progress(0)
            status = st.empty()
            
            # ==========================================
            # 1. MAPPING HEADER (Sesuai Kamus)
            # ==========================================
            status.text("Mapping Header Excel ke BigQuery...")
            
            # Paksa Header Excel jadi Huruf Besar & Buang Spasi (biar cocok sama Key Map)
            df.columns = df.columns.str.strip().str.upper()
            
            # Ganti Nama Kolom (Rename)
            df.rename(columns=EXCEL_TO_BQ_MAP, inplace=True)
            
            # Buang kolom Excel yang tidak ada di Map
            valid_cols = [c for c in df.columns if c in EXCEL_TO_BQ_MAP.values()]
            df = df[valid_cols]

            # ==========================================
            # 2. DATA VALUE CLEANING (Isi Data)
            # ==========================================
            status.text("Cleaning Isi Data...")

            # A. KHUSUS PMA & CHANNEL -> JADI HURUF BESAR (UPPERCASE)
            cols_to_upper = ['pma', 'channel']
            for col in cols_to_upper:
                if col in df.columns:
                    # Ambil isinya, ubah jadi string, buang spasi, jadikan HURUF BESAR
                    df[col] = df[col].astype(str).str.strip().str.upper().replace('NAN', '')

            # B. KOLOM LAIN -> BIARKAN HURUF ASLINYA (Hanya buang spasi & 'nan')
            cols_normal = ['nama_outlet', 'rute', 'kode_salesman', 'description', 'nm_brg']
            for col in cols_normal:
                if col in df.columns:
                    # Tidak ada .upper() disini
                    df[col] = df[col].astype(str).str.strip().replace('nan', '')

            # C. FORMAT TANGGAL
            if 'tgl' in df.columns:
                df['tgl'] = pd.to_datetime(df['tgl'], errors='coerce').dt.date

            # D. FORMAT ANGKA
            for num in ['qty', 'value', 'value_nett']:
                if num in df.columns: 
                    df[num] = pd.to_numeric(df[num], errors='coerce').fillna(0)

            progress.progress(30)
            
            # ==========================================
            # 3. UPLOAD KE BUCKET
            # ==========================================
            status.text("Upload ke Cloud Storage...")
            storage_client = storage.Client()
            bucket = storage_client.bucket(BUCKET_NAME)
            
            # Hapus sampah lama
            blobs = bucket.list_blobs(prefix="upload/")
            for blob in blobs: blob.delete()
            
            pq_name = "temp.parquet"
            df.to_parquet(pq_name, index=False)
            blob = bucket.blob(f"upload/{pq_name}")
            blob.upload_from_filename(pq_name)
            progress.progress(60)
            
            # ==========================================
            # 4. LOAD KE BIGQUERY
            # ==========================================
            status.text("Load ke BigQuery...")
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
            # 5. FINAL CLEANUP
            # ==========================================
            status.text("Membersihkan Bucket...")
            blobs = bucket.list_blobs(prefix="upload/")
            for blob in blobs: blob.delete()
            
            progress.progress(100)
            st.success("🎉 SUKSES! Data Mapping Aman & PMA sudah Uppercase.")
            st.balloons()
            
    except Exception as e:
        st.error(f"Error: {e}")