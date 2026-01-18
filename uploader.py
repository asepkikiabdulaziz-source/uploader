import streamlit as st
import pandas as pd
import os
import json
import tempfile
import gc
import time
from datetime import datetime, timedelta
from google.cloud import storage, bigquery

# ==========================================
# 1. KONFIGURASI HALAMAN
# ==========================================
st.set_page_config(
    page_title="Admin Portal", 
    page_icon="🏢", 
    layout="wide"
)

# Indikator Versi
st.success("✅ SYSTEM READY: VERSI 13.0 (AUTO-RESTART MODE)")

# ==========================================
# 2. AUTH & CONSTANTS
# ==========================================
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
    st.error(f"❌ Masalah Kunci GCP: {e}")
    st.stop()

BUCKET_NAME = "transaksi-upload" 
DATASET_ID = "pma"

# ==========================================
# 3. SCHEMA
# ==========================================
MAP_TRX = {
    "TGL": "tgl", "NO FAKTUR": "no_faktur", "KODE OUTLET": "kode_outlet",
    "NAMA OUTLET": "nama_outlet", "CHANNEL": "channel", "FC": "fc",
    "RUTE": "rute", "PMA": "pma", "KODE SALESMAN": "kode_salesman",
    "KD_BRG": "kd_brg", "NM_BRG": "nm_brg", "BU": "bu", "MARK": "mark",
    "KODE BARANG": "kode_barang", "DESCRIPTION": "description", "QTY": "qty",
    "VALUE": "value", "VALUE NETT": "value_nett", "BLN": "bln",
    "KD SLS2": "kd_sls2", "DIV": "div"
}
SCHEMA_TRX = [
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

MAP_CUST = {
    'KODE OUTLET': 'kode_outlet', 'NAMA OUTLET': 'nama_outlet', 'FC': 'fc',
    'ALAMAT': 'alamat', 'KET.KABUPATEN': 'kabupaten', 'KET.KECAMATAN': 'kecamatan',
    'KET.KELURAHAN': 'kelurahan', 'DIV': 'div', 'TYPE OUTLET': 'type_outlet',
    'FLAG': 'flag', 'TGL REGISTER': 'tgl_register', 'RAYON': 'rayon',
    'KD_SLS': 'kd_salesman', 'NAMA_SLS': 'nama_salesman', 'PMA': 'pma',
    'KODE SCYLLA': 'plan', 'NIK SALESMAN': 'nik_salesman'
}
SCHEMA_CUST = [
    bigquery.SchemaField("kode_outlet", "STRING"),
    bigquery.SchemaField("nama_outlet", "STRING"),
    bigquery.SchemaField("fc", "STRING"),
    bigquery.SchemaField("alamat", "STRING"),
    bigquery.SchemaField("kabupaten", "STRING"),
    bigquery.SchemaField("kecamatan", "STRING"),
    bigquery.SchemaField("kelurahan", "STRING"),
    bigquery.SchemaField("div", "STRING"),
    bigquery.SchemaField("type_outlet", "STRING"),
    bigquery.SchemaField("flag", "STRING"),
    bigquery.SchemaField("tgl_register", "DATE"),
    bigquery.SchemaField("rayon", "STRING"),
    bigquery.SchemaField("kd_salesman", "STRING"),
    bigquery.SchemaField("nama_salesman", "STRING"),
    bigquery.SchemaField("pma", "STRING"),
    bigquery.SchemaField("plan", "STRING"),
    bigquery.SchemaField("nik_salesman", "STRING"),
    bigquery.SchemaField("bln", "STRING")
]

# ==========================================
# 4. SIDEBAR & SESSION STATE
# ==========================================
if 'proc_index' not in st.session_state:
    st.session_state['proc_index'] = 0
if 'is_running' not in st.session_state:
    st.session_state['is_running'] = False
if 'log_history' not in st.session_state:
    st.session_state['log_history'] = []

with st.sidebar:
    st.title("🏢 Admin SFA")
    selected_mode = st.radio("Modul:", ["🚀 Transaksi Harian", "👥 Master Customer (CB)", "📚 Cicil History Data"])
    st.divider()
    
    cutoff_date = None
    force_overwrite = False
    if selected_mode == "🚀 Transaksi Harian":
        default_date = datetime.now().date() - timedelta(days=1)
        cutoff_date = st.date_input("Cut-Off:", value=default_date)
    if selected_mode == "📚 Cicil History Data":
        force_overwrite = st.checkbox("⚠️ Mode Revisi", value=False)

    st.caption("v13.0 - Auto Restart")
    
    # Tombol Reset
    if st.button("🔄 Reset Status"):
        st.session_state['proc_index'] = 0
        st.session_state['is_running'] = False
        st.session_state['log_history'] = []
        st.rerun()

# ==========================================
# 5. LOGIC UTAMA
# ==========================================
active_map = {}; active_schema = []; target_table = ""; enable_date_filter = False; check_collision = False

if selected_mode == "🚀 Transaksi Harian":
    st.title("🚀 Transaksi Harian")
    active_map = MAP_TRX; active_schema = SCHEMA_TRX; target_table = "berjalan"
    enable_date_filter = True; check_collision = False
elif selected_mode == "👥 Master Customer (CB)":
    st.title("👥 Master Customer (CB)")
    active_map = MAP_CUST; active_schema = SCHEMA_CUST; target_table = "staging_cb"
    enable_date_filter = False; check_collision = False
elif selected_mode == "📚 Cicil History Data":
    st.title("📚 Upload History")
    st.info("💡 Tips: Upload per 3-5 File agar server tidak crash.")
    active_map = MAP_TRX; active_schema = SCHEMA_TRX; target_table = "staging_history"
    enable_date_filter = False; check_collision = True

st.divider()

# ==========================================
# 6. ESTAFET PROCESSOR
# ==========================================
uploaded_files = st.file_uploader(f"File ({selected_mode})", type=['xlsx', 'xls'], accept_multiple_files=True)

# Tampilkan Log History
if st.session_state['log_history']:
    with st.expander("📜 Log Aktivitas Sesi Ini", expanded=True):
        for log in st.session_state['log_history']:
            st.text(log)

if uploaded_files:
    total_files = len(uploaded_files)
    current_idx = st.session_state['proc_index']
    
    # Progress Bar Global
    progress = int((current_idx / total_files) * 100)
    st.progress(progress)
    st.caption(f"Progress: {current_idx} / {total_files} File Selesai")

    # Jika tombol mulai ditekan ATAU sedang running (estafet)
    if st.button(f"🚀 MULAI PROSES", type="primary") or st.session_state['is_running']:
        
        # Cek apakah sudah selesai semua
        if current_idx >= total_files:
            st.session_state['is_running'] = False
            st.balloons()
            st.success("🎉 SEMUA FILE SELESAI DIPROSES!")
            if st.button("Selesai & Reset"):
                st.session_state['proc_index'] = 0
                st.session_state['log_history'] = []
                st.rerun()
            st.stop()

        # Mulai Estafet
        st.session_state['is_running'] = True
        file_to_process = uploaded_files[current_idx]
        
        status_box = st.empty()
        status_box.info(f"⏳ Sedang memproses file ke-{current_idx + 1}: **{file_to_process.name}**...")
        
        # --- PROSES 1 FILE ---
        try:
            bq_client = bigquery.Client()
            bucket = storage.Client().bucket(BUCKET_NAME)
            table_ref = f"{DATASET_ID}.{target_table}"

            # 1. Baca Excel
            df = pd.read_excel(file_to_process, dtype=object, engine='openpyxl')
            df.columns = df.columns.str.strip().str.upper()
            df.rename(columns=active_map, inplace=True)

            # 2. Logic Cleaning
            if selected_mode == "👥 Master Customer (CB)":
                df['bln'] = 'DEC'
                fill_cols = ['fc', 'rayon', 'alamat', 'kabupaten', 'kecamatan', 'kelurahan', 'div', 'nama_salesman', 'nik_salesman']
                for col in fill_cols:
                        if col in df.columns: df[col] = df[col].fillna('')
                if 'kd_salesman' in df.columns:
                    df['kd_salesman'] = df['kd_salesman'].astype(str).str.replace(r'\.0$', '', regex=True).replace('nan', 'N/A').str.strip()
                if 'tgl_register' in df.columns:
                        df['tgl_register'] = pd.to_datetime(df['tgl_register'], errors='coerce').dt.date
            else:
                if 'tgl' in df.columns:
                    df['tgl'] = pd.to_datetime(df['tgl'], errors='coerce').dt.date
                    if enable_date_filter and cutoff_date:
                        df = df[df['tgl'] <= cutoff_date]
                    if check_collision and not df.empty:
                        min_d, max_d = df['tgl'].min(), df['tgl'].max()
                        try:
                            rows = list(bq_client.query(f"SELECT count(1) as cnt FROM `{DATASET_ID}.{target_table}` WHERE tgl BETWEEN '{min_d}' AND '{max_d}'").result())
                            if rows[0].cnt > 0:
                                if force_overwrite:
                                    bq_client.query(f"DELETE FROM `{DATASET_ID}.{target_table}` WHERE tgl BETWEEN '{min_d}' AND '{max_d}'").result()
                                    st.session_state['log_history'].append(f"⚠️ {file_to_process.name}: Overwrite Data.")
                                else:
                                    st.session_state['log_history'].append(f"⛔ {file_to_process.name}: SKIP (Duplikat).")
                                    # Lompati upload, langsung ke next file
                                    st.session_state['proc_index'] += 1
                                    st.rerun()
                        except: pass

            nums = ['qty', 'value', 'value_nett']
            for n in nums:
                if n in df.columns: df[n] = pd.to_numeric(df[n], errors='coerce').fillna(0.0).astype(float)

            # 3. Final Schema
            valid_cols = [f.name for f in active_schema if f.name in df.columns]
            df = df[valid_cols]
            schema_types = {f.name: f.field_type for f in active_schema}
            for col in df.columns:
                if schema_types.get(col) == 'STRING':
                    df[col] = df[col].astype(str).str.strip().replace('nan', '').str.replace(r'\.0$', '', regex=True)
                    if col in ['pma', 'kode_outlet', 'fc', 'plan']: df[col] = df[col].str.upper()

            # 4. Upload
            if not df.empty:
                temp_filename = f"temp_{current_idx}.parquet"
                df.to_parquet(temp_filename, index=False)
                bucket.blob(f"upload/{temp_filename}").upload_from_filename(temp_filename)
                os.remove(temp_filename)
                
                # Load BQ
                write_action = "WRITE_APPEND"
                if selected_mode != "📚 Cicil History Data" and current_idx == 0:
                    write_action = "WRITE_TRUNCATE"

                job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET, write_disposition=write_action, schema=active_schema, autodetect=False)
                load_job = bq_client.load_table_from_uri(f"gs://{BUCKET_NAME}/upload/*.parquet", table_ref, job_config=job_config)
                load_job.result()
                
                # Cleanup Bucket
                for blob in bucket.list_blobs(prefix="upload/"): blob.delete()
                
                st.session_state['log_history'].append(f"✅ {file_to_process.name}: Sukses.")
            
            # 5. BERSIHKAN MEMORI
            del df
            gc.collect()

            # 6. UPDATE INDEX & REFRESH HALAMAN (ESTAFET)
            st.session_state['proc_index'] += 1
            st.rerun()

        except Exception as e:
            st.error(f"Gagal memproses {file_to_process.name}: {e}")
            st.session_state['is_running'] = False # Stop jika error fatal
