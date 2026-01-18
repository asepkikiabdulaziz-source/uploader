import streamlit as st
import pandas as pd
import os
import json
import tempfile
import gc
from datetime import datetime, timedelta
from google.cloud import storage, bigquery

# ==========================================
# 1. KONFIGURASI HALAMAN
# ==========================================
st.set_page_config(
    page_title="UPLOADER", 
    page_icon="🏢", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==========================================
# 2. AUTENTIKASI GOOGLE CLOUD
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

# ==========================================
# 3. KONSTANTA & SCHEMA DATABASE
# ==========================================
BUCKET_NAME = "transaksi-upload" 
DATASET_ID = "pma"

# --- A. SCHEMA TRANSAKSI (PENJUALAN) ---
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

# --- B. SCHEMA MASTER CUSTOMER (CB) ---
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
    bigquery.SchemaField("kd_salesman", "STRING"), # WAJIB STRING
    bigquery.SchemaField("nama_salesman", "STRING"),
    bigquery.SchemaField("pma", "STRING"),
    bigquery.SchemaField("plan", "STRING"),
    bigquery.SchemaField("nik_salesman", "STRING"),
    bigquery.SchemaField("bln", "STRING")
]

# ==========================================
# 4. SIDEBAR NAVIGASI
# ==========================================
with st.sidebar:
    st.title("🏢 Admin Uploader")
    st.write("Pilih Modul:")
    
    selected_mode = st.radio(
        "Menu Navigasi",
        ["🚀 Berjalan", "👥 Master Customer (CB)", "📚 Closing"]
    )
    
    st.divider()
    show_preview = st.checkbox("Tampilkan Preview Data", value=True)
    st.caption("v5.0 - Full Features")

# ==========================================
# 5. LOGIC SWITCHER (PENGATUR MODE)
# ==========================================
active_map = {}
active_schema = []
target_table = ""
enable_date_filter = False

# --- MODE 1: TRANSAKSI HARIAN ---
if selected_mode == "🚀 Transaksi Harian":
    st.title("🚀 Upload Transaksi Harian")
    st.info("Mode ini memiliki **Filter Tanggal**. Data masa depan akan dibuang.")
    
    active_map = MAP_TRX
    active_schema = SCHEMA_TRX
    target_table = "berjalan"
    enable_date_filter = True 
    
    col1, col2 = st.columns([1, 3])
    with col1:
        default_date = datetime.now().date() - timedelta(days=1)
        cutoff_date = st.date_input("🛑 Cut-off Tanggal", value=default_date)
    with col2:
        st.write(f"Sistem hanya menerima data s/d tanggal **{cutoff_date.strftime('%d %b %Y')}**.")

# --- MODE 2: MASTER CUSTOMER (CB) ---
elif selected_mode == "👥 Master Customer (CB)":
    st.title("👥 Upload Master Customer (CB)")
    st.warning("⚠️ **PERHATIAN:** Mode ini akan **MENIMPA (REPLACE)** seluruh data Customer lama.")
    
    active_map = MAP_CUST
    active_schema = SCHEMA_CUST
    target_table = "cb_berjalan"
    enable_date_filter = False 

# --- MODE 3: HISTORY ---
elif selected_mode == "📚 Cicil History Data":
    st.title("📚 Upload History Data (Backlog)")
    st.info("Mode bebas hambatan. Semua tanggal diterima. Target: `staging_history`")
    
    active_map = MAP_TRX      
    active_schema = SCHEMA_TRX 
    target_table = "staging_history"
    enable_date_filter = False

st.divider()

# ==========================================
# 6. FILE UPLOADER & QUEUE MONITOR
# ==========================================
uploaded_files = st.file_uploader(f"Pilih File Excel ({selected_mode})", type=['xlsx', 'xls'], accept_multiple_files=True)

if uploaded_files:
    # --- FITUR ANTRIAN FILE (PENGGANTI PAGINATION) ---
    st.success(f"📂 **{len(uploaded_files)} File Siap Diproses.**")
    
    # Buat tabel daftar file
    file_list_data = [{"No": i+1, "Nama File": f.name, "Size (KB)": round(f.size/1024, 1)} for i, f in enumerate(uploaded_files)]
    df_queue = pd.DataFrame(file_list_data)
    
    # Tampilkan dengan tinggi 600px (muat +/- 20 baris)
    with st.expander("📋 Lihat Daftar Lengkap File (Klik Disini)", expanded=True):
        st.dataframe(
            df_queue, 
            use_container_width=True, 
            hide_index=True,
            height=600 # <--- SOLUSI PAGINATION
        )

    # TOMBOL EKSEKUSI
    if st.button(f"🚀 MULAI PROSES UPLOAD ({selected_mode})", type="primary"):
        
        # UI Elements untuk Progress
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Area Preview (Opsional)
        if show_preview:
            st.subheader("🔍 Live Data Preview (20 Baris)")
            preview_container = st.empty()

        # Area Log Filter (Hanya mode transaksi)
        if enable_date_filter:
            log_box = st.expander("📜 Audit Trail: Filter Tanggal", expanded=True)
        
        # Setup Koneksi GCP
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        bq_client = bigquery.Client()
        table_ref = f"{DATASET_ID}.{target_table}"

        # --- PHASE 1: RESET TABEL & BUCKET ---
        status_text.text(f"🧹 Membersihkan Tabel Target: {target_table}...")
        bq_client.delete_table(table_ref, not_found_ok=True)
        
        status_text.text("🧹 Membersihkan Bucket Staging...")
        blobs = bucket.list_blobs(prefix="upload/")
        for blob in blobs: blob.delete()
        
        progress_bar.progress(5)

        # --- PHASE 2: LOOPING FILE ---
        success_count = 0
        total_files = len(uploaded_files)

        for i, file in enumerate(uploaded_files):
            try:
                status_text.text(f"⏳ Sedang Memproses File {i+1} dari {total_files}: {file.name}")
                
                # 1. Baca Excel
                df = pd.read_excel(file, dtype=object, engine='openpyxl')
                
                # 2. Standardisasi Header (Trim & Upper)
                df.columns = df.columns.str.strip().str.upper()
                
                # 3. Rename Header Sesuai Mode
                df.rename(columns=active_map, inplace=True)

                # --- LOGIC KHUSUS: MASTER CUSTOMER (CB) ---
                if selected_mode == "👥 Master Customer (CB)":
                    # a. Hardcode Bulan DEC
                    df['bln'] = 'DEC'
                    
                    # b. Fill NA untuk kolom teks
                    fill_cols = ['fc', 'rayon', 'alamat', 'kabupaten', 'kecamatan', 'kelurahan', 'div', 'nama_salesman', 'nik_salesman']
                    for col in fill_cols:
                        if col in df.columns: df[col] = df[col].fillna('')
                    
                    # c. KD_SLS Wajib String & Bersih (123.0 -> 123)
                    if 'kd_salesman' in df.columns:
                        df['kd_salesman'] = df['kd_salesman'].astype(str).str.replace(r'\.0$', '', regex=True).replace('nan', 'N/A').str.strip()
                    
                    # d. Handle Tanggal Register
                    if 'tgl_register' in df.columns:
                         df['tgl_register'] = pd.to_datetime(df['tgl_register'], errors='coerce').dt.date

                # --- LOGIC KHUSUS: TRANSAKSI ---
                elif selected_mode != "👥 Master Customer (CB)":
                    # a. Convert Tanggal
                    if 'tgl' in df.columns:
                        df['tgl'] = pd.to_datetime(df['tgl'], errors='coerce').dt.date
                        
                        # b. Filter Tanggal (Time Gatekeeper)
                        if enable_date_filter:
                            initial_rows = len(df)
                            df = df[df['tgl'] <= cutoff_date] # Filter inti
                            dropped_rows = initial_rows - len(df)
                            
                            if dropped_rows > 0:
                                log_box.warning(f"⚠️ **{file.name}**: {dropped_rows} baris DIBUANG ( > {cutoff_date}).")
                            else:
                                log_box.success(f"✅ **{file.name}**: Semua Data Lolos.")
                    
                    # c. Paksa Angka jadi Float (Desimal)
                    nums = ['qty', 'value', 'value_nett']
                    for n in nums:
                        if n in df.columns:
                            df[n] = pd.to_numeric(df[n], errors='coerce').fillna(0.0).astype(float)

                # --- FINAL CLEANING (SEMUA MODE) ---
                # 1. Ambil hanya kolom yang ada di Schema
                valid_cols = [f.name for f in active_schema if f.name in df.columns]
                df = df[valid_cols]

                # 2. Bersihkan Kolom String (Hapus .0, Trim, Upper)
                schema_types = {f.name: f.field_type for f in active_schema}
                for col in df.columns:
                    ftype = schema_types.get(col, 'STRING')
                    if ftype == 'STRING':
                        df[col] = df[col].astype(str).str.strip().replace('nan', '').str.replace(r'\.0$', '', regex=True)
                        # Force Uppercase untuk kode tertentu
                        if col in ['pma', 'kode_outlet', 'fc', 'plan', 'channel']:
                             df[col] = df[col].str.upper()

                # --- UPDATE LIVE PREVIEW ---
                if show_preview:
                    preview_container.dataframe(df.head(20), use_container_width=True)

                # --- UPLOAD PARQUET ---
                if not df.empty:
                    temp_filename = f"part_{i}.parquet"
                    df.to_parquet(temp_filename, index=False)
                    blob = bucket.blob(f"upload/{temp_filename}")
                    blob.upload_from_filename(temp_filename)
                    os.remove(temp_filename)
                    success_count += 1
                
                # Cleanup Memory
                del df
                gc.collect()
                
                # Update Progress Bar
                progress_bar.progress(10 + int((i+1) / total_files * 80))
                
            except Exception as e:
                st.error(f"❌ Gagal memproses file {file.name}: {e}")

        # --- PHASE 3: FINAL LOAD TO BIGQUERY ---
        if success_count > 0:
            status_text.text(f"📥 Mengimpor Data ke BigQuery ({target_table})...")
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_TRUNCATE", # Reset isi tabel target
                schema=active_schema,
                autodetect=False 
            )
            
            # Load Semua File *.parquet sekaligus
            load_job = bq_client.load_table_from_uri(
                f"gs://{BUCKET_NAME}/upload/*.parquet", 
                table_ref, 
                job_config=job_config
            )
            
            try:
                load_job.result() # Tunggu sampai selesai
                
                # Cleanup Bucket Akhir
                blobs = bucket.list_blobs(prefix="upload/")
                for blob in blobs: blob.delete()
                
                progress_bar.progress(100)
                st.balloons()
                st.success(f"🎉 SUKSES! Data berhasil masuk ke tabel `{target_table}`.")
                
            except Exception as e:
                st.error("❌ BigQuery Error Details:")
                if hasattr(e, 'errors'): st.json(e.errors)
                else: st.write(e)
        else:
            st.warning("⚠️ Tidak ada data yang berhasil diproses. Periksa format file atau filter tanggal.")
