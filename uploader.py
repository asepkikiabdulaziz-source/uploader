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
    page_title="Data Uploader", 
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
    st.title("🏢 Admin SFA")
    st.write("Pilih Modul Kerja:")
    
    selected_mode = st.radio(
        "Menu Navigasi",
        ["🚀 Berjalan", "👥 Master Customer (CB)", "📚 Closing"]
    )
    
    st.divider()
    
    # Opsi Tambahan
    show_preview = st.checkbox("Tampilkan Preview Data", value=True)
    
    # Checkbox Khusus Mode History
    force_overwrite = False
    if selected_mode == "📚 Closing":
        st.markdown("---")
        st.caption("⚙️ Opsi History")
        force_overwrite = st.checkbox("⚠️ Mode Revisi (Timpa Data)", value=False, help="Centang ini jika ingin menghapus data lama di tanggal yang sama.")
        if force_overwrite:
            st.warning("Mode Revisi AKTIF. Data lama yang bentrok tanggalnya akan DIHAPUS.")

    st.caption("vFinal - Enterprise Grade")

# ==========================================
# 5. LOGIC SWITCHER (PENGATUR MODE)
# ==========================================
active_map = {}
active_schema = []
target_table = ""
enable_date_filter = False
check_collision = False # Cek Duplikat ke BigQuery

# --- MODE 1: TRANSAKSI HARIAN ---
if selected_mode == "🚀 Berjalan":
    st.title("🚀 Berjalan")
    st.info("Mode ini memiliki **Filter Tanggal**. Data masa depan akan dibuang. Tabel target akan di-RESET.")
    
    active_map = MAP_TRX
    active_schema = SCHEMA_TRX
    target_table = "berjalan"
    enable_date_filter = True 
    check_collision = False
    
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
    st.markdown("Logic Khusus: `KD_SLS` String, `BLN`='JAN'.")
    
    active_map = MAP_CUST
    active_schema = SCHEMA_CUST
    target_table = "staging_cb"
    enable_date_filter = False 
    check_collision = False

# --- MODE 3: HISTORY ---
elif selected_mode == "📚 Closing":
    st.title("📚 CLosing")
    st.info("Mode ini **MENAMBAH (APPEND)** data. Fitur **Anti-Duplikat** AKTIF.")
    
    active_map = MAP_TRX      
    active_schema = SCHEMA_TRX 
    target_table = "staging_history"
    enable_date_filter = False
    check_collision = True # Aktifkan Cek Bentrok

st.divider()

# ==========================================
# 6. FILE UPLOADER & PROCESSOR
# ==========================================
uploaded_files = st.file_uploader(f"Pilih File Excel ({selected_mode})", type=['xlsx', 'xls'], accept_multiple_files=True)

if uploaded_files:
    # --- FITUR ANTRIAN FILE (PENGGANTI PAGINATION) ---
    st.success(f"📂 **{len(uploaded_files)} File Siap Diproses.**")
    
    # Buat tabel daftar file
    file_list_data = [{"No": i+1, "Nama File": f.name, "Size (KB)": round(f.size/1024, 1)} for i, f in enumerate(uploaded_files)]
    df_queue = pd.DataFrame(file_list_data)
    
    # Tampilkan dengan tinggi 600px (muat +/- 20 baris)
    with st.expander("📋 Lihat Daftar Antrian File (Klik Disini)", expanded=True):
        st.dataframe(
            df_queue, 
            use_container_width=True, 
            hide_index=True,
            height=600 # <--- SOLUSI PAGINATION
        )

    # TOMBOL EKSEKUSI
    if st.button(f"🚀 MULAI PROSES UPLOAD ({selected_mode})", type="primary"):
        
        # UI Elements
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        if show_preview:
            st.subheader("🔍 Live Data Preview (20 Baris)")
            preview_container = st.empty()

        if enable_date_filter:
            log_box = st.expander("📜 Audit Trail: Filter Tanggal", expanded=True)
        
        # Setup GCP
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        bq_client = bigquery.Client()
        table_ref = f"{DATASET_ID}.{target_table}"

        # --- PHASE 1: RESET (Hanya untuk Mode Non-History) ---
        if selected_mode != "📚 Cicil History Data":
            status_text.text(f"🧹 Membersihkan Tabel Target: {target_table}...")
            bq_client.delete_table(table_ref, not_found_ok=True)
        
        # Bersihkan Bucket (Selalu)
        blobs = bucket.list_blobs(prefix="upload/")
        for blob in blobs: blob.delete()
        
        progress_bar.progress(5)

        # --- PHASE 2: LOOPING FILE ---
        success_count = 0
        total_files = len(uploaded_files)
        collision_detected = False

        for i, file in enumerate(uploaded_files):
            try:
                status_text.text(f"⏳ Analisis File {i+1}/{total_files}: {file.name}")
                
                # 1. Baca Excel
                df = pd.read_excel(file, dtype=object, engine='openpyxl')
                df.columns = df.columns.str.strip().str.upper()
                df.rename(columns=active_map, inplace=True)

                # --- LOGIC MODE MASTER CUSTOMER (CB) ---
                if selected_mode == "👥 Master Customer (CB)":
                    df['bln'] = 'DEC'
                    fill_cols = ['fc', 'rayon', 'alamat', 'kabupaten', 'kecamatan', 'kelurahan', 'div', 'nama_salesman', 'nik_salesman']
                    for col in fill_cols:
                        if col in df.columns: df[col] = df[col].fillna('')
                    
                    if 'kd_salesman' in df.columns:
                        df['kd_salesman'] = df['kd_salesman'].astype(str).str.replace(r'\.0$', '', regex=True).replace('nan', 'N/A').str.strip()
                    if 'tgl_register' in df.columns:
                         df['tgl_register'] = pd.to_datetime(df['tgl_register'], errors='coerce').dt.date

                # --- LOGIC MODE TRANSAKSI / HISTORY ---
                else:
                    if 'tgl' in df.columns:
                        df['tgl'] = pd.to_datetime(df['tgl'], errors='coerce').dt.date
                        
                        # A. Filter Tanggal (Harian Only)
                        if enable_date_filter:
                            initial = len(df)
                            df = df[df['tgl'] <= cutoff_date]
                            if (initial - len(df)) > 0: log_box.warning(f"⚠️ {file.name}: {initial - len(df)} baris DIBUANG.")
                            else: log_box.success(f"✅ {file.name}: OK.")

                        # B. Anti-Duplikat (History Only)
                        if check_collision and not df.empty:
                            min_d, max_d = df['tgl'].min(), df['tgl'].max()
                            # Cek BigQuery
                            check_sql = f"SELECT count(1) as cnt FROM `{DATASET_ID}.{target_table}` WHERE tgl BETWEEN '{min_d}' AND '{max_d}'"
                            try:
                                rows = list(bq_client.query(check_sql).result())
                                if rows[0].cnt > 0:
                                    if force_overwrite: # Hapus data lama
                                        status_text.warning(f"⚠️ Mode Revisi: Menghapus data lama ({min_d} - {max_d})...")
                                        bq_client.query(f"DELETE FROM `{DATASET_ID}.{target_table}` WHERE tgl BETWEEN '{min_d}' AND '{max_d}'").result()
                                    else: # Tolak File
                                        st.error(f"⛔ **SKIP {file.name}**: Data tanggal {min_d} s/d {max_d} sudah ada!")
                                        collision_detected = True
                                        continue # Skip file ini
                            except Exception: pass # Tabel belum ada, aman

                    # Convert Angka to Float
                    nums = ['qty', 'value', 'value_nett']
                    for n in nums:
                        if n in df.columns: df[n] = pd.to_numeric(df[n], errors='coerce').fillna(0.0).astype(float)

                # --- FINAL CLEANUP ---
                valid_cols = [f.name for f in active_schema if f.name in df.columns]
                df = df[valid_cols]
                schema_types = {f.name: f.field_type for f in active_schema}
                for col in df.columns:
                    if schema_types.get(col) == 'STRING':
                        df[col] = df[col].astype(str).str.strip().replace('nan', '').str.replace(r'\.0$', '', regex=True)
                        if col in ['pma', 'kode_outlet', 'fc', 'plan', 'channel']: df[col] = df[col].str.upper()

                # Preview
                if show_preview: preview_container.dataframe(df.head(20), use_container_width=True)

                # Upload Parquet
                if not df.empty:
                    temp_filename = f"part_{i}.parquet"
                    df.to_parquet(temp_filename, index=False)
                    bucket.blob(f"upload/{temp_filename}").upload_from_filename(temp_filename)
                    os.remove(temp_filename)
                    success_count += 1
                
                del df; gc.collect()
                progress_bar.progress(10 + int((i+1) / total_files * 80))
                
            except Exception as e:
                st.error(f"❌ Gagal {file.name}: {e}")

        # --- PHASE 3: FINAL LOAD ---
        if success_count > 0:
            status_text.text(f"📥 Loading ke BigQuery ({target_table})...")
            
            # Logic Append vs Truncate
            write_action = "WRITE_TRUNCATE"
            if selected_mode == "📚 Cicil History Data":
                write_action = "WRITE_APPEND" # Karena append, tadi di atas ada collision check

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=write_action,
                schema=active_schema,
                autodetect=False 
            )
            
            try:
                load_job = bq_client.load_table_from_uri(f"gs://{BUCKET_NAME}/upload/*.parquet", table_ref, job_config=job_config)
                load_job.result()
                
                # Cleanup
                for blob in bucket.list_blobs(prefix="upload/"): blob.delete()
                
                progress_bar.progress(100)
                st.balloons()
                
                if collision_detected:
                    st.warning("⚠️ Proses Selesai. Ada file yang di-SKIP karena duplikat (Cek pesan error di atas).")
                else:
                    if write_action == "WRITE_APPEND":
                        st.success(f"🎉 SUKSES! Data berhasil DITAMBAHKAN ke `{target_table}`.")
                    else:
                        st.success(f"🎉 SUKSES! Data `{target_table}` berhasil DI-REPLACE (TIMPA).")
                    
            except Exception as e:
                st.error(f"BigQuery Error: {e}")
        else:
            if collision_detected:
                st.error("⛔ Proses Dihentikan. Semua file terdeteksi Duplikat.")
            else:
                st.warning("Tidak ada data yang diproses.")

