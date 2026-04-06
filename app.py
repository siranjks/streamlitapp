import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
from groq import Groq
from fpdf import FPDF
import hashlib
import datetime
import re
import json
import os

# --- 1. INITIAL CONFIG & UI OVERHAUL ---
st.set_page_config(page_title="R&S Intelligence Hub", layout="wide", page_icon="🌐")

# Custom CSS for a cleaner, professional look
st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { height: 50px; white-space: pre-wrap; border-radius: 4px 4px 0px 0px; padding: 10px 20px; }
    </style>
""", unsafe_allow_html=True)

MASTER_COLUMNS = [
    "Declaration Date", "Importer Name(EN)", "Importer ID", "Importer Country(EN)",
    "Exporter Name(EN)", "Exporter ID", "Exporter Country(EN)", "HS Code",
    "HSCode Description", "Product(EN)", "Product Description", "Product Category",
    "Quantity", "Quantity Unit(EN)", "Total Price(USD)", "Unit Price(USD)"
]

DB_NAME = 'market_intelligence_v3.db'

# --- 2. SECRETS & DATABASE SETUP ---
# Pulls silently from Streamlit Secrets. Fallback to UI input if not set.
try:
    GROQ_API_KEY = st.secrets["GROQ_API_KEY"]
except:
    GROQ_API_KEY = st.sidebar.text_input("Enter Groq API Key (Or set in App Secrets)", type="password")

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    cols_sql = ", ".join([f'"{col}" TEXT' for col in MASTER_COLUMNS])
    c.execute(f'''CREATE TABLE IF NOT EXISTS TradeData 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                  Row_Hash TEXT UNIQUE,
                  Upload_Date TEXT,
                  Competitor_Check TEXT,
                  {cols_sql})''')
    conn.commit()
    conn.close()

def generate_row_hash(row):
    data_string = f"{str(row.get('Declaration Date',''))}{str(row.get('Importer Name(EN)',''))}{str(row.get('Exporter Name(EN)',''))}{str(row.get('Product Description',''))}{str(row.get('Total Price(USD)',''))}"
    return hashlib.md5(data_string.encode()).hexdigest()

# --- 3. AGGRESSIVE AI TYPO CLEANING ---
def aggressive_ai_clean(company_list):
    """Deep cleaning AI prompt to catch typos like 'ronde', colons, and weird suffixes."""
    if not GROQ_API_KEY or not company_list: return {}
    client = Groq(api_key=GROQ_API_KEY)
    
    prompt = f"""
    You are an expert data analyst cleaning customs data. 
    Review this list of messy company names. Identify severe typos (e.g., 'ronde and schwarz', 'keysight:'), remove trailing punctuation, and group variations under a single, clean, lowercase master name.
    Pay special attention to fixing known brands like 'rohde & schwarz', 'keysight', 'anritsu', 'tektronix'.
    Return ONLY a JSON dictionary where keys are the messy original names and values are the clean master names. Do not include markdown formatting.
    List: {company_list}
    """
    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.1
        )
        return json.loads(completion.choices[0].message.content)
    except:
        return {}

def apply_model_db_rules(df, model_db):
    if 'Product Category' not in df.columns:
        df['Product Category'] = "Uncategorized"
    if 'Model' in model_db.columns and 'Product_Type' in model_db.columns:
        for _, row in model_db.dropna(subset=['Model', 'Product_Type']).iterrows():
            model_name = str(row['Model']).strip()
            category = str(row['Product_Type']).split('|')[0].strip()
            pattern = r'(?<![A-Za-z])' + re.escape(model_name)
            mask = df['Product Description'].str.contains(pattern, case=False, na=False, regex=True)
            df.loc[mask, 'Product Category'] = category
    return df

# --- 4. MAIN APP ---
def main():
    init_db()
    
    # -- SIDEBAR MENU & BACKUP --
    st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/c/cb/Rohde_%26_Schwarz_logo.svg/2560px-Rohde_%26_Schwarz_logo.svg.png", width=150)
    st.sidebar.title("Data Controls")
    
    # Download DB Feature
    st.sidebar.divider()
    st.sidebar.subheader("💾 Database Backup")
    st.sidebar.caption("Streamlit Cloud resets occasionally. Download your database here to save your data safely.")
    if os.path.exists(DB_NAME):
        with open(DB_NAME, "rb") as file:
            st.sidebar.download_button(
                label="📥 Download Master Database (.db)",
                data=file,
                file_name=f"RS_Market_DB_{datetime.datetime.now().strftime('%Y%m%d')}.db",
                mime="application/octet-stream"
            )

    # -- PROFESSIONAL UI TABS --
    tab1, tab2, tab3 = st.tabs(["📥 1. Ingest & Clean Pipeline", "📊 2. Strategic Dashboard", "🔧 3. Database Admin"])

    # --- TAB 1: INGESTION ---
    with tab1:
        st.header("Automated Data Ingestion")
        st.markdown("Upload raw files. The AI will remove duplicates, fix severe typos (e.g., 'Ronde:', 'keysight corp'), and categorize models.")
        
        with st.container(border=True):
            col1, col2 = st.columns(2)
            raw_files = col1.file_uploader("1. Upload Raw Data (Excel/CSV)", accept_multiple_files=True)
            model_file = col2.file_uploader("2. Upload Model Database (Excel)", type=['xlsx'])
            
            if st.button("🚀 Execute Intelligence Pipeline", type="primary") and raw_files and model_file:
                model_db = pd.read_excel(model_file)
                conn = sqlite3.connect(DB_NAME)
                
                try:
                    existing_hashes = set(pd.read_sql("SELECT Row_Hash FROM TradeData", conn)['Row_Hash'].tolist())
                except:
                    existing_hashes = set()

                total_added, total_skipped = 0, 0
                
                for file in raw_files:
                    st.toast(f"Processing {file.name}...")
                    df = pd.read_excel(file) if file.name.endswith('.xlsx') else pd.read_csv(file)
                    
                    available_cols = [c for c in df.columns if c in MASTER_COLUMNS]
                    df = df[available_cols].copy()
                    for missing in [c for c in MASTER_COLUMNS if c not in df.columns]:
                        df[missing] = ""
                    df = df[MASTER_COLUMNS]
                    
                    # Basic Clean & Drop Zero Value
                    df['Total Price(USD)'] = pd.to_numeric(df['Total Price(USD)'], errors='coerce').fillna(0.0)
                    df = df[df['Total Price(USD)'] > 0] 
                    
                    # Colleague Logic
                    df = apply_model_db_rules(df, model_db)
                    
                    # AI TYPO FIXING (Exporters AND Importers)
                    with st.spinner(f"AI repairing shitty company names in {file.name}..."):
                        # Fix Exporters
                        top_exporters = df['Exporter Name(EN)'].dropna().astype(str).unique().tolist()[:60]
                        exp_map = aggressive_ai_clean(top_exporters)
                        if exp_map: df['Exporter Name(EN)'] = df['Exporter Name(EN)'].replace(exp_map)
                        
                        # Fix Importers
                        top_importers = df['Importer Name(EN)'].dropna().astype(str).unique().tolist()[:60]
                        imp_map = aggressive_ai_clean(top_importers)
                        if imp_map: df['Importer Name(EN)'] = df['Importer Name(EN)'].replace(imp_map)
                    
                    # Final Normalization
                    df['Exporter Name(EN)'] = df['Exporter Name(EN)'].astype(str).str.lower().str.strip()
                    
                    # Competitor Check
                    df['Competitor_Check'] = "SAM"
                    df.loc[df['Exporter Name(EN)'].str.contains("rohde|r&s", na=False), 'Competitor_Check'] = "Ours"
                    df.loc[df['Product Category'] == "Uncategorized", 'Competitor_Check'] = "Non-SAM"
                    
                    valid_rows = []
                    for _, row in df.iterrows():
                        row_hash = generate_row_hash(row)
                        if row_hash in existing_hashes:
                            total_skipped += 1
                        else:
                            existing_hashes.add(row_hash)
                            row_dict = row.to_dict()
                            row_dict['Row_Hash'] = row_hash
                            row_dict['Upload_Date'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                            valid_rows.append(row_dict)
                    
                    if valid_rows:
                        pd.DataFrame(valid_rows).to_sql('TradeData', conn, if_exists='append', index=False)
                        total_added += len(valid_rows)
                
                conn.close()
                st.success(f"✅ Pipeline Complete! Added: {total_added} | Duplicates Skipped: {total_skipped}")

    # --- TAB 2: DASHBOARD ---
    with tab2:
        st.header("Strategic Market Intelligence")
        conn = sqlite3.connect(DB_NAME)
        try:
            df = pd.read_sql("SELECT * FROM TradeData", conn)
        except:
            df = pd.DataFrame()
        conn.close()

        if df.empty:
            st.info("Database empty. Upload data to view analytics.")
        else:
            df['Total Price(USD)'] = pd.to_numeric(df['Total Price(USD)'], errors='coerce').fillna(0)
            
            # KPI Metrics
            st.subheader("Market KPIs")
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Market Value (USD)", f"${df['Total Price(USD)'].sum():,.2f}")
            m2.metric("Total Transactions", len(df))
            m3.metric("Unique Exporters", df['Exporter Name(EN)'].nunique())
            
            st.divider()
            
            # Visuals
            exporters = st.multiselect("Filter by Competitor", df['Exporter Name(EN)'].unique())
            plot_df = df[df['Exporter Name(EN)'].isin(exporters)] if exporters else df

            col1, col2 = st.columns(2)
            with col1:
                fig1 = px.pie(plot_df, values='Total Price(USD)', names='Exporter Name(EN)', title="Market Share by Revenue", hole=0.3)
                st.plotly_chart(fig1, use_container_width=True)
            with col2:
                fig2 = px.bar(plot_df.groupby('Product Category')['Total Price(USD)'].sum().reset_index(), 
                              x='Product Category', y='Total Price(USD)', title="Revenue by Product Category")
                st.plotly_chart(fig2, use_container_width=True)

            # Strategy Generator
            with st.expander("🤖 Generate AI Strategy Report", expanded=False):
                user_query = st.text_input("What is your tactical focus? (e.g., Target demographics, beating Anritsu)")
                if st.button("Generate PDF Report") and GROQ_API_KEY:
                    with st.spinner("Analyzing data and writing report..."):
                        client = Groq(api_key=GROQ_API_KEY)
                        summary = plot_df.groupby(['Exporter Name(EN)', 'Product Category'])['Total Price(USD)'].sum().to_string()
                        prompt = f"Data Summary: {summary}\nFocus: {user_query}\nWrite a professional, 3-point tactical sales strategy report for Rohde & Schwarz."
                        
                        try:
                            response = client.chat.completions.create(model="llama-3.3-70b-versatile", messages=[{"role": "user", "content": prompt}], temperature=0.3)
                            report_text = response.choices[0].message.content
                            st.markdown(report_text)
                            
                            pdf = FPDF()
                            pdf.add_page()
                            pdf.set_font("helvetica", 'B', 16)
                            pdf.cell(0, 10, "Tactical Market Strategy Report", new_x="LMARGIN", new_y="NEXT", align='C')
                            pdf.ln(5)
                            pdf.set_font("helvetica", size=11)
                            pdf.multi_cell(0, 8, txt=report_text.encode('latin-1', 'replace').decode('latin-1'))
                            
                            st.download_button("📥 Download PDF", data=bytes(pdf.output()), file_name="RS_Strategy.pdf", mime="application/pdf", type="primary")
                        except Exception as e:
                            st.error(f"AI Error: {e}")

    # --- TAB 3: ADMIN ---
    with tab3:
        st.header("Master SQL Viewer")
        st.write("Edit data directly. Changes override the master database.")
        conn = sqlite3.connect(DB_NAME)
        try:
            df_admin = pd.read_sql("SELECT * FROM TradeData", conn)
            edited_df = st.data_editor(df_admin, num_rows="dynamic", use_container_width=True, height=600)
            if st.button("💾 Hard Save Changes", type="primary"):
                edited_df.to_sql('TradeData', conn, if_exists='replace', index=False)
                st.success("Master database overwritten successfully.")
        except:
            st.warning("No data available.")
        conn.close()

if __name__ == "__main__":
    main()
