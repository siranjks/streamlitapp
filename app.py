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

# Custom CSS for a clean, professional look
st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { height: 50px; border-radius: 4px 4px 0px 0px; padding: 10px 20px; font-weight: bold;}
    </style>
""", unsafe_allow_html=True)

MASTER_COLUMNS = [
    "Declaration Date", "Importer Name(EN)", "Importer ID", "Importer Country(EN)",
    "Exporter Name(EN)", "Exporter ID", "Exporter Country(EN)", "HS Code",
    "HSCode Description", "Product(EN)", "Product Description", "Product Category",
    "Quantity", "Quantity Unit(EN)", "Total Price(USD)", "Unit Price(USD)"
]

DB_NAME = 'market_intelligence_v4.db'

# --- 2. API & DATABASE SETUP ---
try:
    GROQ_API_KEY = st.secrets["GROQ_API_KEY"]
except:
    GROQ_API_KEY = st.sidebar.text_input("Enter Groq API Key", type="password")

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

# --- 3. GROQ AI ENGINES ---
def aggressive_ai_clean(company_list):
    """Uses Groq to aggressively hunt down typos and group companies."""
    if not GROQ_API_KEY or not company_list: return {}
    client = Groq(api_key=GROQ_API_KEY)
    
    prompt = f"""
    You are an expert data cleaner. Analyze this list of messy company names.
    Identify typos (e.g., 'ronde', 'keysight:', 'anritsu corp') and group them into a SINGLE, clean, lowercase master entity name.
    Output ONLY a valid JSON dictionary mapping the messy original name to the clean master name.
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
    except Exception as e:
        st.error(f"Groq API Error during cleaning: {e}")
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
    
    # -- SIDEBAR MENU & RELIABLE BACKUP --
    # FIXED: Using clean, raw URL string to prevent formatting crashes
    st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/c/cb/Rohde_%26_Schwarz_logo.svg/2560px-Rohde_%26_Schwarz_logo.svg.png", width=150)
    st.sidebar.title("System Controls")
    
    st.sidebar.divider()
    st.sidebar.subheader("💾 Database Backup")
    st.sidebar.caption("Download your secure SQLite vault.")
    
    # Bulletproof Database Download
    if os.path.exists(DB_NAME):
        with open(DB_NAME, "rb") as db_file:
            db_bytes = db_file.read()
            st.sidebar.download_button(
                label="📥 Download Master Database (.db)",
                data=db_bytes,
                file_name=f"RS_Market_DB_{datetime.datetime.now().strftime('%Y%m%d')}.db",
                mime="application/x-sqlite3"
            )

    # -- TABS --
    tab1, tab2, tab3 = st.tabs(["📥 1. Intelligence Pipeline", "📊 2. Strategic Dashboard", "🔧 3. Database Admin"])

    # --- TAB 1: INGESTION ---
    with tab1:
        st.header("Automated Ingestion Pipeline (Groq Powered)")
        st.markdown("Upload your Excel sheets. Groq AI will resolve typos, map models, and deduplicate records.")
        
        with st.container(border=True):
            col1, col2 = st.columns(2)
            raw_files = col1.file_uploader("1. Upload Raw Data (Excel/CSV)", accept_multiple_files=True)
            model_file = col2.file_uploader("2. Upload Model Database (Excel)", type=['xlsx'])
            
            if st.button("🚀 Execute Pipeline", type="primary") and raw_files and model_file:
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
                    
                    # 15 Columns Setup
                    available_cols = [c for c in df.columns if c in MASTER_COLUMNS]
                    df = df[available_cols].copy()
                    for missing in [c for c in MASTER_COLUMNS if c not in df.columns]:
                        df[missing] = ""
                    df = df[MASTER_COLUMNS]
                    
                    df['Total Price(USD)'] = pd.to_numeric(df['Total Price(USD)'], errors='coerce').fillna(0.0)
                    df = df[df['Total Price(USD)'] > 0] 
                    
                    # Apply manual model rules
                    df = apply_model_db_rules(df, model_db)
                    
                    # GROQ AI TYPO FIXING (Chunked to prevent token overflow)
                    with st.spinner(f"Groq analyzing rows and repairing company names in {file.name}..."):
                        unique_exporters = df['Exporter Name(EN)'].dropna().astype(str).unique().tolist()
                        exp_map = {}
                        # Process in chunks of 50 to keep JSON output clean and fast
                        for i in range(0, len(unique_exporters), 50):
                            chunk = unique_exporters[i:i+50]
                            chunk_map = aggressive_ai_clean(chunk)
                            if chunk_map: exp_map.update(chunk_map)
                        
                        if exp_map: 
                            df['Exporter Name(EN)'] = df['Exporter Name(EN)'].replace(exp_map)
                    
                    # Force final standardization
                    df['Exporter Name(EN)'] = df['Exporter Name(EN)'].astype(str).str.lower().str.strip()
                    
                    # Categorize SAM
                    df['Competitor_Check'] = "SAM"
                    df.loc[df['Exporter Name(EN)'].str.contains("rohde|r&s", na=False), 'Competitor_Check'] = "Ours"
                    df.loc[df['Product Category'] == "Uncategorized", 'Competitor_Check'] = "Non-SAM"
                    
                    # Hash & Deduplicate
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
                st.success(f"✅ Data Cleaned and Stored! New Records: {total_added} | Duplicates Skipped: {total_skipped}")

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
            st.info("Database is empty. Execute the pipeline in Tab 1 first.")
        else:
            df['Total Price(USD)'] = pd.to_numeric(df['Total Price(USD)'], errors='coerce').fillna(0)
            
            # KPI Metrics
            m1, m2, m3 = st.columns(3)
            m1.metric("Market Value (USD)", f"${df['Total Price(USD)'].sum():,.0f}")
            m2.metric("Total Transactions", len(df))
            m3.metric("Tracked Competitors", df['Exporter Name(EN)'].nunique())
            
            st.divider()
            
            # Interactive Visuals
            exporters = st.multiselect("Filter Competitor View", df['Exporter Name(EN)'].unique())
            plot_df = df[df['Exporter Name(EN)'].isin(exporters)] if exporters else df

            col1, col2 = st.columns(2)
            with col1:
                fig1 = px.pie(plot_df, values='Total Price(USD)', names='Exporter Name(EN)', title="Market Share Revenue", hole=0.4)
                st.plotly_chart(fig1, use_container_width=True)
            with col2:
                fig2 = px.bar(plot_df.groupby('Product Category')['Total Price(USD)'].sum().reset_index(), 
                              x='Product Category', y='Total Price(USD)', title="Revenue by Hardware Category", color='Product Category')
                st.plotly_chart(fig2, use_container_width=True)

            # --- GROQ STRATEGY GENERATOR ---
            with st.expander("🤖 Generate Groq AI Strategy Report", expanded=True):
                user_query = st.text_input("Enter tactical focus (e.g., 'How do we combat Anritsu in this dataset?'):")
                
                if st.button("Generate & Download PDF") and GROQ_API_KEY:
                    with st.spinner("Groq is analyzing patterns and drafting your PDF..."):
                        client = Groq(api_key=GROQ_API_KEY)
                        summary = plot_df.groupby(['Exporter Name(EN)', 'Product Category'])['Total Price(USD)'].sum().to_string()
                        
                        prompt = f"Data: {summary}\nFocus: {user_query}\nWrite a professional, 3-point tactical sales strategy report for Rohde & Schwarz. Format clearly."
                        
                        try:
                            response = client.chat.completions.create(
                                model="llama-3.3-70b-versatile",
                                messages=[{"role": "user", "content": prompt}],
                                temperature=0.3
                            )
                            report_text = response.choices[0].message.content
                            st.markdown(report_text)
                            
                            # Bulletproof PDF Generation
                            pdf = FPDF()
                            pdf.add_page()
                            pdf.set_font("helvetica", 'B', 16)
                            pdf.cell(0, 10, "R&S Tactical Strategy Report", new_x="LMARGIN", new_y="NEXT", align='C')
                            pdf.ln(5)
                            pdf.set_font("helvetica", size=11)
                            
                            # Encode properly to prevent crash on weird characters
                            safe_text = report_text.encode('latin-1', 'replace').decode('latin-1')
                            pdf.multi_cell(0, 8, txt=safe_text)
                            
                            # Use Bytes to ensure a perfect binary file stream for Streamlit Cloud
                            pdf_bytes = bytes(pdf.output())
                            
                            st.download_button(
                                label="📥 Download PDF Strategy", 
                                data=pdf_bytes, 
                                file_name="RS_Market_Strategy.pdf", 
                                mime="application/pdf", 
                                type="primary"
                            )
                        except Exception as e:
                            st.error(f"Failed to generate report: {e}")

    # --- TAB 3: ADMIN ---
    with tab3:
        st.header("Master SQL Override")
        st.caption("Editing data here instantly overwrites the backend SQLite database.")
        conn = sqlite3.connect(DB_NAME)
        try:
            df_admin = pd.read_sql("SELECT * FROM TradeData", conn)
            # Direct edit table
            edited_df = st.data_editor(df_admin, num_rows="dynamic", use_container_width=True, height=500)
            
            # Action Buttons
            colA, colB = st.columns([1, 4])
            with colA:
                if st.button("💾 Overwrite Master DB", type="primary"):
                    edited_df.to_sql('TradeData', conn, if_exists='replace', index=False)
                    st.success("Database overwritten.")
            with colB:
                # Add a CSV download of the cleaned data
                csv = edited_df.to_csv(index=False).encode('utf-8')
                st.download_button("📥 Export Clean Data (CSV)", data=csv, file_name="cleaned_tendata.csv", mime="text/csv")
        except:
            st.warning("No data available.")
        conn.close()

if __name__ == "__main__":
    main()
