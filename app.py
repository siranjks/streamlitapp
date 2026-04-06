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

# --- 1. INITIAL CONFIG ---
st.set_page_config(page_title="R&S Intelligence Hub", layout="wide", page_icon="🌐")

MASTER_COLUMNS = [
    "Declaration Date", "Importer Name(EN)", "Importer ID", "Importer Country(EN)",
    "Exporter Name(EN)", "Exporter ID", "Exporter Country(EN)", "HS Code",
    "HSCode Description", "Product(EN)", "Product Description", "Product Category",
    "Quantity", "Quantity Unit(EN)", "Total Price(USD)", "Unit Price(USD)"
]

DB_NAME = 'market_intelligence.db'

# --- 2. AI & DATABASE SETUP ---
GROQ_API_KEY = st.sidebar.text_input("Enter Groq API Key", type="password")

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    # Create columns dynamically based on MASTER_COLUMNS plus our system columns
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
    """Creates a unique fingerprint for a row to prevent duplicates."""
    data_string = f"{str(row.get('Declaration Date',''))}{str(row.get('Importer Name(EN)',''))}{str(row.get('Exporter Name(EN)',''))}{str(row.get('Product Description',''))}{str(row.get('Total Price(USD)',''))}"
    return hashlib.md5(data_string.encode()).hexdigest()

def ai_normalize_companies(exporters_list):
    """Uses Groq to fix mismatched company names (e.g., 'Keysight Inc' -> 'keysight')."""
    if not GROQ_API_KEY or not exporters_list: return {}
    client = Groq(api_key=GROQ_API_KEY)
    prompt = f"""
    Analyze this list of company names. Group identical companies that have slight spelling variations (e.g., 'rohde & schwarz', 'rohde and schwarz gmbh').
    Return ONLY a valid JSON object mapping the original name to a standardized, lowercase base name.
    List: {exporters_list}
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

def generate_ai_strategy(prompt_text):
    if not GROQ_API_KEY: return "Please enter Groq API Key."
    client = Groq(api_key=GROQ_API_KEY)
    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt_text}],
            temperature=0.3
        )
        return completion.choices[0].message.content
    except Exception as e:
        return f"AI Error: {e}"

# --- 3. PROCESSING LOGIC ---
def apply_model_db_rules(df, model_db):
    """Applies your colleague's exact matching rules using vectorized Pandas (Fast)."""
    if 'Product Category' not in df.columns:
        df['Product Category'] = "Uncategorized"
        
    if 'Model' in model_db.columns and 'Product_Type' in model_db.columns:
        for _, row in model_db.dropna(subset=['Model', 'Product_Type']).iterrows():
            model_name = str(row['Model']).strip()
            category = str(row['Product_Type']).split('|')[0].strip()
            # Regex boundary match
            pattern = r'(?<![A-Za-z])' + re.escape(model_name)
            mask = df['Product Description'].str.contains(pattern, case=False, na=False, regex=True)
            df.loc[mask, 'Product Category'] = category
    return df

# --- 4. MAIN APP ---
def main():
    init_db()
    
    st.sidebar.title("🛠 SDR Command Center")
    menu = ["1. Ingest & Clean", "2. Market Insights", "3. Database Admin"]
    choice = st.sidebar.radio("Navigation", menu)

    # --- PAGE 1: INGESTION ---
    if choice == "1. Ingest & Clean":
        st.title("📥 Automated Data Pipeline")
        st.write("Upload Raw Tendata + Colleague's Model Database. The AI handles the rest.")
        
        col1, col2 = st.columns(2)
        raw_files = col1.file_uploader("1. Upload Raw Data (Excel/CSV)", accept_multiple_files=True)
        model_file = col2.file_uploader("2. Upload Model Database (Excel)", type=['xlsx'])
        
        if st.button("🚀 Run Full Intelligence Pipeline") and raw_files and model_file:
            model_db = pd.read_excel(model_file)
            conn = sqlite3.connect(DB_NAME)
            
            try:
                existing_hashes = set(pd.read_sql("SELECT Row_Hash FROM TradeData", conn)['Row_Hash'].tolist())
            except:
                existing_hashes = set()

            total_added, total_skipped = 0, 0
            
            for file in raw_files:
                st.subheader(f"Processing: {file.name}")
                df = pd.read_excel(file) if file.name.endswith('.xlsx') else pd.read_csv(file)
                
                # 1. Map to Master 15 Columns
                available_cols = [c for c in df.columns if c in MASTER_COLUMNS]
                df = df[available_cols].copy()
                for missing in [c for c in MASTER_COLUMNS if c not in df.columns]:
                    df[missing] = "" # Fill missing master columns with blanks
                
                df = df[MASTER_COLUMNS] # Enforce strict order
                
                # 2. Lowercase Target Columns for Matching
                for col in ["Importer Name(EN)", "Exporter Name(EN)"]:
                    df[col] = df[col].astype(str).str.lower().str.strip()
                
                # 3. Drop Missing criticals
                df['Total Price(USD)'] = pd.to_numeric(df['Total Price(USD)'], errors='coerce').fillna(0.0)
                df = df[df['Total Price(USD)'] > 0] 
                
                # 4. Colleague's Rule Matching
                with st.spinner("Applying Model DB Rules..."):
                    df = apply_model_db_rules(df, model_db)
                
                # 5. Groq AI Company Normalization (Batched to save tokens)
                with st.spinner("AI standardizing competitor names..."):
                    unique_exporters = df['Exporter Name(EN)'].dropna().unique().tolist()
                    # Grab top 50 Exporters by frequency to avoid massive API payloads
                    top_exporters = df['Exporter Name(EN)'].value_counts().head(50).index.tolist()
                    name_map = ai_normalize_companies(top_exporters)
                    if name_map:
                        df['Exporter Name(EN)'] = df['Exporter Name(EN)'].replace(name_map)
                
                # 6. Competitor Check (SAM/Non-SAM)
                df['Competitor_Check'] = "SAM"
                df.loc[df['Exporter Name(EN)'].str.contains("rohde|r&s", na=False), 'Competitor_Check'] = "Ours"
                df.loc[df['Product Category'] == "Uncategorized", 'Competitor_Check'] = "Non-SAM"
                
                # 7. Deduplication Engine
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
            st.success(f"Pipeline Complete! Added {total_added} new records. Skipped {total_skipped} duplicates.")

    # --- PAGE 2: DASHBOARD ---
    elif choice == "2. Market Insights":
        st.title("📊 Strategic Market Analysis")
        conn = sqlite3.connect(DB_NAME)
        try:
            df = pd.read_sql("SELECT * FROM TradeData", conn)
        except:
            df = pd.DataFrame()
        conn.close()

        if df.empty:
            st.warning("Database empty. Please run the Ingestion Pipeline first.")
        else:
            df['Total Price(USD)'] = pd.to_numeric(df['Total Price(USD)'], errors='coerce').fillna(0)
            
            # Interactive Filters
            exporters = st.multiselect("Filter Competitors", df['Exporter Name(EN)'].unique())
            plot_df = df[df['Exporter Name(EN)'].isin(exporters)] if exporters else df

            col1, col2 = st.columns(2)
            with col1:
                fig1 = px.bar(plot_df.groupby('Exporter Name(EN)')['Total Price(USD)'].sum().reset_index(), 
                              x='Exporter Name(EN)', y='Total Price(USD)', title="Competitor Revenue (USD)")
                st.plotly_chart(fig1, use_container_width=True)
            with col2:
                fig2 = px.pie(plot_df, values='Total Price(USD)', names='Product Category', title="Revenue by Category")
                st.plotly_chart(fig2, use_container_width=True)

            st.divider()
            st.subheader("🕵️ AI Strategy Consultant")
            user_query = st.text_input("Ask the AI a specific question based on this data:")
            
            if st.button("Generate Strategy PDF"):
                summary = plot_df.groupby(['Exporter Name(EN)', 'Product Category'])['Total Price(USD)'].sum().to_string()
                prompt = f"Data Summary: {summary}\nFocus: {user_query}\nWrite a 3-point tactical sales strategy report for Rohde & Schwarz."
                report_text = generate_ai_strategy(prompt)
                
                st.write(report_text)
                
                # Clean PDF Output
                pdf = FPDF()
                pdf.add_page()
                pdf.set_font("helvetica", 'B', 16)
                pdf.cell(0, 10, "Tactical Market Strategy Report", new_x="LMARGIN", new_y="NEXT", align='C')
                pdf.ln(10)
                pdf.set_font("helvetica", size=11)
                pdf.multi_cell(0, 10, txt=report_text.encode('latin-1', 'replace').decode('latin-1'))
                
                st.download_button("📥 Download Report (PDF)", data=bytes(pdf.output()), file_name="R&S_Strategy.pdf", mime="application/pdf")

    # --- PAGE 3: ADMIN ---
    elif choice == "3. Database Admin":
        st.title("🔧 Master SQL Viewer")
        conn = sqlite3.connect(DB_NAME)
        try:
            df_admin = pd.read_sql("SELECT * FROM TradeData", conn)
            # Display all 15 columns plus metadata
            edited_df = st.data_editor(df_admin, num_rows="dynamic", use_container_width=True)
            if st.button("💾 Hard Save Changes to Database"):
                edited_df.to_sql('TradeData', conn, if_exists='replace', index=False)
                st.success("Master database updated.")
        except:
            st.warning("No data available to edit.")
        conn.close()

if __name__ == "__main__":
    main()
