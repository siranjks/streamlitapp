import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
from groq import Groq
from fpdf import FPDF
import hashlib
import datetime

# --- INITIAL CONFIG ---
st.set_page_config(page_title="Intelligence Hub 2026", layout="wide", page_icon="📈")

# --- AI SETUP (GROQ) ---
# It's better to store this in Streamlit Secrets (Settings > Secrets) as: GROQ_API_KEY = "your_key"
GROQ_API_KEY = st.sidebar.text_input("Enter Groq API Key", type="password")

def call_groq(prompt):
    if not GROQ_API_KEY:
        return "ERROR: No API Key provided."
    try:
        client = Groq(api_key=GROQ_API_KEY)
        completion = client.chat.completions.create(
            model="llama3-8b-8192",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )
        return completion.choices[0].message.content
    except Exception as e:
        return f"AI Error: {e}"

# --- DATABASE & DEDUPLICATION ---
DB_NAME = 'market_intelligence.db'

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS TradeData 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                  Row_Hash TEXT UNIQUE,
                  Upload_Date TEXT,
                  Trade_Date TEXT, 
                  Importer_Name TEXT, 
                  Exporter_Name TEXT, 
                  Raw_Description TEXT, 
                  Translated_Description TEXT,
                  Category TEXT,
                  Value_USD REAL,
                  Source_File TEXT)''')
    conn.commit()
    conn.close()

def generate_row_hash(row):
    """Creates a unique ID for a row based on its core data."""
    # Ensure all parts are strings and cleaned
    data_string = f"{str(row['Trade_Date']).strip()}{str(row['Importer_Name']).strip()}{str(row['Exporter_Name']).strip()}{str(row['Raw_Description']).strip()}{str(row['Value_USD']).strip()}"
    return hashlib.md5(data_string.encode()).hexdigest()

# --- MAIN APP ---
def main():
    init_db()
    
    st.sidebar.title("🛠 Command Center")
    menu = ["Upload Data", "Market Insights", "Manage Database"]
    choice = st.sidebar.radio("Navigation", menu)

    # --- PAGE 1: UPLOAD & DEDUPLICATION ---
    if choice == "Upload Data":
        st.title("📥 Data Ingestion")
        st.write("Upload files. The system skips duplicates from the DB and within the file itself.")
        
        files = st.file_uploader("Upload Excel/CSV", accept_multiple_files=True, type=['xlsx', 'csv'])
        
        if files:
            for file in files:
                st.subheader(f"📄 Processing: {file.name}")
                df_raw = pd.read_excel(file) if file.name.endswith('.xlsx') else pd.read_csv(file)
                
                all_cols = df_raw.columns.tolist()
                with st.expander(f"Link columns for {file.name}"):
                    c1, c2, c3 = st.columns(3)
                    date_col = c1.selectbox("Date", all_cols, key=f"d_{file.name}")
                    imp_col = c2.selectbox("Importer", all_cols, key=f"i_{file.name}")
                    exp_col = c3.selectbox("Exporter", all_cols, key=f"e_{file.name}")
                    desc_col = c1.selectbox("Description", all_cols, key=f"de_{file.name}")
                    val_col = c2.selectbox("Value (USD)", all_cols, key=f"v_{file.name}")
                
                if st.button(f"Process {file.name}", key=f"btn_{file.name}"):
                    # Standardize
                    df_std = df_raw[[date_col, imp_col, exp_col, desc_col, val_col]].copy()
                    df_std.columns = ['Trade_Date', 'Importer_Name', 'Exporter_Name', 'Raw_Description', 'Value_USD']
                    
                    # Ensure Value_USD is numeric
                    df_std['Value_USD'] = pd.to_numeric(df_std['Value_USD'], errors='coerce').fillna(0.0)

                    # Get existing hashes from DB
                    conn = sqlite3.connect(DB_NAME)
                    query = "SELECT Row_Hash FROM TradeData"
                    try:
                        existing_hashes = set(pd.read_sql(query, conn)['Row_Hash'].tolist())
                    except:
                        existing_hashes = set()
                    
                    new_rows = []
                    skipped_count = 0
                    
                    progress_bar = st.progress(0)
                    total_rows = len(df_std)

                    for idx, row in df_std.iterrows():
                        row_hash = generate_row_hash(row)
                        
                        # Check against DB AND against the current batch we are building
                        if row_hash in existing_hashes:
                            skipped_count += 1
                        else:
                            # Add to tracking set so we don't add it again in this loop
                            existing_hashes.add(row_hash)
                            
                            # AI Logic
                            ai_prompt = f"Translate technical description to English & Categorize (1-word): '{row['Raw_Description']}'. Format: Translation | Category"
                            ai_resp = call_groq(ai_prompt)
                            
                            if ai_resp and "|" in ai_resp:
                                trans_val, cat_val = [x.strip() for x in ai_resp.split("|", 1)]
                            else:
                                trans_val, cat_val = row['Raw_Description'], "Uncategorized"
                            
                            new_rows.append({
                                "Row_Hash": row_hash,
                                "Upload_Date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
                                "Trade_Date": str(row['Trade_Date']),
                                "Importer_Name": str(row['Importer_Name']),
                                "Exporter_Name": str(row['Exporter_Name']),
                                "Raw_Description": str(row['Raw_Description']),
                                "Translated_Description": trans_val,
                                "Category": cat_val,
                                "Value_USD": float(row['Value_USD']),
                                "Source_File": file.name
                            })
                        
                        progress_bar.progress((idx + 1) / total_rows)

                    if new_rows:
                        try:
                            upload_df = pd.DataFrame(new_rows)
                            upload_df.to_sql('TradeData', conn, if_exists='append', index=False)
                            st.success(f"Added {len(new_rows)} new records. Skipped {skipped_count} duplicates.")
                        except Exception as e:
                            st.error(f"Database Save Error: {e}")
                    else:
                        st.warning(f"No new data to add from {file.name}.")
                    conn.close()

    # --- PAGE 2: DASHBOARD ---
    elif choice == "Market Insights":
        st.title("📊 Strategic Dashboard")
        conn = sqlite3.connect(DB_NAME)
        try:
            df = pd.read_sql("SELECT * FROM TradeData", conn)
        except:
            df = pd.DataFrame()
        conn.close()

        if df.empty:
            st.info("Database is empty.")
        else:
            exporters = st.multiselect("Select Competitors", df['Exporter_Name'].unique())
            filtered_df = df[df['Exporter_Name'].isin(exporters)] if exporters else df

            col1, col2 = st.columns(2)
            with col1:
                st.plotly_chart(px.pie(filtered_df, values='Value_USD', names='Exporter_Name', hole=0.4, title="Market Share"))
            with col2:
                st.plotly_chart(px.bar(filtered_df, x='Category', y='Value_USD', color='Exporter_Name', barmode='group', title="Category Value"))

            st.divider()
            st.subheader("💡 Strategic Sales Assistant")
            strategy_query = st.text_input("What specific strategy do you need?")
            
            if st.button("Generate Strategy Report"):
                summary = filtered_df.groupby(['Exporter_Name', 'Category'])['Value_USD'].sum().to_string()
                prompt = f"Data Summary: {summary}\nFocus: {strategy_query}\nTask: Write a 3-point tactical sales strategy report."
                report = call_groq(prompt)
                st.markdown(report)
                
                pdf = FPDF()
                pdf.add_page()
                pdf.set_font("Arial", 'B', 16)
                pdf.cell(200, 10, txt="Tactical Market Strategy Report", ln=True, align='C')
                pdf.ln(10)
                pdf.set_font("Arial", size=11)
                pdf.multi_cell(0, 10, txt=report.encode('latin-1', 'ignore').decode('latin-1'))
                
                pdf_bytes = pdf.output(dest='S')
                st.download_button("📥 Download Report (PDF)", data=pdf_bytes, file_name="Market_Strategy.pdf")

    # --- PAGE 3: ADMIN ---
    elif choice == "Manage Database":
        st.title("🔧 Database Administrator")
        conn = sqlite3.connect(DB_NAME)
        try:
            df_edit = pd.read_sql("SELECT * FROM TradeData", conn)
            edited_df = st.data_editor(df_edit, num_rows="dynamic", key="db_editor")
            if st.button("Save Changes"):
                edited_df.to_sql('TradeData', conn, if_exists='replace', index=False)
                st.success("Changes saved.")
        except:
            st.warning("No database found yet.")
        conn.close()

if __name__ == "__main__":
    main()
