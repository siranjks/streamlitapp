import streamlit as st

st.title("My First Streamlit App")
st.header("Welcome, Supervisor!")
st.write("This app is running live from GitHub.")

name = st.text_input("Enter your name:")
if name:
    st.write(f"Hello {name}, thanks for visiting my app!")
