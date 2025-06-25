
# <-------- PEOPLE WHO WORKED ON THIS FILE: AFNAN ---------->
# CODE OBJECTIVE: To provide a clean-looking front-end for the user

# importing streamlit and libraries
import streamlit as st
import pandas as pd

#importing function from scrape (get's matching listings from 3 marketplaces)
from scrape import process_file


# Set page config
st.set_page_config(page_title="Flywl Marketplace Scraper", layout="centered")

# CSS for styling -> makes background orange & black gradient
st.markdown("""
    <style>
            
    .stApp {
            background: linear-gradient(135deg, #000000, #ff6f00);
        }

    </style>
""", unsafe_allow_html=True)

# display an upload button
uploaded_file = st.file_uploader("Upload File", type=["xlsx", "xls"], label_visibility="collapsed")

if uploaded_file:

    st.info("Processing file, please wait...")
    with st.spinner("Scraping marketplaces..."):

        # call scraping function and pass in uploaded file
        results_df = process_file(uploaded_file)

    #  displaying dataframe of results 
    st.success("Done!")
    st.write("Results:")
    st.dataframe(results_df)

    csv = results_df.to_csv(index=True)

    # allowing user to download the csv file
    st.download_button(
            label="Download CSV Results",
            data=csv,
            file_name='marketplace_results.csv',
    )
