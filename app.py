import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import json
import requests

def process_uploaded_file(file):
    """Processes the uploaded file based on its format."""
    try:
        file_extension = file.name.split('.')[-1]

        if file_extension == 'csv':
            df = pd.read_csv(file)
            return df
        elif file_extension == 'json':
            df = pd.read_json(file)
            return df
        else:
            raise ValueError("Unsupported file format")
    except Exception as error:
        st.error(error)
        return None

def clean_data(df):
    """Clean and preprocess the data."""
    df = df.drop_duplicates()
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='ignore')
    numeric_cols = df.select_dtypes(include='number').columns
    df[numeric_cols] = df[numeric_cols].apply(lambda x: x.fillna(x.mean()))
    return df

def perform_eda(df):
    """Perform exploratory data analysis and visualize results."""
    st.subheader("Exploratory Data Analysis")
    st.write("### Descriptive Statistics")
    st.write(df.describe())
    st.write("### Correlation Matrix")
    corr = df.select_dtypes(include='number').corr()
    st.write(corr)
    fig, ax = plt.subplots()
    sns.heatmap(corr, annot=True, ax=ax)
    st.pyplot(fig)

def calculate_production_volumes(df):
    return df.groupby('energy_source')['production_volume'].sum()

def analyze_consumption_patterns(df):
    return df.groupby(['region', 'sector'])['consumption_volume'].sum()

def identify_major_energy_sources(df):
    production = df.groupby('energy_source')['production_volume'].sum().sort_values(ascending=False)
    consumption = df.groupby('energy_source')['consumption_volume'].sum().sort_values(ascending=False)
    return production, consumption

def analyze_import_export_dynamics(df):
    imports = df.groupby('country')['import_volume'].sum().sort_values(ascending=False)
    exports = df.groupby('country')['export_volume'].sum().sort_values(ascending=False)
    return imports, exports

def calculate_price_trends(df):
    return df.groupby(['date', 'energy_source'])['price'].mean().unstack()

def compare_renewable_nonrenewable(df):
    renewable = df[df['energy_source_type'] == 'renewable']['production_volume'].sum()
    non_renewable = df[df['energy_source_type'] == 'non_renewable']['production_volume'].sum()
    return renewable, non_renewable

def assess_geopolitical_impact(df, event_column):
    return df.groupby(event_column)['trade_volume'].sum()

def display_analysis_results(df):
    st.subheader("Energy Trade Analysis Worldwide")

    # Display production and consumption volumes side by side
    col1, col2 = st.columns(2)

    with col1:
        st.write("### Total Production Volumes")
        production_volumes = calculate_production_volumes(df)
        st.write(production_volumes)
        fig, ax = plt.subplots()
        production_volumes.plot(kind='bar', ax=ax)
        st.pyplot(fig)

    with col2:
        st.write("### Consumption Patterns")
        consumption_patterns = analyze_consumption_patterns(df)
        st.write(consumption_patterns)
        fig = px.bar(consumption_patterns.reset_index(), x='region', y='consumption_volume', color='sector', title="Consumption Patterns by Region and Sector")
        st.plotly_chart(fig)

    st.write("### Major Energy Sources")
    production, consumption = identify_major_energy_sources(df)
    col1, col2 = st.columns(2)
    with col1:
        st.write("Production:")
        st.write(production)
        fig, ax = plt.subplots()
        production.plot(kind='bar', ax=ax, color='blue', alpha=0.7, label='Production')
        st.pyplot(fig)
    with col2:
        st.write("Consumption:")
        st.write(consumption)
        fig, ax = plt.subplots()
        consumption.plot(kind='bar', ax=ax, color='red', alpha=0.7, label='Consumption')
        st.pyplot(fig)

    st.write("### Import/Export Dynamics")
    imports, exports = analyze_import_export_dynamics(df)
    col1, col2 = st.columns(2)
    with col1:
        st.write("Imports:")
        st.write(imports)
        fig, ax = plt.subplots()
        imports.plot(kind='bar', ax=ax, color='green', alpha=0.7, label='Imports')
        st.pyplot(fig)
    with col2:
        st.write("Exports:")
        st.write(exports)
        fig, ax = plt.subplots()
        exports.plot(kind='bar', ax=ax, color='purple', alpha=0.7, label='Exports')
        st.pyplot(fig)

    st.write("### Price Trends")
    price_trends = calculate_price_trends(df)
    st.write(price_trends)
    fig = px.line(price_trends, title="Price Trends Over Time")
    st.plotly_chart(fig)

    st.write("### Renewable vs Non-renewable Energy")
    renewable, non_renewable = compare_renewable_nonrenewable(df)
    col1, col2 = st.columns(2)
    with col1:
        st.write("Renewable Energy Production:", renewable)
    with col2:
        st.write("Non-renewable Energy Production:", non_renewable)
    fig, ax = plt.subplots()
    ax.bar(['Renewable', 'Non-renewable'], [renewable, non_renewable], color=['green', 'grey'])
    st.pyplot(fig)

    st.write("### Geopolitical Impact")
    event_column = 'geopolitical_event'  # Example event column
    geopolitical_impact = assess_geopolitical_impact(df, event_column)
    st.write(geopolitical_impact)
    fig = px.bar(geopolitical_impact, title="Geopolitical Impact on Trade Volumes")
    st.plotly_chart(fig)

def send_post_request(url, data_chunk):
    try:
        response = requests.post(url, json=data_chunk)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error: {e}")
        return None

def send_data_in_chunks(url, data, chunk_size=500):
    num_chunks = len(data) // chunk_size + (1 if len(data) % chunk_size > 0 else 0)
    for i in range(num_chunks):
        chunk = data[i * chunk_size:(i + 1) * chunk_size]
        response = send_post_request(url, chunk)
        if response:
            st.success(f"Chunk {i + 1}/{num_chunks} submitted successfully.")
        else:
            st.error(f"Failed to submit chunk {i + 1}/{num_chunks}.")
            break

def main():
    st.markdown(
        "<h1 style='color: Green'>Energy Trade Analysis Worldwide</h1>",
        unsafe_allow_html=True
    )

    st.markdown(
        "<h2 style='color:PowderBlue;'>CHOOSE A FILE</h2>",
        unsafe_allow_html=True
    )
    uploaded_file = st.file_uploader("Choose a file", type=["csv", "json"], label_visibility="collapsed")
    if uploaded_file is not None:
        df = process_uploaded_file(uploaded_file)
        if df is not None:
            st.subheader("Raw Data")
            st.write(df)

            st.info(f"Total records in raw data: {len(df)}")

            cleaned_data = clean_data(df)

            st.subheader("Cleaned Data")
            st.write(cleaned_data)

            st.info(f"Total records in cleaned data: {len(cleaned_data)}")

            perform_eda(cleaned_data)
            display_analysis_results(cleaned_data)

            csv = cleaned_data.to_csv(index=False)
            st.download_button(
                label="Download Cleaned Data as CSV",
                data=csv,
                file_name="cleaned_data.csv",
                mime="text/csv",
            )

            json_data = json.loads(cleaned_data.to_json(orient='records'))
            url = "https://7a49mfw9t0.execute-api.ap-south-1.amazonaws.com/myfunction"
            send_data_in_chunks(url, json_data, chunk_size=500)  # Adjust chunk_size as needed

            st.success("Application ran successfully!")

if __name__ == "__main__":
    main()
