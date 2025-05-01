# --- Import Libraries ---
import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from datetime import datetime
import re
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from PIL import Image

# Set page config
st.set_page_config(
    page_title="Crop Protection Innovation Survey Dashboard",
    page_icon="🌱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Hide Default Streamlit Elements
hide_streamlit_style = """
    <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        .stSlider [data-baseweb="slider"] {
            padding: 0;
        }
    </style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# --- Data Loading Function ---
@st.cache_data(ttl=3600)  # Cache data for 1 hour
def load_data():
    """Load the survey data from the embedded dataset"""
    try:
        df = pd.read_excel('survey_data.xlsx')
        
        # Store total records count before any processing
        total_records = len(df)
        
        # Replace submitdate with G01Q46 contents
        df['submitdate'] = pd.to_datetime(df['G01Q46'], errors='coerce')
        
        # Remove comma separators from seed column if they exist
        if 'seed' in df.columns:
            df['seed'] = df['seed'].astype(str).str.replace(',', '')
        
        # Identify pesticide data columns
        pest_cols = [col for col in df.columns if 'G03Q19' in col]
        df = convert_pesticide_columns(df, pest_cols)
        
        # Preprocess data
        df['G00Q01'] = df['G00Q01'].str.strip()
        df['G00Q03'] = df['G00Q03'].str.strip()
        
        return df, total_records
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None, 0

# --- Data Processing Functions ---
def clean_numeric(value):
    """Convert various numeric formats to float, handling text entries"""
    if pd.isna(value):
        return np.nan
    if isinstance(value, (int, float)):
        return float(value)
    
    # Remove non-numeric characters except decimal points and negative signs
    cleaned = re.sub(r"[^\d.-]", "", str(value))
    try:
        return float(cleaned) if cleaned else np.nan
    except ValueError:
        return np.nan

def convert_pesticide_columns(df, cols):
    """Convert pesticide-related columns to numeric values"""
    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(clean_numeric)
    return df

def process_text_columns(df, columns):
    """Process text columns for analysis, handling numpy arrays"""
    all_text = []
    for col in columns:
        if col in df.columns:
            # Convert to string and handle NaN values
            text_series = df[col].astype(str).replace('nan', '')
            all_text.extend(text_series.tolist())
    return ' '.join([str(t) for t in all_text if str(t) != ''])

def generate_wordcloud(text, title, colormap='viridis'):
    """Generate and display a word cloud"""
    wordcloud = WordCloud(
        width=800,
        height=400,
        background_color='white',
        colormap=colormap,
        max_words=100,
        contour_width=1,
        contour_color='steelblue'
    ).generate(text)
    
    # Display the generated image
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.imshow(wordcloud, interpolation='bilinear')
    ax.set_title(title, fontsize=16, pad=20)
    ax.axis('off')
    st.pyplot(fig)

# --- Visualization Functions ---
def create_bar_chart(df, x_col, y_col, title, color='steelblue'):
    """Create an Altair bar chart"""
    chart = alt.Chart(df).mark_bar(color=color).encode(
        x=alt.X(f'{x_col}:Q', title=x_col),
        y=alt.Y(f'{y_col}:N', title=y_col, sort='-x')
    ).properties(
        title=title,
        width=600,
        height=400
    )
    return chart

def create_line_chart(df, x_col, y_col, color_col, title):
    """Create an Altair line chart"""
    chart = alt.Chart(df).mark_line(point=True).encode(
        x=alt.X(f'{x_col}:N', title=x_col),
        y=alt.Y(f'{y_col}:Q', title=y_col),
        color=alt.Color(f'{color_col}:N', title=color_col),
        tooltip=[x_col, y_col, color_col]
    ).properties(
        title=title,
        width=600,
        height=400
    )
    return chart

def create_word_frequency_chart(text, title):
    """Create word frequency visualization"""
    words = re.findall(r'\b\w{4,}\b', text.lower())
    word_counts = Counter(words)
    word_df = pd.DataFrame(word_counts.most_common(20), columns=['word', 'count'])
    
    chart = alt.Chart(word_df).mark_bar().encode(
        x='count:Q',
        y=alt.Y('word:N', sort='-x'),
        color=alt.Color('count:Q', scale=alt.Scale(scheme='blues'))
    ).properties(
        title=title,
        width=600,
        height=400
    )
    return chart

# --- UI Components ---
def show_kpi_cards(df, total_records):
    st.subheader("Key Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    # Calculate valid responses (non-empty)
    valid_responses = len(df)
    invalid_responses = total_records - valid_responses
    
    # Display with red font for incomplete count
    col1.markdown(f"""
    <div style="border-radius:10px; padding:10px; background-color:#f0f2f6">
        <h3 style="margin:0; padding:0">Total Responses</h3>
        <p style="margin:0; padding:0; font-size:24px">
            {valid_responses} <span style="color:red">({invalid_responses} incomplete)</span>
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    col2.metric("Countries Represented", df['G00Q01'].nunique())
    col3.metric("Regulators", len(df[df['G00Q03'] == "Regulator"]))
    col4.metric("Industry Representatives", len(df[df['G00Q03'] == "Industry"]))

def show_response_overview(df):
    st.subheader("Response Overview")
    tab1, tab2, tab3 = st.tabs(["By Country", "By Stakeholder", "Over Time"])
    
    with tab1:
        country_counts = df['G00Q01'].value_counts().reset_index()
        country_counts.columns = ['Country', 'Count']
        chart = create_bar_chart(country_counts, 'Count', 'Country', 'Responses by Country')
        st.altair_chart(chart, use_container_width=True)
    
    with tab2:
        stakeholder_counts = df['G00Q03'].value_counts().reset_index()
        stakeholder_counts.columns = ['Stakeholder', 'Count']
        chart = create_bar_chart(stakeholder_counts, 'Count', 'Stakeholder', 'Responses by Stakeholder')
        st.altair_chart(chart, use_container_width=True)
    
    with tab3:
        time_df = df.set_index('submitdate').resample('W').size().reset_index(name='counts')
        time_df.columns = ['Date', 'Count']
        chart = alt.Chart(time_df).mark_line().encode(
            x='Date:T',
            y='Count:Q',
            tooltip=['Date', 'Count']
        ).properties(
            title='Responses Over Time',
            width=800,
            height=400
        )
        st.altair_chart(chart, use_container_width=True)

def show_policy_analysis(df):
    st.subheader("Policy and Regulation Analysis")
    
    # Policy presence
    st.markdown("**Policy and Regulatory Framework Presence**")
    policy_cols = [
        'G00Q11.SQ001_SQ001.', 'G00Q11.SQ002_SQ001.', 
        'G00Q11.SQ003_SQ001.', 'G00Q11.SQ004_SQ001.'
    ]
    policy_names = [
        "Pesticide Policy", "Conventional Pesticide Legislation",
        "Biopesticide Legislation", "IP Protection Legislation"
    ]
    
    policy_df = pd.DataFrame({
        'Policy': policy_names,
        'Yes': [df[col].str.contains('Yes').sum() for col in policy_cols],
        'No': [df[col].str.contains('No').sum() for col in policy_cols]
    }).melt(id_vars='Policy', var_name='Response', value_name='Count')
    
    chart = alt.Chart(policy_df).mark_bar().encode(
        x='Count:Q',
        y='Policy:N',
        color='Response:N',
        tooltip=['Policy', 'Response', 'Count']
    ).properties(
        width=600,
        height=400
    )
    st.altair_chart(chart, use_container_width=True)
    
    # Innovation Ratings
    st.markdown("**Innovation Enabling Ratings (1-5 scale)**")
    rating_cols = [
        'G00Q14.SQ001.', 'G00Q14.SQ002.', 'G00Q14.SQ003.', 
        'G00Q14.SQ004.', 'G00Q14.SQ006.', 'G00Q14.SQ007.'
    ]
    rating_names = [
        "Digital Technologies", "Biotechnology", "Renewable Energy",
        "Artificial Intelligence", "Conventional Pesticides", "Biopesticides"
    ]
    
    rating_df = df[rating_cols].apply(pd.to_numeric, errors='coerce')
    rating_df = rating_df.mean().reset_index()
    rating_df.columns = ['Innovation', 'Average Rating']
    rating_df['Innovation'] = rating_names
    
    chart = alt.Chart(rating_df).mark_bar().encode(
        x='Average Rating:Q',
        y=alt.Y('Innovation:N', sort='-x'),
        color=alt.Color('Average Rating:Q', scale=alt.Scale(scheme='greens')),
        tooltip=['Innovation', 'Average Rating']
    ).properties(
        title='Average Innovation Enabling Ratings',
        width=600,
        height=400
    )
    st.altair_chart(chart, use_container_width=True)

def show_registration_process(df):
    st.subheader("Pesticide Registration Process")
    reg_cols = [
        'G00Q18.SQ001_SQ001.', 'G00Q18.SQ002_SQ001.', 'G00Q18.SQ003_SQ001.',
        'G00Q18.SQ004_SQ001.', 'G00Q18.SQ005_SQ001.', 'G00Q18.SQ006_SQ001.'
    ]
    reg_names = [
        "Dossier Submission", "Initial Admin Actions", "Completeness Check",
        "Dossier Evaluation", "Registration Decision", "Publication"
    ]
    
    reg_df = pd.DataFrame({
        'Step': reg_names,
        'Yes': [df[col].str.contains('Yes').sum() for col in reg_cols]
    })
    
    chart = alt.Chart(reg_df).mark_bar().encode(
        x='Yes:Q',
        y=alt.Y('Step:N', sort='-x'),
        color=alt.Color('Yes:Q', scale=alt.Scale(scheme='purples')),
        tooltip=['Step', 'Yes']
    ).properties(
        title='Registration Process Steps (Conventional Pesticides)',
        width=600,
        height=400
    )
    st.altair_chart(chart, use_container_width=True)

def show_pesticide_data(df):
    st.subheader("Pesticide Registration and Production Data")
    
    # Get all pesticide-related columns
    pest_cols = [col for col in df.columns if 'G03Q19' in col]
    
    if not df[pest_cols].empty:
        years = ['2020', '2021', '2022', '2023', '2024']
        conv_pest = []
        bio_pest = []
        
        for i in range(5):
            conv_col = f'G03Q19.SQ00{i+1}_SQ001.'
            bio_col = f'G03Q19.SQ00{i+1}_SQ002.'
            
            # Use cleaned numeric values
            conv_mean = df[conv_col].mean() if conv_col in df.columns else np.nan
            bio_mean = df[bio_col].mean() if bio_col in df.columns else np.nan
            
            conv_pest.append(conv_mean if not np.isnan(conv_mean) else 0)
            bio_pest.append(bio_mean if not np.isnan(bio_mean) else 0)
        
        pest_df = pd.DataFrame({
            'Year': years,
            'Conventional Pesticides': conv_pest,
            'Biopesticides': bio_pest
        }).melt(id_vars='Year', var_name='Type', value_name='Count')
        
        chart = create_line_chart(pest_df, 'Year', 'Count', 'Type', 
                                'Average Number of Registered Pesticides')
        st.altair_chart(chart, use_container_width=True)
    else:
        st.warning("No pesticide registration data available")

def show_adoption_metrics(df):
    st.subheader("Adoption and Awareness")
    
    # Implementation of innovations
    st.markdown("**Implementation of Innovations (1-5 scale)**")
    impl_cols = [
        'G04Q21.SQ001.', 'G04Q21.SQ002.', 'G04Q21.SQ003.', 'G04Q21.SQ004.'
    ]
    impl_names = [
        "IPM Implementation", "CRISPR Gene Editing", 
        "Advanced Monitoring Systems", "Targeted Pest Behavior Studies"
    ]
    
    impl_df = df[impl_cols].apply(pd.to_numeric, errors='coerce').mean().reset_index()
    impl_df.columns = ['Innovation', 'Average Rating']
    impl_df['Innovation'] = impl_names
    
    chart = alt.Chart(impl_df).mark_bar().encode(
        x='Average Rating:Q',
        y=alt.Y('Innovation:N', sort='-x'),
        color=alt.Color('Average Rating:Q', scale=alt.Scale(scheme='oranges')),
        tooltip=['Innovation', 'Average Rating']
    ).properties(
        width=600,
        height=400
    )
    st.altair_chart(chart, use_container_width=True)
    
    # Farmer awareness and access
    st.markdown("**Farmer Awareness and Access**")
    awareness_cols = ['G00Q30.SQ001.', 'G00Q30.SQ002.']
    awareness_df = df[awareness_cols].apply(pd.to_numeric, errors='coerce').mean().reset_index()
    awareness_df.columns = ['Metric', 'Average Rating']
    awareness_df['Metric'] = ['Awareness', 'Access']
    
    chart = alt.Chart(awareness_df).mark_bar().encode(
        x='Metric:N',
        y='Average Rating:Q',
        color=alt.Color('Average Rating:Q', scale=alt.Scale(scheme='teals')),
        tooltip=['Metric', 'Average Rating']
    ).properties(
        title='Farmer Awareness and Access (1-5 scale)',
        width=600,
        height=400
    )
    st.altair_chart(chart, use_container_width=True)

def show_text_analysis(df, title, columns):
    """Display text analysis for challenges/recommendations with word cloud"""
    st.markdown(f"### {title}")
    
    # Process text columns safely
    text = process_text_columns(df, columns)
    
    if text.strip():
        # Create two columns: word cloud and frequency chart
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Word Cloud Visualization**")
            generate_wordcloud(
                text, 
                title,
                colormap='RdYlGn' if 'Challenge' in title else 'viridis'
            )
        
        with col2:
            st.markdown("**Top 20 Keywords**")
            chart = create_word_frequency_chart(text, f"Most Frequent Terms in {title}")
            st.altair_chart(chart, use_container_width=True)
        
        # Show full text in expander
        with st.expander(f"View all {title.lower()}"):
            st.text(text[:5000])  # Limit to first 5000 chars
    else:
        st.warning(f"No {title.lower()} data available")

# --- Main App ---
def main():
    st.title("🌾 Crop Protection Innovation Survey Dashboard")
    st.markdown("Monitoring the flow of crop protection innovation in low- and middle-income countries")
    
    # Load data
    df, total_records = load_data()
    
    if df is None:
        st.stop()
    
    # Sidebar filters
    st.sidebar.title("Filters")
    
    with st.sidebar.expander("Select Countries", expanded=False):
        selected_countries = st.multiselect(
            "Countries",
            options=df['G00Q01'].unique(),
            default=df['G00Q01'].unique(),
            label_visibility="collapsed"
        )
    
    with st.sidebar.expander("Select Stakeholder Categories", expanded=False):
        selected_stakeholders = st.multiselect(
            "Stakeholders",
            options=df['G00Q03'].dropna().unique(),
            default=df['G00Q03'].dropna().unique(),
            label_visibility="collapsed"
        )
    
    # Date range selection - clean and compact
    if not df['submitdate'].isna().all():
        min_date = df['submitdate'].min().date()
        max_date = max(df['submitdate'].max().date(), datetime.today().date())
        
        st.sidebar.markdown("**Select Date Range**")
        
        # Create a clean date range selector
        col1, col2 = st.sidebar.columns([1, 3])
        with col1:
            st.markdown("From:")
        with col2:
            start_date = st.date_input(
                "Start date",
                value=min_date,
                min_value=min_date,
                max_value=max_date,
                label_visibility="collapsed"
            )
        
        col1, col2 = st.sidebar.columns([1, 3])
        with col1:
            st.markdown("To:")
        with col2:
            end_date = st.date_input(
                "End date",
                value=max_date,
                min_value=min_date,
                max_value=max_date,
                label_visibility="collapsed"
            )
    else:
        today = datetime.today().date()
        start_date = end_date = today
    
    # Apply filters
    filtered_df = df[
        (df['G00Q01'].isin(selected_countries)) &
        (df['G00Q03'].isin(selected_stakeholders))
    ].copy()
    
    # Apply date filter if we have dates
    if not df['submitdate'].isna().all():
        filtered_df = filtered_df[
            (filtered_df['submitdate'].dt.date >= start_date) &
            (filtered_df['submitdate'].dt.date <= end_date)
        ]
    
    if filtered_df.empty:
        st.warning("No data matches the selected filters")
        return
    
    # Dashboard sections
    show_kpi_cards(filtered_df, total_records)
    show_response_overview(filtered_df)
    show_policy_analysis(filtered_df)
    show_registration_process(filtered_df)
    show_pesticide_data(filtered_df)
    show_adoption_metrics(filtered_df)
    
    # Challenges and Recommendations
    st.subheader("Text Analysis")
    show_text_analysis(filtered_df, "Common Challenges", 
                      ['G00Q36', 'G00Q37', 'G00Q38', 'G00Q39', 'G00Q40', 'G00Q41'])
    show_text_analysis(filtered_df, "Key Recommendations", 
                      ['G00Q42', 'G00Q43', 'G00Q44', 'G00Q45'])
    
    # Data explorer
    st.subheader("Data Explorer")
    if st.checkbox("Show raw data"):
        cols_to_show = [col for col in filtered_df.columns 
                       if col not in ['lastpage', 'startlanguage', 'G01Q46']]
        display_df = filtered_df[cols_to_show]
        st.dataframe(display_df)
    
    # Download button
    @st.cache_data
    def convert_df(df):
        return df.to_csv(index=False).encode('utf-8')
    
    csv = convert_df(filtered_df)
    st.download_button(
        label="Download filtered data as CSV",
        data=csv,
        file_name='filtered_survey_data.csv',
        mime='text/csv'
    )
    
    # Footer
    st.markdown("---")
    st.markdown("**Crop Protection Innovation Survey Dashboard** · Powered by Virtual Analytics")

if __name__ == "__main__":
    main()