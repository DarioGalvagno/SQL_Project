import streamlit as st
import duckdb
import pandas as pd

# --- PAGE CONFIG ---
st.set_page_config(page_title="SQL Portfolio | Dario Galvagno", layout="wide", page_icon="📊")

# --- CUSTOM CSS ---
st.markdown("""
    <style>
    .main { background-color: #f5f7f9; }
    .stCode { border: 1px solid #e0e0e0; border-radius: 5px; }
    </style>
    """, unsafe_allow_html=True)

# --- SIDEBAR & PROFILE ---
st.sidebar.title("👨‍💻 SQL Portfolio")
st.sidebar.markdown("### **Created by: Dario Galvagno**")
st.sidebar.divider()

st.sidebar.subheader("🎯 Portfolio Purpose")
st.sidebar.write("""
This application serves as a technical showcase of my ability to bridge 
**Python application development** with **Advanced SQL Analytics**. 

**Key Skills Demonstrated:**
* **Relational Joins**: Connecting Star-Schema tables (Fact/Dimension).
* **Window Functions**: Expert use of `DENSE_RANK`, `LAG`, and `PARTITION BY`.
* **Behavioral Analysis**: Identifying fraud patterns and delivery trends.
* **Data Engineering**: Handling character encoding challenges using DuckDB.

**Note:** All datasets utilized in this portfolio were sourced from **Kaggle**.
""")

st.sidebar.divider()

# --- DATA DICTIONARY ---
portfolio = {
    "Amazon Delivery Operations": {
        "queries": {
            "High-Performance Agents": {
                "sql": "SELECT DISTINCT Order_ID, Agent_Rating, Traffic FROM read_csv_auto('amazon_delivery.csv') WHERE Agent_Rating = 5 AND Traffic = 'Low';",
                "insight": "Identifying best-case scenarios for delivery performance with perfect ratings and low traffic."
            },
            "Late Night Latency": {
                "sql": "SELECT * FROM read_csv_auto('amazon_delivery.csv') WHERE Order_Time > '22:00:00' AND Delivery_Time > 120;",
                "insight": "Analyzing safety and efficiency for orders placed after 10:00 PM with long lead times."
            },
            "Traffic Impact Classification": {
                "sql": """SELECT Order_ID, Delivery_Time, Traffic,
CASE
    WHEN Delivery_Time < 60 THEN 'Fast'
    WHEN Delivery_Time BETWEEN 60 AND 120 THEN 'Average'
    ELSE 'Slow'
END AS Delivery_Speed_Status
FROM read_csv_auto('amazon_delivery.csv') 
WHERE Traffic = 'Jam';""",
                "insight": "Categorizing customer experience specifically during high-congestion (Jam) conditions."
            },
            "Peak Delivery Days": {
                "sql": "SELECT strftime('%A', CAST(Order_Date AS DATE)) AS Order_Day, COUNT(Order_ID) AS Total_Orders FROM read_csv_auto('amazon_delivery.csv') GROUP BY 1 ORDER BY Total_Orders DESC;",
                "insight": "Resource leveling: Identifying which days of the week require the highest courier density."
            },
            "Click-to-Ship Latency": {
                "sql": "SELECT Order_ID, date_diff('minute', CAST(Order_Time AS TIME), CAST(Pickup_Time AS TIME)) AS Prep_Time_Minutes FROM read_csv_auto('amazon_delivery.csv');",
                "insight": "Measures warehouse efficiency by calculating the gap between order placement and courier pickup."
            },
            "Workforce Demographics": {
                "sql": """SELECT AVG(Delivery_Time) AS Avg_Delivery_Time,
CASE 
    WHEN Agent_Age < 25 THEN 'Gen Z'
    WHEN Agent_Age BETWEEN 25 AND 40 THEN 'Millennial'
    ELSE 'Experienced'
END AS Workforce_Demographic
FROM read_csv_auto('amazon_delivery.csv')
GROUP BY Workforce_Demographic;""",
                "insight": "Correlating delivery experience/age with operational efficiency."
            },
            "High-Risk Delivery Report": {
                "sql": """SELECT Order_ID, 
CASE
    WHEN Traffic = 'Jam' AND Weather = 'Stormy' THEN 'Critical'
    WHEN Traffic = 'Jam' OR Weather = 'Stormy' THEN 'High'
    ELSE 'Normal'
END AS Risk_Level
FROM read_csv_auto('amazon_delivery.csv')
WHERE Risk_Level IN ('Critical', 'High');""",
                "insight": "Proactive customer service report identifying orders likely to miss the delivery promise."
            },
            "Efficiency Frontier (Top 10%)": {
                "sql": """WITH EfficiencyRank AS (
    SELECT Order_ID, Area, Delivery_Time, PERCENT_RANK() OVER(PARTITION BY Area ORDER BY Delivery_Time) AS Efficiency
    FROM read_csv_auto('amazon_delivery.csv')
)
SELECT * FROM EfficiencyRank WHERE Efficiency <= 0.10;""",
                "insight": "Using PERCENT_RANK to find the top 10% fastest deliveries relative to local Area constraints."
            },
            "Distance-Based Speed Analysis": {
                "sql": """SELECT Order_ID, Area, Traffic,
SQRT(POWER(Drop_Latitude - Store_Latitude, 2) + POWER(Drop_Longitude - Store_Longitude, 2)) AS Distance_Units,
(SQRT(POWER(Drop_Latitude - Store_Latitude, 2) + POWER(Drop_Longitude - Store_Longitude, 2)) / NULLIF(Delivery_Time, 0)) * 100 AS Speed_Index
FROM read_csv_auto('amazon_delivery.csv')
WHERE Delivery_Time > 0
ORDER BY Speed_Index ASC;""",
                "insight": "Identifying 'Friction' by calculating speed index using the Pythagorean approximation $$\\sqrt{(lat2-lat1)^2 + (long2-long1)^2}$$."
            }
        }
    },
    "Sales & Retail Analytics": {
        "queries": {
            "Revenue & Profit Margin by Category": {
                "sql": """WITH CategorySales AS (
    SELECT pc.ProductCategory,
           SUM(fs.SalesQuantity * p.UnitPrice) AS TotalRevenue,
           SUM(fs.SalesQuantity * p.UnitCost) AS TotalCost
    FROM read_csv_auto('fact_sales.csv', ignore_errors=True) fs
    JOIN read_csv_auto('product.csv', ignore_errors=True) p ON fs.ProductKey = p.ProductKey
    JOIN read_csv_auto('product_subcategory.csv', ignore_errors=True) psc ON p.ProductSubcategoryKey = psc.ProductSubcategoryKey
    JOIN read_csv_auto('product_category.csv', ignore_errors=True) pc ON psc.ProductCategoryKey = pc.ProductCategoryKey
    GROUP BY pc.ProductCategory
)
SELECT ProductCategory,
       ROUND(TotalRevenue, 2) AS Revenue,
       ROUND(TotalRevenue - TotalCost, 2) AS Profit,
       ROUND(((TotalRevenue - TotalCost) / NULLIF(TotalRevenue, 0)) * 100, 2) || '%' AS ProfitMargin
FROM CategorySales ORDER BY Profit DESC;""",
                "insight": "Calculates financial health by joining 4 tables to derive margins across product lines."
            },
            "Store Benchmark Analysis": {
                "sql": """WITH StorePerformance AS (
    SELECT s.StoreName, SUM(p.UnitPrice * fs.SalesQuantity) AS StoreRevenue
    FROM read_csv_auto('fact_sales.csv', ignore_errors=True) fs
    JOIN read_csv_auto('product.csv', ignore_errors=True) p ON p.ProductKey = fs.ProductKey
    JOIN read_csv_auto('store.csv', ignore_errors=True) s ON s.StoreKey = fs.StoreKey
    GROUP BY s.StoreName
),
CompanyStats AS (
    SELECT AVG(StoreRevenue) AS avg_revenue FROM StorePerformance
)
SELECT sp.StoreName, ROUND(sp.StoreRevenue, 2) AS Revenue,
       ROUND(cs.avg_revenue, 2) AS CompanyAvg,
       CASE WHEN sp.StoreRevenue > cs.avg_revenue THEN 'ABOVE AVERAGE' ELSE 'BELOW AVERAGE' END AS Status
FROM StorePerformance sp, CompanyStats cs ORDER BY sp.StoreRevenue DESC;""",
                "insight": "Compares individual store revenue against the company-wide average benchmark."
            }
        }
    },
    "Fraud Detection": {
        "queries": {
            "Impossible Travel Detection": {
                "sql": """WITH TravelHistory AS (
    SELECT transaction_id, user_id, hour, country, 
           LAG(country) OVER(PARTITION BY user_id ORDER BY hour) AS prev_country,
           LAG(hour) OVER(PARTITION BY user_id ORDER BY hour) AS prev_hour
    FROM read_csv_auto('fraud_data.csv', ignore_errors=True)
),
TimeCalc AS (
    SELECT *, (hour - prev_hour) AS hours_since_last_tx,
    CASE WHEN country != prev_country THEN 1 ELSE 0 END AS LocationChange
    FROM TravelHistory
)
SELECT * FROM TimeCalc 
WHERE LocationChange = 1 AND hours_since_last_tx < 3
ORDER BY hours_since_last_tx ASC;""",
                "insight": "Detects high-risk accounts transacting in different countries within a 3-hour window."
            }
        }
    },
    "Spotify Analytics": {
        "queries": {
            "Vibe Category Market Share": {
                "sql": """SELECT 
    CASE 
        WHEN energy > 0.8 AND loudness > -0.6 THEN 'Stadium Power' 
        WHEN danceability > 0.7 AND energy < 0.7 AND loudness > -10.0 THEN 'Club Banger' 
        WHEN energy < 0.6 AND danceability > 0.6 AND loudness < -10.0 THEN 'Chill Groove' 
        WHEN energy < 0.4 AND danceability < 0.4 AND loudness < -15.0 THEN 'True Ambient' 
        ELSE 'Standard Radio Mix' 
    END AS audio_category,
    COUNT(*) AS total_songs
FROM read_csv_auto('spotify_tracks.csv', ignore_errors=True)
GROUP BY 1 ORDER BY 2 DESC;""",
                "insight": "Segmenting tracks into custom labels using multi-feature audio thresholds."
            }
        }
    }
}

# --- NAVIGATION ---
dataset_choice = st.sidebar.selectbox("📂 Select Dataset", list(portfolio.keys()))
query_choice = st.sidebar.selectbox("🔍 Select Analysis", list(portfolio[dataset_choice]["queries"].keys()))

selected = portfolio[dataset_choice]["queries"][query_choice]

# --- MAIN CONTENT ---
st.header(f"{dataset_choice}: {query_choice}")
st.write(f"**Objective:** {selected['insight']}")

with st.expander("👀 View SQL Logic", expanded=True):
    st.code(selected["sql"], language="sql")

try:
    df = duckdb.query(selected["sql"]).df()
    st.subheader("📊 Query Results")
    st.dataframe(df, use_container_width=True)

    csv_data = df.to_csv(index=False).encode('utf-8')
    st.download_button(label="📥 Download Results", data=csv_data, file_name=f"{query_choice}.csv", mime='text/csv')

except Exception as e:
    st.error(f"Execution Error: {e}")
    st.warning("Ensure the CSV files are in the same directory as this script.")

st.divider()
st.caption(f"© 2026 | Created by Dario Galvagno | Built with Streamlit, DuckDB, and Python")