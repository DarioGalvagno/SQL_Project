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
* **Behavioral Analysis**: Identifying fraud patterns and audio trends.
* **Data Engineering**: Handling character encoding challenges using DuckDB.
""")

st.sidebar.divider()

# --- DATA DICTIONARY ---
# Using read_csv_auto with ignore_errors=True to handle the Unicode byte mismatch
portfolio = {
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
            },
            "Daily Growth Tracker (LAG)": {
                "sql": """WITH DailySales AS (
    SELECT fs.DateKey, SUM(fs.SalesQuantity * p.UnitPrice) AS DailyRevenue
    FROM read_csv_auto('fact_sales.csv', ignore_errors=True) fs
    JOIN read_csv_auto('product.csv', ignore_errors=True) p ON p.ProductKey = fs.ProductKey
    GROUP BY fs.DateKey
)
SELECT DateKey, ROUND(DailyRevenue, 2) AS Today,
       ROUND(LAG(DailyRevenue) OVER(ORDER BY DateKey), 2) AS Yesterday,
       ROUND(((DailyRevenue - LAG(DailyRevenue) OVER (ORDER BY DateKey)) / 
       NULLIF(LAG(DailyRevenue) OVER (ORDER BY DateKey), 0)) * 100, 2) || '%' AS Growth
FROM DailySales;""",
                "insight": "Uses Window Functions to track day-over-day revenue percentage changes."
            },
            "Return Rate Analysis by Store": {
                "sql": """SELECT s.StoreName, SUM(fs.SalesQuantity) AS TotalSold, SUM(fs.ReturnQuantity) AS TotalReturned,
       ROUND((SUM(fs.ReturnQuantity) * 1.0 / NULLIF(SUM(fs.SalesQuantity),0)) * 100, 2) AS ReturnPercentage
FROM read_csv_auto('store.csv', ignore_errors=True) s
JOIN read_csv_auto('fact_sales.csv', ignore_errors=True) fs ON s.StoreKey = fs.StoreKey
GROUP BY s.StoreName ORDER BY ReturnPercentage DESC;""",
                "insight": "Identifies stores with high return volumes to flag potential quality or service issues."
            },
            "Revenue by Country": {
                "sql": """SELECT g.RegionCountryName, ROUND(SUM(fs.SalesQuantity * p.UnitPrice), 2) AS Revenue
FROM read_csv_auto('fact_sales.csv', ignore_errors=True) fs
JOIN read_csv_auto('product.csv', ignore_errors=True) p ON p.ProductKey = fs.ProductKey
JOIN read_csv_auto('store.csv', ignore_errors=True) s ON s.StoreKey = fs.StoreKey
JOIN read_csv_auto('geography.csv', ignore_errors=True) g ON g.GeographyKey = s.GeographyKey
GROUP BY g.RegionCountryName ORDER BY Revenue DESC;""",
                "insight": "Global sales distribution across different geographic regions."
            },
            "Sales Efficiency (Per Employee)": {
                "sql": """SELECT s.StoreName, s.EmployeeCount, SUM(fs.SalesQuantity) AS TotalQty,
       ROUND((SUM(fs.SalesQuantity) * 1.0 / NULLIF(s.EmployeeCount, 0)), 2) AS SalesPerEmployee
FROM read_csv_auto('store.csv', ignore_errors=True) s
JOIN read_csv_auto('fact_sales.csv', ignore_errors=True) fs ON s.StoreKey = fs.StoreKey
GROUP BY s.StoreName, s.EmployeeCount ORDER BY SalesPerEmployee DESC;""",
                "insight": "Measures staff productivity by calculating sales volume relative to store headcount."
            },
            "Product Ranking within Stores": {
                "sql": """WITH ProductSales AS (
    SELECT s.StoreName, p.ProductName, SUM(fs.SalesQuantity * p.UnitPrice) AS TotalRevenue
    FROM read_csv_auto('fact_sales.csv', ignore_errors=True) fs
    JOIN read_csv_auto('product.csv', ignore_errors=True) p ON fs.ProductKey = p.ProductKey
    JOIN read_csv_auto('store.csv', ignore_errors=True) s ON s.StoreKey = fs.StoreKey
    GROUP BY s.StoreName, p.ProductName
)
SELECT StoreName, ProductName, ROUND(TotalRevenue, 2) AS Revenue,
       DENSE_RANK() OVER(PARTITION BY StoreName ORDER BY TotalRevenue DESC) AS StoreRank
FROM ProductSales ORDER BY StoreName, StoreRank;""",
                "insight": "Ranks top-selling products individually for every store in the database."
            },
            "Store Density (Sales/SqFt)": {
                "sql": """SELECT s.StoreName, s.SellingAreaSize, SUM(fs.SalesQuantity) AS TotalItems,
       ROUND((SUM(fs.SalesQuantity) * 1.0 / NULLIF(s.SellingAreaSize, 0)), 2) AS SalesPerFoot
FROM read_csv_auto('store.csv', ignore_errors=True) s
JOIN read_csv_auto('fact_sales.csv', ignore_errors=True) fs ON s.StoreKey = fs.StoreKey
GROUP BY s.StoreName, s.SellingAreaSize ORDER BY SalesPerFoot DESC;""",
                "insight": "Analyzes real estate efficiency: items sold per square foot of selling space."
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
            },
            "Spending Outliers (Z-Score)": {
                "sql": """WITH UserStats AS (
    SELECT *, AVG(amount) OVER(PARTITION BY user_id) AS user_avg,
           STDDEV(amount) OVER(PARTITION BY user_id) AS user_std
    FROM read_csv_auto('fraud_data.csv', ignore_errors=True)
)
SELECT *, (amount - user_avg) / NULLIF(user_std, 0) AS z_score
FROM UserStats
WHERE ABS(z_score) > 3 ORDER BY z_score DESC;""",
                "insight": "Uses Z-Scores to find transactions that are statistical outliers based on individual user history."
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

#

with st.expander("👀 View SQL Logic", expanded=True):
    st.code(selected["sql"], language="sql")

try:
    # Use DuckDB to execute the query
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