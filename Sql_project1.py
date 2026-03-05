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
        "csv": "fraud_data.csv",
        "queries": {
            "Impossible Travel Detection": {
                "sql": """WITH TravelHistory AS (
    SELECT transaction_id, user_id, hour, country, 
           LAG(country) OVER(PARTITION BY user_id ORDER BY hour) AS prev_country,
           LAG(hour) OVER(PARTITION BY user_id ORDER BY hour) AS prev_hour
    FROM 'fraud_data.csv'
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
    FROM 'fraud_data.csv'
)
SELECT *, (amount - user_avg) / NULLIF(user_std, 0) AS z_score
FROM UserStats
WHERE ABS(z_score) > 3 ORDER BY z_score DESC;""",
                "insight": "Uses Z-Scores to find transactions that are statistical outliers based on individual user history."
            },
            "High Risk vs Global Average": {
                "sql": """WITH Global_Avg_CTE AS (
    SELECT AVG(device_risk_score) as platform_avg 
    FROM 'fraud_data.csv'
),
User_Avg_CTE AS (
    SELECT user_id, AVG(device_risk_score) as user_avg
    FROM 'fraud_data.csv'
    GROUP BY user_id
)
SELECT u.user_id, u.user_avg, g.platform_avg
FROM User_Avg_CTE u
CROSS JOIN Global_Avg_CTE g
WHERE u.user_avg > g.platform_avg
ORDER BY u.user_avg DESC;""",
                "insight": "Cross-joins user metrics against platform benchmarks to identify high-risk segments."
            },
            "Rapid-Fire Transactions": {
                "sql": """WITH CalcTime AS (
    SELECT transaction_id, user_id, hour, 
           LAG(hour) OVER (PARTITION BY user_id ORDER BY hour) AS prev_hour
    FROM 'fraud_data.csv'
)
SELECT *, (hour - prev_hour) AS hour_diff
FROM CalcTime
WHERE (hour - prev_hour) = 0
ORDER BY transaction_id DESC;""",
                "insight": "Identifies potential bot behavior where multiple transactions occur within the same hour."
            },
            "Regional Top Spenders": {
                "sql": """WITH RegionTopSpenders AS (
    SELECT user_id, country, amount,
           DENSE_RANK() OVER(PARTITION BY country ORDER BY amount DESC) as rank
    FROM 'fraud_data.csv'
)
SELECT * FROM RegionTopSpenders WHERE rank <= 2 ORDER BY country, rank;""",
                "insight": "Applies DENSE_RANK to find the top two highest spenders per country."
            },
            "Market Dominance Ratio": {
                "sql": """WITH UserCategoryTotals AS (
    SELECT user_id, transaction_type, SUM(amount) AS user_total_in_cat
    FROM 'fraud_data.csv'
    GROUP BY user_id, transaction_type
),
GlobalCategoryTotals AS (
    SELECT *, SUM(user_total_in_cat) OVER(PARTITION BY transaction_type) AS grand_total_for_cat
    FROM UserCategoryTotals
)
SELECT user_id, transaction_type, user_total_in_cat,
       (user_total_in_cat * 100.0 / grand_total_for_cat) AS percentage_of_cat_volume
FROM GlobalCategoryTotals
WHERE percentage_of_cat_volume > 50
ORDER BY percentage_of_cat_volume DESC;""",
                "insight": "Finds users who control more than 50% of the total volume in a specific transaction category."
            }
        }
    },
    "Spotify Analytics": {
        "csv": "spotify_tracks.csv",
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
FROM 'spotify_tracks.csv'
GROUP BY 1 ORDER BY 2 DESC;""",
                "insight": "Segmenting tracks into custom labels using multi-feature audio thresholds."
            },
            "Artist Diversity Index": {
                "sql": """WITH ArtistVibes AS (
    SELECT artist_name,
           CASE 
               WHEN energy > 0.8 THEN 'High Energy'
               WHEN energy < 0.4 THEN 'Chill'
               ELSE 'Balanced'
           END AS vibe
    FROM 'spotify_tracks.csv'
)
SELECT artist_name, COUNT(DISTINCT vibe) AS diversity_score
FROM ArtistVibes
GROUP BY artist_name
HAVING diversity_score > 1
ORDER BY diversity_score DESC LIMIT 10;""",
                "insight": "Ranks artists based on their musical range across different audio profiles."
            },
            "High-Performance Genres": {
                "sql": """SELECT genre, AVG(popularity) AS avg_popularity, COUNT(*) AS count_tracks
FROM 'spotify_tracks.csv'
GROUP BY genre
HAVING AVG(popularity) > 60 AND COUNT(*) > 100
ORDER BY avg_popularity DESC;""",
                "insight": "Filters for genres that achieve high popularity across a significant catalog size."
            },
            "Power Hour Intensity": {
                "sql": """SELECT genre, COUNT(*) as count_tracks, AVG(energy) as avg_energy, AVG(danceability) as avg_danceability
FROM 'spotify_tracks.csv'
GROUP BY genre
HAVING count_tracks > 7100
ORDER BY avg_energy DESC;""",
                "insight": "Analyzes correlation between energy and danceability for high-volume genres."
            },
            "Prolific Artists (Unique Albums)": {
                "sql": """SELECT artist_name, COUNT(*) AS total_tracks, COUNT(DISTINCT album_name) AS unique_albums
FROM 'spotify_tracks.csv'
GROUP BY artist_name
ORDER BY unique_albums DESC LIMIT 10;""",
                "insight": "Compares track volume vs. unique album releases to identify the most prolific creators."
            },
            "Metal Genre Popularity": {
                "sql": """SELECT artist_name, AVG(popularity) as avg_popularity
FROM 'spotify_tracks.csv'
WHERE genre = 'Metal'
GROUP BY artist_name
HAVING COUNT(*) > 5
ORDER BY avg_popularity DESC LIMIT 10;""",
                "insight": "Finds the highest-rated Metal artists with a statistically significant track count."
            },
            "Quiet-Power Profiles": {
                "sql": """SELECT artist_name, AVG(energy) as avg_energy, AVG(loudness) as avg_loudness 
FROM 'spotify_tracks.csv'
GROUP BY artist_name 
HAVING COUNT(*) >= 5 AND avg_energy > 0.8 AND avg_loudness < -15.0;""",
                "insight": "Technical audit finding high-energy tracks with low recording decibels (Quiet Power)."
            },
            "Workout Warrior Tracks": {
                "sql": """SELECT track_name, artist_name, energy, danceability
FROM 'spotify_tracks.csv'
WHERE energy > 0.9 AND danceability > 0.7
ORDER BY energy DESC LIMIT 10;""",
                "insight": "Filtering for tracks that meet the mathematical criteria for high-intensity exercise."
            },
            "Genre Popularity Gap": {
                "sql": """SELECT genre, MAX(popularity) as most_popular, MIN(popularity) as least_popular, COUNT(*) as song_count
FROM 'spotify_tracks.csv'
GROUP BY genre
ORDER BY song_count DESC LIMIT 10;""",
                "insight": "Measures the range of popularity within the most common music genres."
            },
            "Duplicate Track Audit": {
                "sql": """SELECT track_name, artist_name, COUNT(*) AS occurrence_count
FROM 'spotify_tracks.csv'
GROUP BY track_name, artist_name
HAVING occurrence_count > 1
ORDER BY occurrence_count DESC LIMIT 10;""",
                "insight": "A data cleaning query to identify redundant records in the Spotify database."
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