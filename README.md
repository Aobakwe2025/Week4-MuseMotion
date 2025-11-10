 # MuseMotion: Electric Vehicle Data Analysis & Dashboard
 <h3>"From insight to ignition"</h3>  

MuseMotion is a data engineering platform built to process, analyze, and visualize insights from electric vehicle (EV) datasets. It automates data ingestion, transformation, and cloud-based storage using Python-powered ETL pipelines and advanced SQL queries.
By transforming raw EV data into structured intelligence, MuseMotion bridges data engineering and sustainability—helping teams extract meaningful insights that drive innovation in the electric mobility space. 

##  💡 Why We Created MuseMotion 
MuseMotion was built to demonstrate how data engineers can translate large, messy datasets into reliable and insightful metrics—tracking battery performance, charging trends, and EV efficiency at scale.

Our goal was to create a working prototype that integrates SQL, Python, and Azure cloud services to simulate a professional-grade ETL process, building the bridge between data insight and actionable ignition in the EV ecosystem.  

## ⚙️ Core Features
- Automated ETL Pipeline: End-to-end Extract, Transform, and Load process using Python.
SQL Analysis: Advanced queries (joins, aggregations, subqueries, CRUD operations) for EV data insights.
- Cloud Integration (Azure): Uploads both raw and processed data to Azure Blob Storage and connects to Azure SQL Database.
- Data Quality Checks: Cleans, validates, and logs data transformations for accuracy and consistency.
- Scalable Design: Supports local SQLite testing and cloud deployment for production-ready workflows.

## ⛓ Tech Stack
Database Analysis:  
- Kaggle Datasets for dataset sourcing.  
- SQL for data querying and manipulation.  
- MySQL Database.  
- Pandas analysis.  


Cloud Platform:   
- Microsoft Azure.

## 📷 Project Structure

```
MuseMotion/  
├── data/                   # Data storage folder.  
│   ├── raw/                # Unprocessed CSV/JSON files (source data).  
│   └── processed/          # Cleaned and transformed datasets ready for analysis.  
│ 
├── sql/                    # SQL-related scripts and schema definitions.  
│   ├── schema.sql          # Database schema creation and table relationships.  
│   ├── queries.sql         # Analysis queries (joins, aggregations, CTEs, CRUD).  
│   └── analysis.sql        # Insight-driven queries and reports.  
│ 
├── src/                    # Core Python ETL code. 
│   ├── extract.py          # Data extraction from local or external sources. 
│   ├── transform.py        # Data cleaning, formatting, and validation. 
│   ├── load.py             # Load processed data into SQLite and Azure SQL. 
│   └── utils.py            # Helper functions (logging, config, error handling). 
│ 
├── dashboard/              # Optional visualization interface. 
│   └── app.py              # Streamlit dashboard for data exploration and metrics. 
│ 
├── config/                 # Configuration and environment setup. 
│   ├── .env.example        # Template for environment variables. 
│   └── azure_config.json   # Azure connection details (non-sensitive placeholders). 
│ 
├── tests/                  # Unit and integration tests for ETL components. 
│ ├── test_etl.py           # Tests for data extraction, transformation, and loading. 
│ └── test_sql.py           # Tests for SQL queries and schema validation. 
│ 
├── requirements.txt         # Project dependencies (pandas, SQLAlchemy, azure-storage-blob, Streamlit, etc.) 
├── README.md                # Project documentation (this file). 
└── docs/                    # Documentation and sprint planning.
  ├── sprint_plan_week4.md   # Week 4 sprint overview and team tasks. 
  └── architecture_diagram.png # Visual diagram of ETL and cloud architecture.

```

## 🗓️ Future Improvements
- Long-Term Tracking: Extend data collection to analyze EV performance over time, not just single snapshots.
- Predictive Analytics: Integrate models to forecast EV demand and charging patterns.
- Enhanced Visualization: Expand Streamlit or Power BI dashboards for deeper insights.
- Automated Notifications: Use Azure Logic Apps to send alerts for failed uploads or pipeline errors.
- Full Azure Integration: Transition from SQLite to fully cloud-hosted Azure SQL workflows.

## 📄 License
This project was created as part of a coding bootcamp group's Data Pipeline Builder

## 👩🏽‍💻 The Git Girls Team

| Member | Role | Responsibilities |
|---------|------|------------------|
| **Aobakwe Modillane** | Scrum Master. | Project management, repository setup, dashboard development, cloud integration, documentation. |
| **Boikanyo Maswi** | Junior Developer. | SQL scripts, ETL logic, README & repo documentation, README.md, repo about. |
| **Luyanda Zuma** | Junior Develper. | SQL scripts, ETL logic, README & repo documentation. |
| **Nqobile Masombuka** | Junior Developer. | Excel data cleaning, documentation, README.md. |

<h3>Made with 💜 by Git Girls.</h3>  
 
