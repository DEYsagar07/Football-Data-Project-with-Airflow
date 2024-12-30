# Football-Data-Project-with-Airflow

🚀 End-to-End Data Engineering Project: Football Data Analysis ⚽📊

I recently completed an end-to-end data engineering project utilizing a hybrid architecture that integrates Apache Airflow with Azure to analyze football data. Here’s an overview of the steps and tools involved:

1️⃣ Data Extraction: Extracted football data from Wikipedia (HTML format) and set up automated task scheduling with Apache Airflow, orchestrated using Docker. This Airflow setup enables seamless integration with Azure for orchestrating workflows in the cloud.

2️⃣ Data Storage: Initially stored the extracted data in PostgreSQL, then imported it into Microsoft SQL Server (on-premises).

3️⃣ Cloud Transfer with Azure: Used Azure Data Factory to securely transfer the data from SQL Server to Azure Data Lake Storage Gen2 for scalable storage.

4️⃣ Data Transformation: Leveraged Azure Databricks to clean, transform, and prepare the data, saving the results in the (transformed_data) folder for further analysis.

5️⃣ Data Analysis: Queried the transformed data using Azure Synapse Analytics to gain deeper insights.

6️⃣ Visualization: Built dynamic and insightful dashboards in Power BI to visualize and present the data.

This project highlights the power of combining Apache Airflow for workflow automation with Azure’s comprehensive data services, creating a robust, scalable, end-to-end data pipeline that integrates both cloud and on-premises solutions. 💼💡

hashtag#DataEngineering hashtag#Azure hashtag#ApacheAirflow hashtag#Databricks hashtag#DataFactory hashtag#SynapseAnalytics hashtag#PowerBI hashtag#BigData hashtag#DataPipeline
