# Dagger
## Introduction
The resale property market in Singapore poses a significant challenge for interested buyers due to the numerous platforms available to view listings. In addition to the constant fluctuation in housing prices, it increases the difficulty for buyers to view past trends or price ranges that could potentially provide them much more insights into their decision making. Hence, the fragmentation of the real estate market and consistent price volatility has made it increasingly challenging for prospective buyers to accurately compare prices and make informed decisions.

Given the turbulent nature of the resale market in Singapore, it is crucial that potential buyers have access to a reliable and up-to-date platform that allows them to gain insights into the current state of the resale market. Our project consists of two main objectives: (1) to develop a predictive model to forecast resale prices of housing units in Singapore and (2) create an interactive dashboard for comprehensive analysis of housing prices trends and their features. As a secondary objective, we also want to allow potential renters to use our platform to gain an understanding about the current rental market in Singapore. 

## Data Pipeline and Tech Stack
We used the OneMap API and data.gov.sg's API to extract relevant data asynchronously. The extracted data is put through some data transformation to ensure it is consistent with our Data Warehouse design, and is subsequently loaded into Google BigQuery. The downstream models include BigQuery's ML for Resale Flat Price Predictions and Tableau as a Dashboarding application.
![IS3107 ER Diagram - Data Pipeline](https://github.com/dylanlo8/Dagger/assets/100820436/3cd95da0-cd52-4b58-9424-bf8ba64051d3)

## Data Warehouse ER Diagram
![IS3107 ER Diagram - Copy of Final ER Diagram (1)](https://github.com/dylanlo8/Dagger/assets/100820436/21b0e9e6-8272-40df-8971-a45c5786c659)

## Code
This repository contains 3 key files in our project.
dags/main.py: Our defined DAG that runs the ETL Pipeline in Apache Airflow.
XGBoost_model.sql: The code we used to create a XGBoost BigQuery ML Model for Resale Flat Price Predictions
