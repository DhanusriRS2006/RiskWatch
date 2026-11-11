# RiskWatch: Real-Time Multi-Disease Risk Prediction System

RiskWatch is an end-to-end big data pipeline that streams patient health records through Kafka, scores each patient for **Diabetes, Heart Disease and Stroke** risk in real time using Spark MLlib, stores the predictions in MongoDB, and displays them on a live dashboard.

I designed and built the entire system as my final project for Big Data Architecture at Rajalakshmi Engineering College, Chennai.

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.8-231F20?logo=apachekafka&logoColor=white)
![Hadoop](https://img.shields.io/badge/HDFS-Hadoop%203.2-66CCFF?logo=apachehadoop&logoColor=black)
![MongoDB](https://img.shields.io/badge/MongoDB-7-47A248?logo=mongodb&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit&logoColor=white)

<!-- Add a dashboard screenshot: ![RiskWatch Dashboard](docs/dashboard.png) -->

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [System Architecture](#system-architecture)
- [How It Works](#how-it-works)
- [Tech Stack](#tech-stack)
- [Dataset](#dataset)
- [Machine Learning Models](#machine-learning-models)
- [Risk Classification](#risk-classification)
- [Dashboard](#dashboard)
- [Design Decisions](#design-decisions)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Limitations and Future Work](#limitations-and-future-work)
- [Author](#author)

---

## Overview

Most health risk tools check one disease at a time using a static form. In a real screening setting, records arrive continuously and the people who need attention first are the ones that matter most.

RiskWatch handles this as a streaming problem. Every incoming patient record is scored for three diseases at once, assigned a risk level, and shown on a dashboard within seconds. High-risk patients trigger alerts so they can be prioritised.

The project covers the full data lifecycle: raw data ingestion, distributed cleaning and training, real-time scoring, persistent storage, and visualisation, all running in containers on a single laptop.

## Key Features

- **Multi-disease scoring:** one patient record produces three independent risk predictions (Diabetes, Heart Disease, Stroke)
- **Real-time processing:** Kafka and Spark Structured Streaming score patients as they arrive
- **Distributed batch training:** data cleaning and model training run on Spark, with data and models stored in HDFS
- **Three-level risk classification:** Low, Medium and High, with alerts for high-risk patients
- **Live dashboard:** risk distribution, alerts over time, latest high-risk patients and a live patient feed
- **One-command deployment:** HDFS, Kafka, MongoDB and the application all start with Docker Compose
- **Runs on a laptop:** only free, open source tools, with a low-RAM mode for 8 GB machines

## System Architecture

RiskWatch follows the **Lambda architecture** pattern with three layers.

```
                 BATCH LAYER (run once)
 CSV dataset --> HDFS --> Spark (clean) --> Spark MLlib --> 3 models saved in HDFS
                                                               |
                 SPEED LAYER (real time)                       v
 producer.py --> Kafka topic "patients" --> Spark Structured Streaming
                                           (apply 3 models, assign risk level)
                                                               |
                 SERVING LAYER                                 v
                       Streamlit dashboard <-- MongoDB (predictions)
```

| Layer | Purpose | Components |
|---|---|---|
| Batch | Clean historical data and train models | HDFS, Spark, Spark MLlib |
| Speed | Score new patients as they arrive | Kafka, Spark Structured Streaming |
| Serving | Store and display results | MongoDB, Streamlit |

## How It Works

**1. Batch layer (run once)**
The raw CSV is uploaded to HDFS. A Spark job removes nulls and duplicates, checks disease prevalence, splits the data 80/20 into train and test sets, and saves both as Parquet. A second job trains one Random Forest model per disease, evaluates it on the test set, and saves the three models back to HDFS.

**2. Speed layer (continuous)**
A Python producer publishes patient records to the Kafka topic `patients` at a configurable rate. A Spark Structured Streaming job reads from the topic, loads the three saved models, scores each patient for every disease, and assigns a risk level based on predicted probability. High-risk patients are printed as alerts in the console.

**3. Serving layer**
Each scored patient is written to MongoDB. The Streamlit dashboard reads from MongoDB and refreshes every few seconds to show live results.

## Tech Stack

| Layer | Tool |
|---|---|
| Storage | HDFS (Hadoop 3.2) |
| Batch processing | Apache Spark 3.5 (PySpark) |
| Machine learning | Spark MLlib (Random Forest) |
| Ingestion | Apache Kafka 3.8 |
| Stream processing | Spark Structured Streaming |
| Database | MongoDB 7 |
| Dashboard | Streamlit |
| Deployment | Docker Compose |
| Language | Python 3.11 |

## Dataset

[Diabetes Health Indicators Dataset](https://www.kaggle.com/datasets/alexteboul/diabetes-health-indicators-dataset) by Alex Teboul, based on the CDC Behavioral Risk Factor Surveillance System (BRFSS 2015) survey.

- **Size:** 253,680 records
- **Inputs:** health and lifestyle indicators such as blood pressure, cholesterol, BMI, smoking, physical activity, general health, age, education and income
- **Targets:** Diabetes, Heart Disease or Attack, and Stroke

The dataset is not included in this repository. Download `diabetes_binary_health_indicators_BRFSS2015.csv` from Kaggle and place it in the `data/` folder. A synthetic data generator with the same columns is included for quick testing.

## Machine Learning Models

One **Random Forest classifier** is trained per disease using Spark MLlib, so each disease gets its own model and its own probability output.

| Disease | AUC | Accuracy | Precision | Recall |
|---|---|---|---|---|
| Diabetes | TBD | TBD | TBD | TBD |
| Heart Disease | TBD | TBD | TBD | TBD |
| Stroke | TBD | TBD | TBD | TBD |

Evaluation is done on a held-out 20% test set. Training also prints the top contributing features for each disease.

## Risk Classification

The model's predicted probability is converted into a simple level that is easy to act on.

| Probability | Risk Level |
|---|---|
| Below 30% | Low |
| 30% to 60% | Medium |
| 60% and above | High (alert) |

## Dashboard

The Streamlit dashboard refreshes every few seconds and shows:

- Total patients scored and number of alerts
- Risk level distribution for each disease
- Alerts over time
- Latest high-risk patients
- Live patient feed
- Model performance summary

## Design Decisions

- **Lambda architecture:** training is expensive and done once in batch, while scoring is cheap and done continuously in the speed layer. Keeping them separate lets each scale on its own.
- **Spark MLlib over scikit-learn:** models are trained and applied inside Spark, so the same models score streaming data with no format conversion.
- **Kafka as the ingestion layer:** it decouples the data source from processing, so the producer can be replaced by a real hospital feed without changing the pipeline.
- **MongoDB for predictions:** a document store fits semi-structured prediction records and is easy for the dashboard to query.
- **Shared scoring module:** `scoring.py` holds the prediction logic in one place so the batch and streaming code stay consistent.
- **Docker Compose:** the full stack is reproducible with one command instead of installing Hadoop, Kafka and Spark manually.
- **Configurable storage:** a low-RAM mode swaps HDFS for local disk so the project still runs on 8 GB laptops.

## Project Structure

```
riskwatch/
├── docker-compose.yml          # HDFS, Kafka, MongoDB and app container
├── Dockerfile                  # Python 3.11 + Java + PySpark
├── requirements.txt
├── config.py                   # all settings in one place
├── data/
│   └── generate_sample_data.py # synthetic data with the same columns
├── batch/
│   ├── preprocess.py           # ingest to HDFS, clean, split
│   └── train_models.py         # train and evaluate 3 models
├── streaming/
│   ├── producer.py             # sends patients to Kafka
│   ├── scoring.py              # shared prediction logic
│   └── stream_predict.py       # Spark Streaming to MongoDB
├── dashboard/
│   └── app.py                  # live dashboard
└── docs/
    └── RiskWatch_Explanation.pdf
```

## Quick Start

Requires Docker Desktop and 8 GB RAM (16 GB recommended). Place the dataset CSV in `data/` first.

```bash
git clone https://github.com/DhanusriRS2006/riskwatch.git
cd riskwatch
docker compose up -d --build

# Batch layer (run once)
docker compose exec app python batch/preprocess.py
docker compose exec app python batch/train_models.py

# Live system (run in 3 separate terminals)
docker compose exec app python streaming/stream_predict.py
docker compose exec app python streaming/producer.py --rate 2
docker compose exec app streamlit run dashboard/app.py --server.address 0.0.0.0
```

Open http://localhost:8501 to see the dashboard.

## Limitations and Future Work

- The data comes from a public health survey, not clinical tests, so RiskWatch is a screening and prioritisation tool and not a diagnosis tool.
- The producer replays dataset records to simulate a live feed. A real deployment would connect to hospital or wearable data sources.
- Possible next steps: email or SMS alerts for high-risk patients, model explainability with feature contributions per patient, scaling Kafka and Spark to a multi-node cluster, and adding more diseases.

## Author

**Dhanusri R S**
B.Tech Artificial Intelligence and Data Science, Rajalakshmi Engineering College, Chennai
[GitHub](https://github.com/DhanusriRS2006)

## Disclaimer

RiskWatch is an academic project built for learning. It is not a medical device and must not be used for real diagnosis or treatment decisions.
