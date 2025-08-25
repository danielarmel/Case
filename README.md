# Case
# 🐾 Dog Breeds Data Project

This project uses dbt to transform raw dog breed data into clean, structured dimensional and fact models, enabling insight generation via Looker Studio. The analysis is based on characteristics such as lifespan, weight, height, temperament, and family-friendliness.

---

## 📊 Insights from the Dashboard

### 🐶 Which breeds live the longest?

The bar chart shows the **Top 10 breeds with the longest predicted lifespans**, based on averages from lifespan intervals.  
The **West Highland White Terrier** leads with an average lifespan of nearly 18 years. Other small breeds such as the **Maltese**, **Pekingese**, and **Rat Terrier** are also among the longest-living. This supports the known correlation between small size and longer life expectancy in dogs.

### ⚖️ Weight Distribution

Breeds are ranked by **average weight**, calculated as the mean of their min–max weight range.  
The **Saint Bernard**, **Boerboel**, and **Great Dane** are among the heaviest breeds, each exceeding 65 kg. These larger breeds generally have shorter lifespans, which aligns with the tradeoff seen when comparing this data with lifespan values.

### 👨‍👩‍👧‍👦 Top Temperaments of Family-Friendly Dogs

For family-friendly dogs (defined via specific temperament traits), the most common descriptors are:
- **Affectionate**
- **Friendly**
- **Strong Willed**
- **Clownish**

The **American Bully** stands out, appearing multiple times with a versatile range of positive temperaments.

---

## Tech Stack

- **dbt** for transformation and modeling
- **BigQuery** as the data warehouse
- **Looker Studio** for visualisation
- **GitHub Actions** for CI/CD and deployment
- **YAML + Jinja** for parameterisation and configuration


---


Badges

[![Deploy to Production](https://github.com/danielarmelzamani/Case/actions/workflows/deploy.yml/badge.svg)](https://github.com/danielarmelzamani/Case/actions/workflows/deploy.yml)