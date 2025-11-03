# ETL BMI Data Pipeline

A simple, production-style **ETL pipeline** in Python that:
- **Extracts** data from multiple formats (CSV, JSON Lines, XML)
- **Transforms** height (inches → meters) and weight (pounds → kilograms), with numeric cleaning
- **Loads** a unified CSV and writes progress **logs** — all using `pandas` and the standard library

---

## ✨ Features

- ✅ Reads **CSV**, **JSON (lines)**, and **XML**
- ✅ Robust numeric conversion with `errors='coerce'`
- ✅ Unit conversion: inches → meters, pounds → kilograms
- ✅ Timestamped logging to `Target/log_file.txt`
- ✅ Clean, configurable paths (Source/Target folders)

---

## 🗂 Project Structure
```
ETL_BMI_data/
├─ ETL_script.py
├─ README.md
├─ source/
│ ├─ sample.csv
│ ├─ sample.json
│ └─ sample.xml
└─ Target/ # <-- outputs are written here
    ├─ transformed_data.csv
    └─ log_file.txt
```

---

## 🛠 Requirements

- Python 3.9+ (tested on 3.11/3.13)
- Packages:
  - `pandas`

Install with:

```
pip install -r requirements.txt
```
