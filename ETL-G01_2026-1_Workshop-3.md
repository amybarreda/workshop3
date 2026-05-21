[cite_start]**FACULTY OF ENGINEERING AND BASIC SCIENCES** [cite: 3]
[cite_start]**ACADEMIC PROGRAM: DATA ENGINEERING AND ARTIFICIAL INTELLIGENCE** [cite: 3]
[cite_start]**COURSE: ETL (G01)** [cite: 4]
[cite_start]**Workshop-3: Streaming ETL with Apache Kafka and Machine Learning** [cite: 5]

---

## [cite_start]1. Introduction [cite: 6]
[cite_start]This workshop focuses on the transition from traditional batch ETL pipelines to event-driven streaming pipelines. [cite: 7] 

[cite_start]Students have already worked with: [cite: 8]
* [cite_start]ETL pipelines in Python [cite: 9]
* [cite_start]Data cleaning and transformation [cite: 10]
* [cite_start]Data quality concepts [cite: 11]
* [cite_start]Dimensional modeling [cite: 12]
* [cite_start]Apache Airflow [cite: 12]
* [cite_start]Batch orchestration [cite: 13]
* [cite_start]Data Warehouses [cite: 14]

[cite_start]This workshop introduces: [cite: 15]
* [cite_start]Streaming data pipelines [cite: 16]
* [cite_start]Apache Kafka [cite: 16]
* [cite_start]Event-driven processing [cite: 16]
* [cite_start]Real-time ML inference [cite: 16]
* [cite_start]Streaming analytics [cite: 17]

---

## [cite_start]2. Workshop Goal [cite: 18]
[cite_start]Design and implement a streaming ETL pipeline capable of generating real-time predictions using Apache Kafka and a pre-trained machine learning model. [cite: 19]

---

## [cite_start]3. Learning Objectives [cite: 20]
[cite_start]By the end of this workshop, students will be able to: [cite: 21]
1. [cite_start]Integrate heterogeneous datasets into a unified analytical schema. [cite: 22]
2. [cite_start]Build a batch ETL pipeline for machine learning preparation. [cite: 23]
3. [cite_start]Train and serialize a regression model. [cite: 24]
4. [cite_start]Implement a Kafka producer that streams events. [cite: 25]
5. [cite_start]Implement a Kafka consumer that performs real-time inference. [cite: 26]
6. [cite_start]Validate streaming events before prediction. [cite: 27]
7. [cite_start]Store prediction results in a database. [cite: 29]
8. [cite_start]Build analytical visualizations using prediction results. [cite: 30]

---

## [cite_start]4. General Architecture [cite: 31]

[cite_start]**Offline Process** [cite: 32]
* [cite_start]Historical CSV Files [cite: 33]
* [cite_start]↓ [cite: 34]
* [cite_start]Data Profiling (EDA + Cleaning + Schema Harmonization) [cite: 35]
* [cite_start]↓ [cite: 36]
* [cite_start]Feature Engineering [cite: 37]
* [cite_start]↓ [cite: 38]
* [cite_start]Train Regression Model [cite: 39]
* [cite_start]↓ [cite: 40]
* [cite_start]Save model.pkl [cite: 41]

[cite_start]**Streaming Process** [cite: 42]
* [cite_start]Historical CSV Files [cite: 43]
* [cite_start]↓ [cite: 44]
* [cite_start]Kafka Producer (stream raw data) [cite: 45, 46]
* [cite_start]↓ [cite: 47]
* [cite_start]Kafka Topic [cite: 48]
* [cite_start]↓ [cite: 49]
* [cite_start]Kafka Consumer [cite: 50]
* [cite_start]↓ [cite: 51]
* [cite_start]Store raw evento [cite: 52]
* [cite_start]↓ [cite: 53]
* [cite_start]Validate Event Schema [cite: 54]
* [cite_start]↓ [cite: 55]
* [cite_start]Load model.pkl [cite: 56]
* [cite_start]↓ [cite: 57]
* [cite_start]Generate Prediction [cite: 58]
* [cite_start]↓ [cite: 59]
* [cite_start]Store Prediction Results [cite: 60]
* [cite_start]↓ [cite: 61]
* [cite_start]Dashboard & KPIs [cite: 62]

---

## [cite_start]5. Dataset Description [cite: 63]
[cite_start]You are provided with multiple CSV files containing World Happiness data from different years. [cite: 69, 70]

[cite_start]**Files:** [cite: 71]
* [cite_start]2015.csv [cite: 72]
* [cite_start]2016.csv [cite: 73]
* [cite_start]2017.csv [cite: 74]
* [cite_start]2018.csv [cite: 76]
* [cite_start]2019.csv [cite: 77]

[cite_start]**The datasets contain:** [cite: 78]
* [cite_start]Happiness score [cite: 79]
* [cite_start]GDP [cite: 80]
* [cite_start]Health indicators [cite: 81]
* [cite_start]Family/social support [cite: 82]
* [cite_start]Freedom indicators [cite: 83]
* [cite_start]Corruption perception [cite: 84]
* [cite_start]Generosity [cite: 85]
* [cite_start]Country information [cite: 86]

[cite_start]**Important:** [cite: 87]
The datasets do NOT share exactly the same schema. [cite_start]You must analyze and harmonize the datasets before integrating them. [cite: 88]

---

## [cite_start]6. Activities [cite: 89]

### [cite_start]PART A - Data Profiling and Machine Learning [cite: 90]
[cite_start]Objective: Build a batch ETL pipeline capable of preparing data for machine learning. [cite: 91]

[cite_start]**Step 1 - Exploratory Data Analysis (EDA)** [cite: 92]
[cite_start]Perform EDA on all datasets. [cite: 93] [cite_start]You must analyze: [cite: 94]
* [cite_start]Missing values [cite: 95]
* [cite_start]Duplicated records [cite: 96]
* [cite_start]Inconsistent column names [cite: 97]
* [cite_start]Inconsistent data types [cite: 98]
* [cite_start]Schema differences between years [cite: 99]
* [cite_start]Potential outliers [cite: 100]

[cite_start]Deliverables: [cite: 101]
* [cite_start]EDA notebook [cite: 104]
* [cite_start]Data quality observations [cite: 105]
* [cite_start]Unified schema proposal [cite: 105]

[cite_start]**Step 2 - Data Cleaning and Harmonization** [cite: 106]
[cite_start]Design a unified analytical schema. [cite: 107] [cite_start]You must: [cite: 108]
* [cite_start]Standardize column names [cite: 109]
* [cite_start]Standardize data types [cite: 110]
* [cite_start]Remove or handle missing values [cite: 111]
* [cite_start]Resolve schema inconsistencies [cite: 112]
* [cite_start]Merge datasets into a unified dataset [cite: 113]

[cite_start]Important: [cite: 114] [cite_start]You must justify your cleaning decisions. [cite: 115]

[cite_start]**Step 3 - Feature Engineering** [cite: 116]
[cite_start]Prepare features for machine learning. [cite: 117] [cite_start]Generate descriptive statistics and visualizations to explore relationships among variables (e.g., GDP, social support, life expectancy). [cite: 118] [cite_start]Select and preprocess the features that are most relevant for predicting the happiness score. [cite: 119]

[cite_start]Requirements: [cite: 120]
* [cite_start]Select meaningful features [cite: 121]
* [cite_start]Justify feature selection [cite: 122]
* [cite_start]Avoid target leakage [cite: 123]
* [cite_start]Handle categorical data if necessary [cite: 124]
* [cite_start]Normalize or scale features if required [cite: 125]

[cite_start]Important: [cite: 126] The focus of this workshop is pipeline integration, not model optimization. [cite_start]Use a simple regression model. [cite: 127]

[cite_start]**Step 4 - Train Regression Model** [cite: 128]
[cite_start]Train a regression model capable of predicting happiness score. [cite: 129]
[cite_start]Suggested models: [cite: 130]
* [cite_start]Linear Regression [cite: 131]
* [cite_start]Random Forest Regressor [cite: 132]
* [cite_start]Decision Tree Regressor [cite: 133]

[cite_start]You must: [cite: 135]
* Split the data into training and testing sets. [cite_start]Suggestion: [cite: 136]
    * [cite_start]70% training data [cite: 138]
    * [cite_start]30% testing data [cite: 139]
* [cite_start]Train the model [cite: 140]
* [cite_start]Evaluate the model [cite: 141]
* [cite_start]Save the trained model as: [cite: 142]
    * [cite_start]`model.pkl` [cite: 144]

[cite_start]Suggested metrics: [cite: 145]
* [cite_start]MAE [cite: 146]
* [cite_start]RMSE [cite: 147]
* [cite_start]R2 [cite: 148]

[cite_start]Deliverables: [cite: 149]
* [cite_start]Training notebook [cite: 150]
* [cite_start]Evaluation metrics [cite: 151]
* [cite_start]Serialized model [cite: 152]

### [cite_start]PART B - Streaming ETL with Apache Kafka [cite: 155]
[cite_start]Objective: [cite: 156] [cite_start]Implement a streaming inference pipeline using Kafka. [cite: 157]

[cite_start]**Kafka Requirements** [cite: 158]
[cite_start]You must use: [cite: 159]
* [cite_start]Apache Kafka [cite: 160]
* [cite_start]Python producer [cite: 161]
* [cite_start]Python consumer [cite: 162]

[cite_start]Recommended environment: [cite: 163]
* [cite_start]Docker Compose [cite: 164]

[cite_start]**Kafka Architecture** [cite: 165]
[cite_start]Producer [cite: 166] [cite_start]-> Kafka Topic [cite: 168] [cite_start]-> Consumer [cite: 170]

[cite_start]**Step 5 - Kafka Producer** [cite: 173]
[cite_start]Implement a producer that streams records. [cite: 174]
[cite_start]Requirements: [cite: 175]
* [cite_start]Stream records one by one [cite: 176]
* [cite_start]Serialize events as JSON [cite: 177]
* [cite_start]Send events to a Kafka topic [cite: 178]
* [cite_start]Required topic name: `happiness-predictions` [cite: 179]

[cite_start]Required JSON format: [cite: 180]
```json
{
"country": "Colombia",
"year": 2019,
"gdp": 1.2,
"family": 0.8,
"health": 0.9,
"freedom": 0.6,
"generosity": 0.3,
"corruption": 0.1,
"actual_happiness_score": 6.2
}

http://googleusercontent.com/immersive_entry_chip/0

---

## 9. Deliverables [cite: 304]
**1. GitHub Repository** [cite: 305]
The repository must include: [cite: 306]
* ETL notebooks [cite: 308]
* Producer and consumer scripts [cite: 309]
* Serialized model [cite: 310]
* SQL scripts [cite: 311]
* Dashboard files/screenshots [cite: 312]
* requirements.txt [cite: 313]
* README.md [cite: 314]

**2. README.md (MANDATORY)** [cite: 315]
The README must include: [cite: 316]
* Project description [cite: 317]
* Architecture explanation [cite: 318]
* Data cleaning decisions [cite: 318]
* Feature engineering decisions [cite: 318]
* Kafka pipeline explanation [cite: 318]
* Database schema [cite: 318]
* Dashboard explanation [cite: 319]
* Execution instructions [cite: 320]

**3. Dashboard** [cite: 321]
Include: [cite: 322]
* screenshots [cite: 323]
* dashboard file or link [cite: 324]
* KPI explanations [cite: 325]

---

## 10. Evaluation Criteria [cite: 326]
**Project** [cite: 327]

| Criteria | Weight | [cite: 328]
| :--- | :--- | 
| Data Integration & Cleaning | 1.0 | [cite: 328]
| Feature Engineering | 0.5 | [cite: 328]
| ML Pipeline | 0.5 | [cite: 328]
| Kafka Producer | 0.5 | [cite: 331]
| Kafka Consumer | 0.5 | [cite: 331]
| Event Validation | 0.5 | [cite: 331]
| Database Design & Loading | 0.5 | [cite: 331]
| Dashboard & KPIs | 0.5 | [cite: 331]
| Documentation & Reproducibility | 0.5 | [cite: 331]

Project: 70% [cite: 333]
Presentation (Clarity and Structure, Communication and Professionalism): 30% [cite: 334]

**Key Insight** [cite: 335]
This workshop is NOT focused on maximizing ML accuracy. [cite: 336]
The main goal is: [cite: 337] Building an integrated streaming ETL pipeline capable of generating real-time predictions. [cite: 338, 339]

Focus on: [cite: 340]
* clean architecture [cite: 341]
* reproducibility [cite: 342]
* pipeline reliability [cite: 343]
* data consistency [cite: 344]
* streaming integration [cite: 345]
* rather than model complexity. [cite: 346]