# Geo-Spatial-Hotspot-Detection-with-Spark-and-Scala


# Assignment-3-sagar158


### ✅ `README.md` for  Hotspot Analysis

```markdown
# CSE511 – Assignment 3: Hotspot Analysis using Apache Spark

## 📚 Course: Data Processing at Scale (Spring 2025)  
**Instructor**: [Professor Name]  
**Student**: [Your Full Name]  
**ASU ID**: [Your ASU ID]  

---

## 📌 Project Overview

This assignment implements two spatial analysis tasks using Apache Spark:

1. **Hotzone Analysis**:  
   Performs a spatial range join between NYC taxi pickup points and a grid of rectangular zones.  
   The output shows how many pickups fall within each zone.

2. **Hotcell Analysis**:  
   Applies statistical hotspot detection using the Getis-Ord Gi* score.  
   The output lists the top 50 spatio-temporal cells with the highest z-scores.

---

## 🧠 Technologies Used

- Apache Spark 2.4.5  
- Scala 2.11  
- sbt (Scala Build Tool)  
- Docker for isolated environment  
- Spark SQL APIs (for DataFrame processing)

---

## 🗂️ Repository Structure

```bash
.
├── src/
│   └── main/
│       └── scala/
│           └── cse511/
│               ├── HotzoneAnalysis.scala        # Hotzone logic
│               ├── HotzoneUtils.scala           # Hotzone helpers
│               ├── HotcellAnalysis.scala        # Hotcell logic
│               ├── HotcellUtils.scala           # Hotcell helpers
├── target/
│   └── scala-2.11/
│       └── cse511-assembly-0.1.0-SNAPSHOT.jar   # Compiled jar
```

---

## ⚙️ How to Run (Locally with Docker)

> Make sure Docker is installed and running.

### 1. Clone the repo

```bash
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>
```

### 2. Start Docker container

```bash
docker run -it --rm -v "${PWD}:/root/cse511" -w /root/cse511 veedata/cse511-assignment3 bash
```

### 3. Build the JAR (if needed)

```bash
sbt clean assembly
```

### 4. Run Spark job

```bash
spark-submit --class cse511.Main target/scala-2.11/cse511-assembly-0.1.0-SNAPSHOT.jar result/output \
hotzoneanalysis src/resources/point_hotzone.csv src/resources/zone-hotzone.csv \
hotcellanalysis src/resources/yellow_tripdata_2009-01_point.csv
```


## ✅ Results

- **Hotzone output**: `(rectangle_id, count)` pairs saved in `/result/output0/`
- **Hotcell output**: `(x, y, z)` cells sorted by Getis-Ord score in `/result/output1/`

These results have been cross-verified with official solution files from the Dropbox link provided in class.


## 📝 Notes

- All `.csv` inputs must be placed inside `src/resources/`
- The output is saved as Spark part files (e.g., `part-00000`)


## 📦 Submission Files

- ✅ 4 Scala source files (`HotzoneAnalysis`, `HotzoneUtils`, `HotcellAnalysis`, `HotcellUtils`)
- ✅ Final compiled JAR: `cse511-assembly-0.1.0-SNAPSHOT.jar`


## 📧 Contact

If you have any questions about this implementation, feel free to contact me via sagar158@asu.edu
