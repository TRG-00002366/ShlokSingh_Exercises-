# Exercise: Spark Installation and Setup

## Overview
In this exercise, you will install Apache Spark on your local machine, configure the necessary environment variables, and verify your setup by running a simple PySpark job.

**Duration:** 45-60 minutes  
**Mode:** Individual

---

## Prerequisites
- Python 3.8 or later installed
- Administrator access to install software
- At least 4GB of free disk space

---

## Core Tasks

### Task 1: Install Java (if not already installed)
Spark requires Java 8, 11, or 17.

1. Check if Java is installed:
   ```bash
   java -version
   ```

2. If not installed, download and install from:
   - [Oracle JDK](https://www.oracle.com/java/technologies/downloads/)
   - Or [OpenJDK](https://adoptium.net/)

3. Set `JAVA_HOME` environment variable to your Java installation directory.

### Task 2: Download and Install Spark

1. Go to [Apache Spark Downloads](https://spark.apache.org/downloads.html)
2. Select:
   - Spark release: Latest stable (3.5.x)
   - Package type: Pre-built for Apache Hadoop
3. Download the `.tgz` file
4. Extract to a location (e.g., `C:\spark` or `/opt/spark`)

### Task 3: Configure Environment Variables

Set the following environment variables:

| Variable | Value (Example) |
|----------|-----------------|
| `SPARK_HOME` | `C:\spark\spark-3.5.0-bin-hadoop3` |
| `PATH` | Add `%SPARK_HOME%\bin` |
| `PYSPARK_PYTHON` | `python` (or full path to Python) |

**Windows:**
1. Open System Properties > Advanced > Environment Variables
2. Add/modify the variables above
3. Restart your terminal

**Linux/Mac:**
```bash
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

### Task 4: Install PySpark

```bash
pip install pyspark
```

### Task 5: Verify Installation

1. Open a terminal and run:
   ```bash
   spark-shell --version
   ```
   You should see Spark version information.

2. Start PySpark shell:
   ```bash
   pyspark
   ```
   You should see the Spark banner and a `>>>` prompt.

3. In the PySpark shell, run:
   ```python
   sc.parallelize([1, 2, 3, 4, 5]).reduce(lambda a, b: a + b)
   ```
   Expected output: `15`

4. Exit the shell:
   ```python
   exit()
   ```

---

## Deliverables

Create a file named `setup_verification.txt` containing:

1. Output of `java -version`
2. Output of `spark-shell --version`
3. Screenshot or output of the PySpark test (sum of 1-5 = 15)
4. Any issues you encountered and how you resolved them

---

## Definition of Done

- [ ] Java is installed and JAVA_HOME is set
- [ ] Spark is downloaded and extracted
- [ ] SPARK_HOME and PATH are configured
- [ ] PySpark is installed via pip
- [ ] `spark-shell --version` runs without errors
- [ ] PySpark shell starts and runs test calculation
- [ ] `setup_verification.txt` is created

---

## Troubleshooting Tips

**"java is not recognized"**
- Ensure JAVA_HOME is set correctly
- Restart your terminal after setting environment variables

**"pyspark command not found"**
- Ensure SPARK_HOME/bin is in your PATH
- Try running `python -m pyspark` instead

**"Python version mismatch"**
- Set PYSPARK_PYTHON to the correct Python executable

---

## Additional Resources
- [Spark Installation Guide](https://spark.apache.org/docs/latest/)
- Written Content: `spark-setup.md`
- Demo: `demo_spark_setup_verification.py`
