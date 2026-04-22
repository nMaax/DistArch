# PySpark Installation Guide: Arch Linux + uv + direnv

This guide is optimized for Arch Linux users who need to run PySpark locally using `uv` for package management and `direnv` for Java version isolation (keeping your Hadoop/Java 8 setup safe).

## 1. System Prerequisites
Spark 4.x requires Java 17 or 21. Java 26 is currently too new for the Spark startup scripts.
```bash
sudo pacman -S jdk21-openjdk uv direnv procps-ng
```

## 2. Project Setup
Create your workspace and initialize the environment:
```bash
mkdir spark_project && cd spark_project
uv init
uv add pyspark jupyterlab
```

## 3. Environment Isolation (`.envrc`)
Create a file named `.envrc` in the project root. This ensures that when you enter this folder, your system switches to Java 21 and configures Spark without touching your global settings.

**File: `.envrc`**
```bash
# 1. Java Isolation
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH="$JAVA_HOME/bin:$PATH"

# 2. Python / uv Integration
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"

# 3. Spark 4.x + Modern Java Fix
# This allows Spark to access internal Java modules required for memory management.
export PYSPARK_SUBMIT_ARGS="--driver-java-options '--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED' pyspark-shell"
```
*Run `direnv allow` after saving.*

---

## 4. Running JupyterLab
Because of the `.envrc`, you simply launch:
```bash
uv run jupyter lab
```

## 5. Spark Initialization (Notebook)
No environment hacks are needed inside the notebook anymore. Just start the session:

```python
from pyspark.sql import SparkSession

# The host/bindAddress configs prevent Arch hostname resolution errors
spark = SparkSession.builder \
    .appName("ArchLocalSpark") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

print(f"✅ Spark {spark.version} is active on Java {spark._jvm.java.lang.System.getProperty('java.version')}")
```

---

## Troubleshooting Tips
* **Gateway Errors:** Usually means the hostname isn't resolving. Ensure the `127.0.0.1` configs are in your builder.
* **ClassVersion Errors:** Double-check that `java -version` returns 21 (Class 61). If it says 55, it's finding Java 11.
* **Hadoop Jobs:** Since we used `.envrc`, your Hadoop jobs in other folders will still use your global Java 8 default.

Now that your workstation is fully operational, are you diving into RDDs or heading straight for the DataFrame API?
