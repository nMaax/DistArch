# Hadoop Installation Guide

## 1. Java Version Management
Arch Linux uses `archlinux-java` to switch between versions. For these labs, you must ensure Java 8 is active.

```fish
# List installed versions
archlinux-java status

# Set Java 8 as the default
sudo archlinux-java set java-8-openjdk
```

---

## 2. Environment Configuration
Add this to your `~/.config/fish/config.fish` (or `~/.config/fish/conf.d/java.fish`). This ensures that `JAVA_HOME` always follows whatever version you set with the command above.

```fish
# Set JAVA_HOME to the Arch default symlink
set -gx JAVA_HOME /usr/lib/jvm/default

# Optimizing Maven memory for Big Data tasks
set -gx MAVEN_OPTS "-Xms512m -Xmx2048m"
```

---

## 3. The Development Workflow (CLI)
Since you are not installing Hadoop globally, use Maven's lifecycle to build and test.

### Build the Project
Run this in the root of your project to compile your Driver, Mapper, and Reducer classes[cite: 79, 80, 81, 110].
```fish
mvn clean package
```
This produces your "thin" JAR in `target/MapReduceProject-1.0.0.jar`.

### Run Locally (Simulation)
Since you don't have the `hadoop` command, use Maven to execute your code. Maven will automatically pull the required Hadoop libraries from your `~/.m2` folder and put them in the classpath for you.

```fish
mvn exec:java \
  -Dexec.mainClass="it.polito.bigdata.hadoop.DriverBigData" \
  -Dexec.args="2 example_data ex_out test_prefix"
```
* **mainClass**: The full path to your Driver.
* **args**: Your program arguments (separated by spaces, no commas).

---

## 4. Preparing for the Cluster
When you are ready to upload to `jupyter.polito.it`, you need a JAR that contains only your code, not the libraries.

### Verify the JAR
Confirm the JAR is "thin" (contains only your `.class` files):
```fish
jar -tf target/MapReduceProject-1.0.0.jar | grep it/polito
```

### Upload to Cluster
Use `scp` to move your JAR to the Polito environment:
```fish
scp target/MapReduceProject-1.0.0.jar your_username@jupyter.polito.it:~/labs/
```

---

## 5. Summary of Key Commands
| Task | Command |
| :--- | :--- |
| **Switch Java** | `sudo archlinux-java set java-8-openjdk` |
| **Clean & Build** | `mvn clean package` |
| **Local Test** | `mvn exec:java -Dexec.mainClass="..." -Dexec.args="..."` |
| **Check Output** | `cat ex_out/part-r-00000` |

> **Note on log4j**: You will likely see `WARN` messages about `log4j` when running locally. You can safely ignore these; they do not affect the execution of your MapReduce logic.
