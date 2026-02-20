# CLAUDE.md

## Project Overview

A collection of small Big Data infrastructure utility tools for Hadoop/HDFS and ZooKeeper operations. The repository contains independent sub-projects, each solving a specific operational need.

## Repository Structure

```
mytools/
├── CLAUDE.md              # This file
├── README.md              # Project overview (Chinese)
├── .gitignore             # Ignores *.class files
├── dfsusage/              # HDFS directory usage web viewer
│   ├── README.md          # Build and run instructions
│   ├── pom.xml            # Maven build configuration
│   └── src/main/
│       ├── scala/com/meituan/mthdp/dfsusage/
│       │   └── DfsUsage.scala   # Jetty handler + entry point
│       └── resources/
│           └── log4j.properties # Logging config (INFO level, console)
└── zookeeper/             # ZooKeeper node utilities
    └── ZKOperation.java   # Recursive ZK node traversal tool
```

## Sub-Projects

### dfsusage

A Scala web application using embedded Jetty that provides a browser-based view of HDFS directory structure and disk usage statistics.

- **Language:** Scala 2.10.4
- **Build system:** Maven
- **Key dependencies:** Hadoop 2.2.0, Jetty 8.1.14, Meituan security library
- **Package:** `com.meituan.mthdp.dfsusage`
- **Entry point:** `DfsUsage` object (`main` method) in `DfsUsage.scala`
- **Default port:** 8989
- **Architecture:** Single `AbstractHandler` implementation that maps URL paths directly to HDFS paths, renders HTML tables with Scala XML literals

**Build:**
```bash
cd dfsusage
mvn clean compile assembly:single
```

**Run** (requires Hadoop environment):
```bash
$HADOOP_HOME/bin/hadoop jar dfsusage-1.0-SNAPSHOT.jar com.meituan.mthdp.dfsusage.DfsUsage
```

### ZKOperation

A standalone Java CLI tool for recursive traversal and manipulation of ZooKeeper nodes.

- **Language:** Java
- **Build system:** Manual `javac` with ZooKeeper classpath
- **Key dependency:** Apache ZooKeeper 3.3.5
- **Entry point:** `ZKOperation.main`
- **Default connection:** `localhost:2181`, 30-second session timeout
- **Operations:** `show` (list paths), `showdetail` (show data + stat), `delete` (recursive delete)
- **Architecture:** Strategy pattern via `NodeOperation` interface; stack-based iterative postorder tree traversal

**Build:**
```bash
export ZOOBINDIR=$ZOOBINDIR
javac -cp `$ZOOBINDIR/zkEnv.sh | awk -F= '{print $2}'` ZKOperation.java
```

**Run:**
```bash
java ZKOperation [show|showdetail|delete] <zk-path>
```

## Development Notes

### Languages and Conventions

- **Scala code** follows standard Scala idioms with XML literals for HTML generation
- **Java code** uses inner classes and the strategy pattern for extensibility
- **Package naming** follows `com.meituan.mthdp.*` convention for Maven-managed projects
- ZKOperation has no package declaration (standalone script style)

### Build

- Only the `dfsusage` sub-project uses Maven; ZKOperation is compiled standalone
- Maven produces an uber JAR (`jar-with-dependencies`) for deployment
- No multi-module Maven setup; each tool is independent

### Testing

- No automated tests exist in this repository
- No test frameworks are configured
- Testing is done manually against live Hadoop/ZooKeeper clusters

### Linting and Formatting

- No linting or formatting tools are configured
- No pre-commit hooks

### CI/CD

- No CI/CD pipeline is configured

## Key Patterns for AI Assistants

1. **Each sub-project is self-contained.** New tools should be added as new top-level directories with their own build configuration.
2. **Keep tools simple.** These are operational utilities, not production services. Compact, single-purpose implementations are preferred.
3. **Update the root README.md** when adding new tools (note: it is written in Chinese).
4. **The `.gitignore` is minimal** (only `*.class`). Add patterns as needed for new tool types.
5. **No shared dependencies or parent POM.** Each sub-project manages its own dependencies independently.
