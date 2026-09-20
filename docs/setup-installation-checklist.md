# Setup and Installation Checklist

## Required Tools

This dissertation implementation needs the following local tools:

- Docker Desktop with WSL 2 backend.
- Docker Compose.
- Java JDK 21 LTS preferred.
- Maven.
- Git.
- Python for experiment analysis.

## Current Machine Status

Checked on the local Windows machine from the repository workspace.

### Installed

| Tool | Status | Notes |
| --- | --- | --- |
| Docker CLI | Installed | `Docker version 29.6.1` |
| Docker Desktop | Installed | `Docker Desktop 4.80.0.232116` |
| Docker Compose | Installed | `docker-compose --version` reports v5.5.1 |
| WSL 2 | Installed | Default distribution: `docker-desktop`; default version: `2` |
| Java | Installed | JDK 21 is installed and configured through `JAVA_HOME`; Java 25 and Java 11 also exist |
| Maven | Installed | Apache Maven 3.8.9 |
| Git | Installed | Git 2.51.0 |
| Python | Installed | Python 3.14.0 |
| winget | Installed | winget 1.29.290 |

### Recommended Additions

No required tool installations remain before starting the Kafka environment.

## Docker Status

Docker Desktop is installed and running. Docker daemon calls are now healthy.

Verified command:

```text
docker ps
```

Current result shows two existing non-dissertation containers:

```text
aceest-fitness-api-prod
aceest-fitness-api-dev
```

Docker Compose is available through the legacy command:

```text
docker-compose --version
```

Result:

```text
Docker Compose version v5.5.1
```

### Docker Context

Run:

```powershell
docker context ls
```

Expected active context:

```text
desktop-linux *
```

If needed:

```powershell
docker context use desktop-linux
```

## Recommended Java Setup

The machine has JDK 21 installed:

```text
openjdk version "21.0.12.1" 2026-08-18 LTS
```

JDK 21 location:

```powershell
C:\Program Files\Eclipse Adoptium\jdk-21.0.12.101-hotspot
```

`JAVA_HOME` has been persisted with:

```powershell
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-21.0.12.101-hotspot"
```

After restarting the IDE, the user verified that both `java -version` and
`mvn -version` report Java 21.0.12.1, and `JAVA_HOME` points to the JDK above.
For any older terminal that still uses another Java version:

```powershell
$env:JAVA_HOME='C:\Program Files\Eclipse Adoptium\jdk-21.0.12.101-hotspot'
$env:Path="$env:JAVA_HOME\bin;$env:Path"
java -version
mvn -version
```

Verified Maven with explicit JDK 21:

```text
Apache Maven 3.8.9
Java version: 21.0.12.1, vendor: Eclipse Adoptium
```

## Minimum Setup Needed Before Implementation

Before creating the Kafka Docker environment, these commands must work:

```powershell
docker ps
docker-compose --version
java -version
mvn -version
git --version
python --version
```

Current status:

- Docker works.
- Docker Compose works through `docker-compose`.
- JDK 21 is installed.
- Maven works with JDK 21 when `JAVA_HOME` is active.
- Git and Python are installed.

Installation verification is complete. Continue with [Step 1: Local Kafka Environment](kafka-local-setup.md).
