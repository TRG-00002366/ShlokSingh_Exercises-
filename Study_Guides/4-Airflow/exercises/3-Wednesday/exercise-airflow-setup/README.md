# Lab: Setting Up Your Airflow Environment

## Overview
**Type:** Implementation (Code Lab)  
**Duration:** 2-3 hours  
**Mode:** Individual

## Learning Objectives
By completing this exercise, you will:
- Set up a local Airflow environment using Docker Compose
- Understand the role of each Airflow component
- Verify that all services are running correctly
- Access and navigate the Airflow web UI

## Prerequisites
- Docker Desktop installed and running
- Basic understanding of Docker concepts
- Terminal/command line familiarity

---

## The Scenario

Your team is starting a new data engineering project that requires workflow orchestration. Before building any pipelines, you need to set up a local development environment for Apache Airflow. Your task is to get Airflow running locally and verify all components are healthy.

---

## Core Tasks

### Task 1: Configure Docker Compose (30 minutes)

Navigate to `starter_code/` and complete the Docker Compose file.

1. Open `docker-compose.yml`
2. Complete the **TODO** sections:
   - Configure the PostgreSQL database service
   - Set up the Airflow webserver with correct port mapping
   - Configure the Airflow scheduler
   - Set up shared volumes for DAGs and logs

**Hints:**
- The official Airflow image is `apache/airflow:2.7.3`
- PostgreSQL should use image `postgres:13`
- Webserver needs port 8080 exposed
- All Airflow services share common environment variables

### Task 2: Start the Environment (15 minutes)

1. Open a terminal in the `starter_code/` directory
2. Run: `docker-compose up -d`
3. Check container status: `docker-compose ps`
4. Wait for all containers to become healthy

**Expected Output:**
All containers should show status "healthy" or "running"

### Task 3: Verify Component Health (20 minutes)

Complete the verification checklist:

1. **Database Connection:**
   ```bash
   docker-compose exec airflow-webserver airflow db check
   ```
   Expected: "Connection successful"

2. **Webserver Access:**
   - Open browser to `http://localhost:8080`
   - Log in with username `airflow` and password `airflow`

3. **Scheduler Status:**
   ```bash
   docker-compose logs airflow-scheduler | tail -20
   ```
   Look for: "Scheduler started" or heartbeat messages

4. **Health Endpoint:**
   ```bash
   curl http://localhost:8080/health
   ```
   Expected: JSON showing "healthy" status

### Task 4: Explore the UI (30 minutes)

Navigate through the Airflow UI and document what you find:

1. **DAGs Page:** How many example DAGs are visible? (Note: may be 0 if examples disabled)
2. **Admin Menu:** Find the Connections page. What connection types are available?
3. **Browse Menu:** What information is shown under "DAG Runs"?
4. **Security Menu:** How many roles exist by default?

Record your findings in `answers.md`.

---

## Stretch Goals

1. **Custom Configuration:** Modify `airflow.cfg` settings via environment variables to:
   - Change the DAG folder scan interval to 30 seconds
   - Disable loading example DAGs (if enabled)

2. **Add a Service:** Add Redis to your Docker Compose (for future Celery executor use)

3. **Multiple Workers:** Configure the LocalExecutor with parallelism of 4

---

## Definition of Done

- [ ] All Docker containers are running and healthy
- [ ] Airflow UI is accessible at localhost:8080
- [ ] Login with airflow/airflow credentials works
- [ ] Health endpoint returns successful status
- [ ] `answers.md` contains UI exploration findings

---

## Submission

1. Ensure your `docker-compose.yml` has all TODOs completed
2. Complete `answers.md` with your findings
3. Take a screenshot of the healthy Airflow UI dashboard
4. Commit all files to your repository

---

## Troubleshooting Tips

| Issue | Solution |
|-------|----------|
| Port 8080 already in use | Change the host port in docker-compose.yml |
| Containers keep restarting | Check logs: `docker-compose logs [service]` |
| Database connection failed | Ensure PostgreSQL is healthy before Airflow starts |
| UI shows 502 error | Wait 60 seconds for initialization to complete |
| Scheduler not starting | Check for database migration issues |

---

## Resources

- [Airflow Docker Documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- Written Content: `setting-up-airflow.md`
- Demo Reference: `demo_airflow_setup/`
