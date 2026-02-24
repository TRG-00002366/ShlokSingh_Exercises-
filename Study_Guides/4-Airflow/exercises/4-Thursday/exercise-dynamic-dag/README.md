# Pair Programming: Building a Dynamic DAG Factory

## Overview
**Type:** Implementation (Code Lab)  
**Duration:** 3-4 hours  
**Mode:** Pair Programming (Driver/Navigator)

## Learning Objectives
By completing this exercise as a pair, you will:
- Generate DAG tasks dynamically from a JSON configuration file
- Use TaskGroups to organize generated tasks
- Practice pair programming techniques
- Build a reusable DAG pattern

---

## Pair Programming Protocol

### Roles

**Driver:**
- Controls the keyboard and writes code
- Focuses on implementation details
- Asks questions when stuck

**Navigator:**
- Reviews code as it's written
- Thinks about the bigger picture
- Catches bugs and suggests improvements
- Keeps track of requirements

### Rotation Schedule

| Phase | Duration | Driver | Navigator |
|-------|----------|--------|-----------|
| Phase 1: Configuration | 30 min | Partner A | Partner B |
| Phase 2: DAG Structure | 45 min | Partner B | Partner A |
| Phase 3: Task Functions | 45 min | Partner A | Partner B |
| Phase 4: Testing | 30 min | Partner B | Partner A |

**Switch roles at each phase!**

---

## The Scenario

Your data platform team processes data for multiple clients. Each client has different:
- Data sources (tables to extract)
- Schedules (how often to run)
- Priorities (which resources to use)

Instead of creating separate DAGs for each client, you'll build a **DAG Factory** that generates pipelines dynamically from a JSON configuration file.

---

## Core Tasks

### Phase 1: Design the Configuration (Driver: Partner A)

1. Open `starter_code/config/pipelines.json`
2. Design a JSON schema that includes:
   - Client name
   - List of tables to process
   - Schedule (cron expression or preset)
   - Priority level

3. Create configurations for at least 3 clients:
   - "acme_corp": 3 tables, daily schedule, high priority
   - "globex_inc": 2 tables, hourly schedule, medium priority
   - "initech": 4 tables, weekly schedule, low priority

**Navigator:** Verify the JSON is valid and covers edge cases.

### Phase 2: Build the DAG Structure (Driver: Partner B)

1. Open `starter_code/dags/dynamic_factory_dag.py`
2. Implement the configuration loading:
   - Read the JSON file
   - Handle file not found errors
   - Validate required fields exist

3. Create the DAG definition:
   - Use a `with DAG()` context manager
   - Generate TaskGroups for each client
   - Add start and end markers

**Navigator:** Ensure the code handles errors gracefully.

### Phase 3: Implement Task Functions (Driver: Partner A)

1. Implement the extract, transform, and load functions
2. For each client, create a TaskGroup containing:
   - One task per table (extract)
   - One transform task (aggregates all extracts)
   - One load task

3. Connect dependencies:
   - All extract tasks run in parallel within a client
   - Transform waits for all extracts
   - Load runs after transform

**Navigator:** Check that XCom is used correctly between tasks.

### Phase 4: Test and Verify (Driver: Partner B)

1. Copy the DAG and config to Airflow
2. Verify the DAG appears in the UI
3. Check that TaskGroups are visible
4. Trigger the DAG and observe:
   - All clients process correctly
   - Tasks within each client follow dependencies

**Navigator:** Document any bugs found and verify fixes.

---

## Configuration Schema

Your `pipelines.json` should follow this structure:

```json
{
  "clients": [
    {
      "name": "client_name",
      "tables": ["table1", "table2"],
      "schedule": "@daily",
      "priority": "high"
    }
  ]
}
```

---

## Stretch Goals

1. **Add Validation:**
   Add a validation task that checks record counts before loading

2. **Priority-Based Pools:**
   Assign tasks to different pools based on client priority

3. **Multiple DAGs:**
   Generate a separate DAG for each client instead of one combined DAG

---

## Definition of Done

- [ ] JSON configuration for 3+ clients is valid
- [ ] DAG loads without import errors
- [ ] TaskGroups appear for each client
- [ ] Tasks within groups have correct dependencies
- [ ] Each table generates a separate extract task
- [ ] DAG runs successfully end-to-end
- [ ] Both partners understand all code

---

## Pair Review Checklist

Before submitting, discuss these questions:

1. What would happen if we added a new client to the JSON?
2. How could we make this more reusable?
3. What error handling is missing?
4. How would this scale to 100 clients?

---

## Submission

1. Completed `pipelines.json` configuration
2. Completed `dynamic_factory_dag.py`
3. Screenshot of TaskGroups in Airflow UI
4. `pair_reflection.md` with learnings from collaboration

---

## Resources

- Written Content: `dynamic-dags.md`, `parameterized-dags.md`
- Demo Reference: `demo_dynamic_dag/`
