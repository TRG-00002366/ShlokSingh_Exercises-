# Exercise: EMR Cluster Creation

## Overview
Create your own EMR cluster following the documented steps. This is a guided exercise to practice cloud deployment.

**Duration:** 45-60 minutes  
**Mode:** Individual

---

## Prerequisites

- AWS account with appropriate permissions
- AWS CLI configured with credentials
- Understanding of EMR concepts from written content

---

## Core Tasks

### Task 1: Plan Your Cluster

Before creating, decide on configuration:

| Setting | Your Choice | Rationale |
|---------|-------------|-----------|
| Cluster name | | |
| Release | emr-7.0.0 | Latest stable |
| Primary instance | | |
| Core instances | | |
| Core count | | |

### Task 2: Create via Console

1. Navigate to AWS EMR in the console
2. Click "Create cluster"
3. Configure:
   - Name: `pyspark-training-[yourname]`
   - Release: `emr-7.0.0`
   - Applications: Select Spark
   - Primary: `m5.xlarge`
   - Core: `r5.large` (2 instances)
4. Select your EC2 key pair
5. Create the cluster

### Task 3: Document the Process

Create `cluster_creation.md`:

```markdown
# EMR Cluster Creation Log

## Cluster Details
- Cluster ID: j-XXXXX
- Name: 
- Created: [timestamp]

## Configuration
- Release: emr-7.0.0
- Primary: m5.xlarge
- Core: r5.large x 2

## Status
- [ ] Cluster starting
- [ ] Cluster bootstrapping
- [ ] Cluster running
- [ ] Cluster terminated (after exercise)

## Observations
[Note anything interesting or unexpected]
```

### Task 4: Monitor Cluster Status

```bash
# Check cluster status
aws emr describe-cluster \
    --cluster-id j-XXXXX \
    --query 'Cluster.Status.State'

# Wait for WAITING status
```

### Task 5: SSH to Cluster (Optional)

```bash
# Get primary node DNS
aws emr describe-cluster \
    --cluster-id j-XXXXX \
    --query 'Cluster.MasterPublicDnsName'

# SSH to cluster
ssh -i your-key.pem hadoop@[primary-dns]

# On cluster, verify Spark
spark-shell --version
```

### Task 6: Terminate Cluster

**IMPORTANT:** Always terminate when done to avoid charges!

```bash
aws emr terminate-clusters --cluster-ids j-XXXXX
```

Or via Console: Select cluster > Terminate

---

## Deliverables

1. `cluster_creation.md` - Your documentation
2. Screenshot of running cluster (optional)
3. Confirmation of termination

---

## Definition of Done

- [ ] Cluster created successfully
- [ ] Reached WAITING state
- [ ] Documentation completed
- [ ] Cluster terminated (to avoid charges)

---

## Cost Warning

EMR clusters incur charges while running. Always terminate when done with exercises!

---

## Additional Resources
- Written Content: `creating-aws-spark-emr-cluster.md`
- Demo: `demo_emr_cluster_creation.md`
