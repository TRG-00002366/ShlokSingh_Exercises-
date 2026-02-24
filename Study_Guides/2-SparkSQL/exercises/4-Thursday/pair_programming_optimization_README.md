# Pair Programming: Pipeline Optimization Challenge

## Overview
**Day:** Thursday (Collaborative Day)  
**Mode:** Pair Programming (Driver/Navigator)  
**Duration:** 3-4 hours  
**Topics:** Partitioning, Caching, Bucketing, Performance Optimization

## Pair Programming Rules

### Roles
- **Driver**: Controls the keyboard, writes code
- **Navigator**: Reviews code, thinks strategically, catches errors
- **Switch roles every 20 minutes!**

### Communication
- Navigator should NOT dictate code verbatim
- Driver should explain their thinking as they code
- Both should discuss approach before coding

---

## The Challenge

You have inherited a slow-running data pipeline. Your job is to optimize it using the techniques learned this week:
- Partitioning (repartition, coalesce)
- Caching (cache, persist)
- Bucketing

The pipeline processes sales data and generates multiple reports. Currently, it runs in sequence and does not leverage Spark's optimization capabilities.

---

## Phase 1: Baseline Analysis (30 mins)
**Driver: Partner A | Navigator: Partner B**

1. Open `pair_programming_optimization.py`
2. Run the baseline pipeline and note the execution time
3. Examine the execution plan using `.explain()`
4. Identify bottlenecks:
   - Where are shuffles happening?
   - Is data being recomputed?
   - Are partitions appropriately sized?

**Document your findings:**
- [ ] Baseline execution time: ____
- [ ] Number of shuffles identified: ____
- [ ] Data recomputation issues: ____

---

## Phase 2: Apply Caching (30 mins)
**Driver: Partner B | Navigator: Partner A**

1. Identify DataFrames used multiple times
2. Apply appropriate caching strategy
3. Re-run and measure improvement

**Questions to discuss:**
- Which storage level is appropriate?
- Should we cache before or after transformations?
- How do we verify caching is working?

**Document:**
- [ ] DataFrames cached: ____
- [ ] Storage levels chosen: ____
- [ ] New execution time: ____

---

## Phase 3: Optimize Partitioning (30 mins)
**Driver: Partner A | Navigator: Partner B**

1. Check current partition counts
2. Identify where repartition vs coalesce should be used
3. Optimize partition counts for the workload

**Questions to discuss:**
- How many partitions should we have for our data size?
- Where should we use repartition vs coalesce?
- Should we partition by any columns?

**Document:**
- [ ] Original partition counts: ____
- [ ] Optimized partition counts: ____
- [ ] New execution time: ____

---

## Phase 4: Implement Bucketing (45 mins)
**Driver: Partner B | Navigator: Partner A**

1. Identify joins that would benefit from bucketing
2. Create bucketed versions of the tables
3. Verify join optimization with explain()

**Questions to discuss:**
- Which tables should be bucketed?
- What bucket count makes sense?
- How do we verify the join is optimized?

---

## Phase 5: Final Optimization Review (30 mins)
**Both Partners Together**

1. Compare baseline vs optimized execution times
2. Review the final execution plan
3. Document the improvements

---

## Deliverables

1. **Optimized code** in `pair_programming_optimization.py`
2. **Performance report** (fill in below):

```
OPTIMIZATION REPORT
===================
Baseline Time:      ____
Optimized Time:     ____
Improvement:        ____% faster

Techniques Applied:
- Caching: [describe]
- Partitioning: [describe]  
- Bucketing: [describe]

Key Learnings:
- [Partner A insight]
- [Partner B insight]
```

---

## Definition of Done
- [ ] Both partners have been Driver and Navigator
- [ ] Baseline measured and documented
- [ ] At least 30% performance improvement achieved
- [ ] All optimizations explained and justified
- [ ] Final execution plan reviewed
- [ ] Performance report completed
