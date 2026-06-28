# Sketch Config Selection: MIP Formulation

## Sets

| Symbol | Definition |
|--------|-----------|
| $A = \{a_1, \ldots, a_m\}$ | All AQEs (Atomic Query Expressions), deduplicated across all RQEs |
| $R = \{r_1, \ldots, r_n\}$ | All RQEs (Repeating Query Expressions); each $r = (\text{QE}_r, T_r)$ |
| $R_a \subseteq R$ | RQEs that reference AQE $a$ |
| $G$ | Candidate streaming configs, exhaustively enumerated over all combinations of $(s, p, W, S)$ from parameter ranges derived from the AQE workload. Each $g = (s, p, \tau, W, S, n_g, d_g, \text{metric}, L, \phi)$ where $s$ = sketch family, $p$ = params, $\tau \in \{\text{tumbling}, \text{sliding}\}$, $W$ = window size, $S$ = slide interval ($S = W$ for tumbling), $n_g$ = number of windows used at query time, $d_g$ = physical storage depth (number of completed windows retained), $L$ = label set, $\phi$ = spatial filter (a filter on label values / metadata restricting which time series are ingested into this config). For every $a \in A$, $\text{EXACT}_a \in G$. |

**Relationship between $n_g$ and $d_g$:**
- Tumbling: $d_g = n_g$ — every retained window is used at query time.
- Sliding: $d_g \geq (n_g - 1)(W_g/S_g) + 1$ — to retrieve $n_g$ non-overlapping windows spaced $W_g$ apart, the system must retain $(n_g-1)(W_g/S_g)+1$ completed windows in storage; only $n_g$ of those are loaded at query time. The cost formulation charges for the $n_g$ loaded windows only; the remaining $d_g - n_g$ retained-but-unused windows are storage overhead not modeled in $\text{Mem\_query}$. For sliding, $n_g$ is a config-generation parameter used to size $d_g$; the optimizer operates on $d_g$ directly via (RETs) and does not reference $n_g$.

## Inputs

| Symbol | Source | Definition |
|--------|--------|-----------|
| $T_r$ | RQE | Repeat interval for RQE $r$ (ms) |
| $\varepsilon_a$ | User-specified | Accuracy tolerance for AQE $a$ |
| $\text{SLA}_a$ | User-specified | Latency budget for AQE $a$ (default $\infty$) |
| $\text{range}_a$ | AQE | Lookback duration baked into $a$'s range vector |
| $\theta_a = (N_a, D_a)$ | Profiled | $N_a$ = cardinality; $D_a$ = data distribution, family-specific: Zipf (with parameter $\alpha$), Gaussian (with parameters $\mu, \sigma$), or empirical trace |
| $\rho_g$ | Measured from Prometheus | Item arrival rate (items/sec) for $g$'s metric and label filter |
| $N_g$ | Measured from Prometheus | Number of distinct label-group combinations for $g$'s metric and label filter; used when $\lnot\,\text{subpop\_aware}(s)$ to determine how many sketch instances config $g$ must maintain |
| $\text{AtomicCosts}(s, p)$ | sketch-bench | $\text{mem}(s,p),\ \text{ins}(s,p),\ \text{mrg}(s,p),\ \text{sub}(s,p),\ \text{qry}(s,p)$ |
| $w = (w_1, w_2, w_3, w_4)$ | User-specified | Weights for ingest CPU, ingest memory, query CPU, query memory |

## Sketch Properties (per family $s$)

| Property | Meaning |
|----------|---------|
| $\text{mergeable}(s)$ | Two instances can be merged into one representing their union |
| $\text{subtractable}(s)$ | Incremental update is defined: the current query result can be derived from the previous result by adding new data and subtracting evicted data, requiring O(1) sketch operations regardless of $n$ |
| $\text{subpop\_aware}(s)$ | One instance handles all label-group keys internally |

## Derived Quantities

**Query frequency:**
$$f_a = \sum_{r \in R_a} \frac{1}{T_r} \quad \text{(queries/sec)}$$

*(Assumes no cross-RQE result reuse: simultaneous RQEs referencing the same AQE each count as independent query work.)*

**Label-group multiplier:**
$$N(s, g) = \begin{cases} 1 & \text{if } \text{subpop\_aware}(s) \\ N_g & \text{otherwise} \end{cases}$$

where $N_g$ is measured from Prometheus for $g$'s metric and label filter (see Inputs). *(Requires a profiling step before the optimizer runs — see Open Items.)*

**Windows needed to cover $a$'s range from config $g$:**
$$n(a, g) = \left\lceil \frac{\text{range}_a}{W_g} \right\rceil$$

**Query method** — fully determined by $(\tau_g, n(a,g), s)$; not a decision variable. For sliding configs, only those with $S_g \mid W_g$ (slide divides window size) are included in $G$, ensuring the $n$ non-overlapping windows used in a merge/subtract land on exact slide boundaries.

| Condition | Method |
|-----------|--------|
| $\text{EXACT}_a$ | Exact |
| $n(a,g) = 1$ | Direct |
| $n(a,g) > 1$, $\text{subtractable}(s)$ | Subtract |
| $n(a,g) > 1$, $\text{mergeable}(s)$, not subtractable | Merge |
| $n(a,g) > 1$, neither property | Infeasible |

When both Subtract and Merge apply ($n(a,g) > 1$, $\text{subtractable}(s)$, $\text{mergeable}(s)$), Subtract takes priority: it requires $O(1)$ sketch operations regardless of $n(a,g)$ vs. $O(n(a,g))$ merges for Merge. The CPU advantage compounds as $n(a,g)$ grows; Mem\_query is the same ($2 \cdot N(s,g) \cdot \text{mem}$) in both cases.

The Subtract method uses the incremental update formula: let $[t_0, t_1]$ be the current query window ($t_1 - t_0 = \text{range}_a$). Given the previous query result $T[t_0 - T_r,\, t_1 - T_r]$ already in the $+1$ $\text{Mem\_active}$ slot, the newly completed window $T[t_1 - T_r,\, t_1]$ (loaded from storage), and the evicted window $T[t_0 - T_r,\, t_0]$ (loaded from storage), compute $T[t_0, t_1] = T[t_0-T_r, t_1-T_r] + T[t_1-T_r, t_1] - T[t_0-T_r, t_0]$. This applies to both tumbling and sliding ingest types.

## Feasibility Constraints

Rather than folding all conditions into an opaque $\text{Feasible}(a,g)$ predicate, each condition is expressed as an explicit pre-computed parameter $c_{a,g} \in \{0,1\}$ that bounds $x_{a,g}$. $\text{Feasible}(a,g) = 1$ iff all of the following hold:

**Capability matching:**
$$x_{a,g} \leq \mathbf{1}[\text{CapMatch}(a, g)] \tag{CAP}$$

$\text{CapMatch}(a,g) = 1$ iff $g$'s label set, metric, and spatial filter cover $a$'s requirements. Directional: a finer-grained $g$ can serve a coarser $a$ via merge at query time; the reverse is infeasible — once $g$ aggregates away a label, that information is gone.

**Retention** (tumbling):
$$x_{a,g} \leq \mathbf{1}[n_g \geq n(a,g)] \quad \tau_g = \text{tumbling} \tag{RETt}$$

For tumbling, $n_g = d_g$ — the query working set and the storage depth are the same thing. This is equivalent to $d_g \geq n(a,g)$, which is the tumbling special case of (RETs) with $S_g = W_g$.

**Retention** (sliding):
$$x_{a,g} \leq \mathbf{1}\!\left[d_g \geq (n(a,g)-1)\cdot\frac{W_g}{S_g} + 1\right] \quad \tau_g = \text{sliding} \tag{RETs}$$

For sliding, $n_g$ (query working set) and $d_g$ (storage depth) differ. To retrieve $n(a,g)$ non-overlapping windows spaced $W_g$ apart, the config must have retained at least $(n(a,g)-1)(W_g/S_g)+1$ completed windows. Note that $n_g \cdot W_g \geq \text{range}_a$ is trivially satisfied for sliding by the ceiling construction and gives no useful information — the binding constraint is on $d_g$.

**Window size:**
$$x_{a,g} \leq \mathbf{1}[W_g \leq \text{range}_a] \tag{WIN}$$

A sketch must not over-cover the query range; data absorbed before $\text{range}_a$ ago cannot be extracted after the fact.

*Implication for Direct queries:* $n(a,g) = 1$ requires $\lceil \text{range}_a / W_g \rceil = 1$, i.e. $W_g \geq \text{range}_a$. Combined with (WIN) this means the Direct method is only possible when $W_g = \text{range}_a$ exactly — Direct configs are uniquely sized per AQE.

**Freshness** (ingest-type specific):
$$x_{a,g} \leq \mathbf{1}\!\left[W_g \leq \min_{r \in R_a} T_r\right] \quad \tau_g = \text{tumbling} \tag{FRESHt}$$
$$x_{a,g} \leq \mathbf{1}\!\left[S_g \leq \min_{r \in R_a} T_r\right] \quad \tau_g = \text{sliding} \tag{FRESHs}$$

For tumbling, a completed window must exist for every query cycle, so $W \leq T_r$. For sliding, a new completed window appears every $S$ seconds, so the binding constraint is $S \leq T_r$ — $W$ can exceed $T_r$ for sliding.

**Accuracy:**
$$x_{a,g} \leq \mathbf{1}[\text{Error}(a, g, \theta_a) \leq \varepsilon_a] \tag{ACC}$$

$\text{Error}(a, g, \theta_a)$ is an empirical lookup from sketch-bench, indexed by $(s, p, \theta_a)$. $\text{Error}(\text{EXACT}_a) = 0$ always.

**Latency:**
$$x_{a,g} \leq \mathbf{1}[\text{Latency}(a, g) \leq \text{SLA}_a] \tag{LAT}$$

$\text{Latency}(a,g) \approx \text{CPU\_query}(a,g)$ *(to be confirmed against end-to-end sketch-bench measurements)*.

**Valid query method:**
$$x_{a,g} \leq \mathbf{1}[\text{QueryMethodValid}(a, g)] \tag{QM}$$

$\text{QueryMethodValid}(a,g) = 1$ iff the query method derived from $(\tau_g, n(a,g), s)$ is not Infeasible in the table above.

$\text{EXACT}_a$ satisfies all conditions for any $a$: $\text{Error} = 0$, $\text{Latency} = \text{exact query cost}$, no window constraints apply.

## Cost Functions

### Ingestion memory (steady-state active windows only)

Retained completed windows are charged at query time via $\text{Mem\_query}$ below, not as a continuous allocation.

$$\text{Mem\_active}(g) = \begin{cases} (1 + \mathbf{1}[\text{subtractable}(s)]) \cdot N(s,g) \cdot \text{mem}(s,p) & \tau_g = \text{tumbling} \\ (\lceil W_g / S_g \rceil + \mathbf{1}[\text{subtractable}(s)]) \cdot N(s,g) \cdot \text{mem}(s,p) & \tau_g = \text{sliding} \end{cases}$$

The $\mathbf{1}[\text{subtractable}(s)]$ term accounts for the one extra instance needed to store the previous query result for the Subtract incremental update.

### Ingestion CPU (rate)

$$\text{CPU\_ingest}(g) = \begin{cases} \rho_g \cdot \text{ins}(s,p) & \tau_g = \text{tumbling} \\ \rho_g \cdot \lceil W_g / S_g \rceil \cdot \text{ins}(s,p) & \tau_g = \text{sliding} \end{cases}$$

### Ingestion cost (rate — independent of AQE assignment)

$$\text{IngestCost}(g) = w_1 \cdot \text{CPU\_ingest}(g) + w_2 \cdot \text{Mem\_active}(g)$$

### Query cost (per invocation — depends on query method)

$\text{Mem\_query}$ is the transient peak working memory per query invocation (retained windows loaded from storage), not steady-state.

| Method | $\text{CPU\_query}(a,g)$ | $\text{Mem\_query}(a,g)$ |
|--------|--------------------------|--------------------------|
| Direct | $N(s,g) \cdot \text{qry}(s,p)$ | $N(s,g) \cdot \text{mem}(s,p)$ |
| Merge | $N(s,g) \cdot \bigl((n(a,g)-1) \cdot \text{mrg}(s,p) + \text{qry}(s,p)\bigr)$ | $n(a,g) \cdot N(s,g) \cdot \text{mem}(s,p)$ |
| Subtract | $N(s,g) \cdot \bigl(\text{mrg}(s,p) + \text{sub}(s,p) + \text{qry}(s,p)\bigr)$ | $2 \cdot N(s,g) \cdot \text{mem}(s,p)$ |
| Exact | $c_{\text{exact}}$ (system constant) | $0$ |

$$\text{QueryCost}(a,g) = w_3 \cdot \text{CPU\_query}(a,g) + w_4 \cdot \text{Mem\_query}(a,g)$$

## Decision Variables

$$y_g \in \{0,1\} \quad \forall g \in G \qquad \text{(is config } g \text{ deployed)}$$

$$x_{a,g} \in \{0,1\} \quad \forall a \in A,\ g \in G \text{ s.t. } \text{Feasible}(a,g) = 1 \qquad \text{(is AQE } a \text{ served by config } g\text{)}$$

## Objective

$$\min \sum_{g \in G} y_g \cdot \text{IngestCost}(g) \ + \ \sum_{\substack{a \in A,\ g \in G \\ \text{Feasible}(a,g)=1}} x_{a,g} \cdot f_a \cdot \text{QueryCost}(a,g)$$

Both terms are cost rates (cost/sec). $f_a$ converts the per-query $\text{QueryCost}$ into a rate commensurate with the continuously-accruing $\text{IngestCost}$.

## MIP Constraints

$$\sum_{\substack{g \in G \\ \text{Feasible}(a,g)=1}} x_{a,g} = 1 \qquad \forall a \in A \tag{1}$$

$$x_{a,g} \leq y_g \qquad \forall a \in A,\ g \in G \tag{2}$$

$$x_{a,g},\ y_g \in \{0,1\} \tag{3}$$

**(1)** Every AQE is assigned to exactly one config. Always satisfiable since $\text{EXACT}_a$ satisfies all feasibility conditions for any $a$.
**(2)** An AQE cannot be served by a config that is not deployed.
**(3)** Integrality. Feasibility is enforced by restricting $x_{a,g}$ to the domain where $\text{Feasible}(a,g) = 1$ — expanding into (CAP), (RET), (WIN), (FRESHt)/(FRESHs), (ACC), (LAT), (QM); all pre-computed, adding no solver complexity.

## Problem Class

Uncapacitated facility-location MIP: $y_g$ = "open facility $g$", $x_{a,g}$ = "assign demand $a$ to facility $g$", $\text{IngestCost}(g)$ = fixed facility cost, $f_a \cdot \text{QueryCost}(a,g)$ = assignment cost. Standard structure — solvable with off-the-shelf MIP solvers (CBC, OR-Tools, Gurobi).

## Open Items

1. **$\text{Latency}(a,g) \approx \text{CPU\_query}(a,g)$**: to be confirmed against end-to-end sketch-bench measurements. If other latency components (storage reads, network) dominate, this approximation needs revision.
2. **$N_g$ (distinct label-group count)**: for non-subpop-aware sketch families, $N(s,g) = N_g$ requires a profiling step (e.g. `count(metric{filters...})` from Prometheus) before the optimizer runs.
3. **$\theta_a$ profiling**: cardinality $N_a$ and distribution $D_a$ must be estimated before $\text{Error}(a,g,\theta_a)$ can be looked up from sketch-bench. Profiling strategy not yet designed.
4. **sketch-bench data gap**: empirical error and cost lookup requires cardinality and distribution shape to be swept parameters in sketch-bench benchmarks. This data does not yet exist and must be collected.
5. **Sliding storage overhead**: $d_g$ is the physical storage depth for sliding configs; only $n_g \leq d_g$ windows are loaded at query time. The $d_g - n_g$ retained-but-unused windows represent storage cost not captured in $\text{Mem\_query}$.
6. **Mem\_active subtractable scaling**: The $+\mathbf{1}[\text{subtractable}(s)]$ term in $\text{Mem\_active}(g)$ allocates one previous-result slot per config, but each AQE assigned to $g$ using the Subtract method needs its own slot (different range $\Rightarrow$ different previous result). With $k$ such AQEs, the correct term is $+k$, not $+1$. Long-term fix: move the per-previous-result cost into $\text{QueryCost}$ so it is charged per assignment.
7. **Subtract formula assumes $T_r = W_g$**: The incremental formula treats $T[t_1 - T_r, t_1]$ as a single newly completed window. When $T_r > W_g$ (allowed by FRESHt), this span covers $\lfloor T_r / W_g \rfloor$ completed windows; the Subtract cost (merging in new data and subtracting evicted data) then scales with $\lfloor T_r / W_g \rfloor$, not O(1). The current cost table silently assumes $T_r = W_g$.
8. **IngestCost(EXACT_a) is undefined**: $\text{EXACT}_a \in G$ participates in $\sum_g y_g \cdot \text{IngestCost}(g)$, but IngestCost is defined only for sketch configs (via $s, p, \rho_g, W_g, S_g$) — none of which exist for an EXACT config. Presumably $\text{IngestCost}(\text{EXACT}_a) = 0$ since Prometheus handles raw ingest, but this must be stated explicitly.

## TODO

1. **Label-dimension merge cost**: (CAP) allows a finer-grained $g$ to serve a coarser $a$ by aggregating away label dimensions at query time, but the query cost table has no entry for this. When $g$'s label set is a proper superset of $a$'s, querying $a$ from $g$ requires a group-by and reduce over the extra label values — a cost proportional to $N_g / N_a$ operations — currently silent in the model. Either add a label-merge cost term to $\text{CPU\_query}$, or restrict (CAP) to require exact label-set match.

2. **FRESHt per-RQE vs. per-AQE**: (FRESHt) uses $\min_{r \in R_a} T_r$, banning any tumbling window $W_g > \min T_r$ for AQE $a$ even though slower RQEs could tolerate it. If $a$ is queried by a 1s RQE and a 60s RQE, all configs with $W_g > 1\text{s}$ are ruled infeasible, forcing small windows even when most query load runs at 60s cadence. Consider whether (FRESHt) should be relaxed per-$(a, r)$ pair — at the cost of introducing per-$(a, r, g)$ assignment variables.
