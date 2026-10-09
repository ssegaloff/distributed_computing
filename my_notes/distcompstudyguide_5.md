# DS 7200 Study Guide — Module 5

Sep 28, 2026 · @Sabine

## How to use this guide

Everything here comes from your repo's `05_clustering_and_dim_reduction` folder: the two lecture notebooks (`mllib_clustering` and `dimensionality_reduction`), the *k-Means Extensions* and *SVD Summary* slides, the k-means initialization reading, and the lab. The clustering outputs quoted below are from **your own run** of `mllib_clustering` (9/28, on Beans-Legion). The dimension reduction outputs are the instructor's saved outputs; your copy hasn't been rerun yet.

It follows the same conventions as the earlier guides. Where I added an explanation or example that isn't in the course material, it's marked **(my addition)**. Where the source material itself has a problem, it's marked **Heads up**. I checked the numbers I added by recomputing them in NumPy and scikit-learn. Where Spark's output was available, they match it.

**What I checked for this guide:**

- **Every image** in the Module 5 folder.
- **Both `.ppt` decks** against their PDFs. They match slide for slide: same 11 and 12 slides, same text, no speaker notes, no hidden slides. The PDFs are all you need.
- **Every link** in the notebooks, the reading and `textbooks.md` (see *Readings, textbooks, and links* below).
- **Spark's source code and docs**, for the claims about how Spark works internally that the course material doesn't state. Those are marked as confirmed where they come up.

## Module 5: Clustering & Dimension Reduction

Module 4 was supervised learning, where every row has a label. Module 5 turns to **unlabeled** data. It starts with k-means clustering, which you'll use to look for structure in Fidelity mutual funds. It then covers dimension reduction. The module intro explains why this matters with big data: fewer feature dimensions means less storage and less compute time.

**Learning outcomes.** By the end you should be able to:

- Implement k-means clustering in MLlib.
- Apply dimension reduction techniques using PySpark.
- Distinguish between SVD and PCA.
- Progress toward an end-to-end predictive modeling project on a large dataset.

**What's due.** Per agenda 10, **Lab 4 (Clustering Fidelity Mutual Funds)** and **Quiz 5 (Dimension Reduction, MLlib Clustering)** are due **Friday, Oct 9 at 11:59pm ET**. Lab 3 and Quiz 4 from Module 4 are still due Friday, Oct 2.

**In class (agenda 10):** review the *MLlib – Clustering* notebook and the *k-means Extensions* slides.

### Readings, textbooks, and links

**The course textbooks** (from `textbooks.md`, which links each one on O'Reilly):

- *Learning Spark, 2nd Edition* — Damji, Das, Lee, Wenig (2020)
- *Learning PySpark* — Drabas, Lee (2017)
- *Designing Data-Intensive Applications* — Kleppmann

**What Module 5 points to in them.** Both lecture notebooks cite ***Learning Spark*, Chapter 11: Machine Learning with MLlib.**

**Heads up: that chapter is in the *1st* edition, not the required 2nd.** **(My addition)** I checked both tables of contents:

- In the **1st edition** (Karau, Konwinski, Wendell, Zaharia, 2015), Ch. 11 is *Machine Learning with MLlib*. It has sections on **clustering** and **dimensionality reduction**, written for the older RDD API (`pyspark.mllib`). That fits this module exactly.
- In the **2nd edition** (the required one), Ch. 11 is *Managing, Deploying, and Scaling Machine Learning Pipelines* (MLflow, deployment, hyperparameter tuning). MLlib is **Ch. 10**, the chapter Module 4 assigned.

If you want the reading the notebooks mean, it's the 1st edition's Ch. 11. In the required books, the closest match is *Learning PySpark* **Ch. 6, *Introducing the ML Package***, whose contents include a clustering section (*Finding clusters in the births dataset*). I only checked the tables of contents, not the chapters themselves, so I can't say how much of this module they cover.

**The module's own reading** (`reading.md`, on k-means initialization) is two links: the k-means++ Wikipedia page and the k-means|| paper. The slides reproduce the key parts of both, so this guide covers them in the initialization section.

**Link check (9/28).** Every link resolves:

| Link | Where | Status |
| --- | --- | --- |
| [k-means++ (Wikipedia)](https://en.wikipedia.org/wiki/K-means%2B%2B) | reading | Works. Same algorithm as slide 4. |
| [k-means\|\| paper](http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf) | reading | Works. *Scalable K-Means++*, Bahmani, Moseley, Vattani, Kumar, Vassilvitskii (VLDB 2012). |
| [Spark clustering docs](https://spark.apache.org/docs/latest/ml-clustering.html) | clustering notebook | Works (now shows Spark 4.2). Covers the same five models. |
| [Cluster cohesion (Towards Data Science)](https://towardsdatascience.com/explain-ml-in-a-simple-way-k-means-clustering-e925d019743b) | clustering notebook | Works, no paywall. A general k-means walkthrough with the Iris dataset. |
| [Silhouette (Wikipedia)](https://en.wikipedia.org/wiki/Silhouette_(clustering)) | clustering notebook | Works. |
| [Silhouette toy example (Medium)](https://medium.com/@MrBam44/how-to-evaluate-the-performance-of-clustering-algorithms-3ba29cad8c03) | clustering notebook | **Couldn't verify.** Medium served only its page frame to my fetch tool, so I couldn't see the article. Open it in your browser; it may be paywalled. |
| [k-means extensions deck](https://github.com/UVADS/distributed_computing/blob/main/05_clustering_and_dim_reduction/content_clustering/k_means_extensions.ppt) | clustering notebook | Works. It's the same file as in your repo. |
| [Spark dimensionality reduction docs](https://spark.apache.org/docs/latest/mllib-dimensionality-reduction.html) | dim. reduction notebook | Works. The SVD cost details in the notebook are copied from here. |
| [`pca_rowmatrix_example.py`](https://github.com/apache/spark/blob/master/examples/src/main/python/mllib/pca_rowmatrix_example.py) | dim. reduction notebook | Works. It's the source of the notebook's PCA example. |
| [`svd_example.py`](https://github.com/apache/spark/blob/master/examples/src/main/python/mllib/svd_example.py) | dim. reduction notebook | Works. Same data as the notebook, but it asks for the top **5** singular values; the notebook uses 4. |
| [PCA (Wikipedia)](http://en.wikipedia.org/wiki/Principal_component_analysis) | dim. reduction notebook | Works. |
| [SVD (Wikipedia)](https://en.wikipedia.org/wiki/Singular_value_decomposition) | dim. reduction notebook | Works. It also covers the Eckart–Young theorem used below. |
| O'Reilly textbook links | `textbooks.md` | *Learning Spark* redirects to its public page on oreilly.com, and the *Learning PySpark* page loads. I didn't check the *Designing Data-Intensive Applications* link. The full text needs your UVA sign-in, which I don't have. |

**(My addition) One thing from the k-means|| paper that the slides skip:** in practice you don't need anywhere near O(log ψ) rounds. The authors report that after as few as **five rounds**, k-means|| consistently matched or beat the other initialization methods. It works as well as k-means++ once rounds × ℓ ≥ k, meaning enough candidates have been sampled in total. That's why Spark can get away with a small default (below).

## Part 1: Clustering

### Unsupervised learning

In unsupervised learning, the labels are unknown. The analyst wants to split the observations into groups of high similarity, where "similar" is defined in terms of the feature space. The notebook gives two common uses:

- **Data exploration:** discover the properties that similar observations share.
- **Outlier detection:** outliers generally form their own groups, often singletons.

**Clustering models in Spark's DataFrame API:** k-means, Gaussian mixture, power iteration clustering (PIC), latent Dirichlet allocation (LDA), and bisecting k-means. The course covers the first two.

**Heads up:** the notebook says these are "supported in `spark.mllib` with the DataFrame API." **(My addition)** The DataFrame API is `pyspark.ml`; `spark.mllib` is the older RDD API (Module 4's two interfaces). The code in the notebook imports from `pyspark.ml.clustering`, which is the DataFrame one.

### k-means: the idea

The notebook's before/after picture shows the goal: about 30 unlabeled points on the left; on the right, the same points split into three circled groups (k = 3), each drawn in its own color.

k-means is the most popular clustering algorithm and is widely used in industry. It's relatively simple, has a single parameter (k), and always converges on a solution, though not necessarily the best one.

The slides give the setup: given N observations, assign each to one of k groups. Each group has a **centroid**, its balance point (the mean of its members).

**(My addition)** The notebook and slides never spell out the loop itself. It's two steps, repeated until nothing changes:

1. **Assign:** put each point in the cluster whose centroid is nearest.
2. **Update:** move each centroid to the mean of the points now assigned to it.

Each step can only lower the total squared distance from points to their centroids (the WSS, below), so the loop always stops. But it stops at the first arrangement it can't improve, which may be a **local** minimum rather than the best possible one. That's why the starting centroids matter so much, and why the slides spend most of their time on initialization.

**The notebook's k-means spec sheet:**

| Item | Description |
| --- | --- |
| Supervised/unsupervised | Unsupervised |
| Initialization | Random assignment |
| Assumptions | Euclidean distance |
| Preprocessing | Scaling |
| Parameters | k, the number of clusters |
| Metrics | Inertia |
| Strengths | One parameter, relatively simple |
| Weaknesses | May not find the global optimum; can't handle non-quantitative (e.g. categorical) data; assumes spherical clusters |

**Heads up, three small problems with that table and its text:**

1. **Initialization:** the table says "random assignment," but the same notebook (and the reading) says Spark's **default** is `k-means||`. Random is just one option.
2. **"Global maximum":** the intro says k-means may not reach "the global maximum." **(My addition)** k-means *minimizes* WSS, so it's the global **minimum** it may miss.
3. **Metrics:** "inertia" is scikit-learn's name for WSS. Spark calls the same quantity the **training cost**: `model.summary.trainingCost`. Its docstring in Spark's source reads "sum of squared distances to the nearest centroid for all points in the training dataset. This is equivalent to sklearn's inertia." The lab folder's old plot calls it "WSSE." The Spark evaluator the notebook actually uses computes the **silhouette score**, a different metric.

**(My addition)** Why "Euclidean" and "scaling" belong together: k-means measures closeness with straight-line distance, so a feature measured in large units dominates the distance. Scaling puts features on an equal footing, as in Module 4. The "spherical clusters" weakness comes from the same place. Every point goes to its nearest centroid, so each cluster is a round-ish region around its center. Long, thin, or curved clusters get chopped up.

### The k-means workflow

The notebook's recommended sequence:

1. Feature selection.
2. Feature standardization.
3. Run the algorithm for a **sequence** of k values.
4. Examine the results and remediate outliers. **Loop on steps 3–4 as needed.**
5. Select the best k (written k\*) and extract the cluster assignments.
6. Enrich the clusters with domain knowledge.

Step 6 is the one people skip. The algorithm only returns group numbers; deciding what a cluster *means* is up to you.

### Measuring a clustering: WSS

The quality measure for k-means is the **within-cluster sum of squares (WSS)**. For each cluster, sum the squared distances from each point to the cluster's centroid; then add those sums across all clusters. It measures each cluster's **internal cohesion**: small WSS means tight clusters.

The notebook's cohesion diagram shows the two things a good clustering has: **internal cohesion** (points close to their own centroid) and **external separation** (clusters far from each other).

**Heads up:** the notebook's formula reads


$$
\text{WSS} = 1 - \frac{\text{Between Sum of Squares}}{\text{Total Sum of Squares}}
$$


**(My addition)** That's not WSS. The three sums of squares are linked by **TSS = WSS + BSS**: total variation around the overall mean splits into variation *within* clusters plus variation *between* cluster centers. Dividing through by TSS gives 1 − BSS/TSS = **WSS/TSS**, the *fraction* of the total variation left inside the clusters. The formula is right if you read its left side as "WSS as a share of TSS"; WSS itself is just the raw sum.

**(My addition) Worked on the notebook's data.** `kmeans_data.txt` has six points: (0,0,0), (0.1,0.1,0.1), (0.2,0.2,0.2), and (9,9,9), (9.1,9.1,9.1), (9.2,9.2,9.2). With k = 2 the centroids are (0.1,0.1,0.1) and (9.1,9.1,9.1).

- **WSS** = 0.12. Each cluster has two points 0.1 away from the centroid in each of 3 coordinates: 2 × 3 × 0.01 = 0.06 per cluster.
- **TSS** = 364.62, measured around the overall mean (4.6, 4.6, 4.6).
- **BSS** = TSS − WSS = 364.50.
- **WSS/TSS** = 0.12 ÷ 364.62 ≈ 0.0003. Almost all the variation is *between* the two clusters, which is what a clean split looks like.

### Choosing k: the scree plot and the elbow

Make a **scree plot** of WSS against the number of clusters k, and look for the **elbow**. Past that point, adding clusters reduces WSS only marginally. The notebook's intuition: before the elbow, each new cluster splits apart a real group; after it, you're mostly splitting well-formed clusters that should stay whole.

In the notebook's example plot, WSS falls steeply from about 88 (k = 1) to 47 (k = 2) and 32 (k = 3), then flattens. The elbow is around **k = 2 or 3**. Elbows are often judgment calls like this.

**Heads up:** **(my addition)** look closely at that plot: WSS goes **up** at k = 6 (about 21 → 22), and again around k = 11 and k = 14. With the best possible clustering, adding a cluster can never increase WSS, because you could always keep the old clustering and split off one point. So those bumps mean k-means got stuck in a worse local minimum at those k's. It's a real-world illustration of the "may not find the global optimum" weakness. (The unused `fido_kmeans.png` in the lab folder has the same kind of bump at k = 8.)

**Ungraded exercise 3 (the notebook's answer):** can WSS reach zero? Yes: set **k = n**, one cluster per observation. Every point is its own centroid, so WSS = 0. But nothing has been grouped, so it tells you nothing. **(My addition)** I checked the pattern on the six-point data: WSS is 364.62 at k = 1, 0.12 at k = 2, and 0 at k = 6. That's why you look for the elbow rather than the minimum.

### Initialization: random, k-means++, k-means||

This is the heart of the *k-Means Extensions* slides and the reading.

**Why it matters (slides 2–3).** Initialization is essential to the result. The simplest method picks k random points as the starting centroids. But k-means started this way performs poorly on both **efficiency and quality**: runtime can be exponential in the worst case, and the result can be far from the global optimum even after repeated random restarts. **Better initialization leads to better quality and faster convergence.**

**k-means++ (slide 4).** Choose centers in a controlled way: the centers already chosen **stochastically bias** the choice of the next one.

1. Choose one center uniformly at random from the data points.
2. For each point x not yet chosen, compute D(x), the distance from x to the nearest center already chosen.
3. Choose one new center at random, where each point's chance of being picked is proportional to **D(x)²**. Points far from every existing center are the most likely picks.
4. Repeat steps 2–3 until k centers have been chosen.
5. Run standard k-means from those centers.

**Its limitation (slide 5): it's sequential.** It needs **k passes** over the data, one per center, because each choice depends on the ones before it. On a cluster with a large k, that's k full scans of distributed data just to get started.

**k-means|| ("k-means parallel", slides 6–11).** The same idea, redesigned so each round can pick many centers at once. It's Spark's default initialization.

The **cost** of a set of centers C is the sum of squared distances from each point to its nearest center:

$$
\phi_Y(C) = \sum_{y \in Y} \min_{i=1,\dots,k} \lVert y - c_i \rVert^2
$$

This is just WSS again, now under the name "cost" or SSE. The goal of k-means is to choose centroids that minimize it.

The algorithm (Algorithm 2 from the paper, with the slides' notes):

```text
1: C <- sample one point uniformly at random from X
2: psi <- cost of X with respect to C
3: for O(log psi) rounds do
4:     C' <- sample each point x independently, with probability  l * d^2(x, C) / cost(C)
5:     C <- C union C'
6: end for
7: for each x in C, set w_x = number of points in X closer to x than to any other point in C
8: recluster the weighted points in C into k clusters
```

- **Lines 1–6** oversample. As in k-means++, far-away points are favored, but each round samples **many** points independently instead of one. **ℓ** (ell) is the **oversampling factor**: it controls how many extra candidates each round adds. You end up with more than k candidates.
- **Lines 7–8** cull the candidates back down to k. Each candidate gets a **weight**: the number of data points closest to it. Then the weighted candidates are clustered into k groups, and the final centroids are the averages of each group.
- **Note (slides 9–10):** the final centroids come from averaging, so they generally **aren't** any of the sampled points. In the slide's illustration, candidates with weights 20 and 10 average to one star, and candidates with weights 25 and 15 to another.

**How it parallelizes (slide 11).** The paper implements it in MapReduce:

- **Line 4:** each mapper samples its own chunk of points independently.
- **Line 7:** the points can be divided among mappers to count which candidate each is nearest to.
- **Computing the cost:** distribute chunks of X to mappers; mappers compute squared distances; a reducer adds them up.

**(My addition)** Why this is faster: k-means++ needs k passes, and k can be large. k-means|| needs only the number of sampling rounds, which is small: O(log ψ) in theory, about five in the paper's experiments. Spark's `initSteps` parameter sets it, and it defaults to **2** (confirmed in Spark's source). The final reclustering in line 8 runs on the small candidate set, not the full data, so it's cheap. It's the same pattern as Module 4: heavy work spread over partitions, a small summary combined at the end.

### k-means in Spark

**Setting the initialization.** `initMode` is `'random'` or `'k-means||'`; `'k-means||'` is the default.

**The methods:** `fit()` trains the model; `clusterCenters()` returns the centroids as a list of arrays; `transform()` assigns each row to its nearest center, the same fit/transform pattern as Module 4.

**The example** (your run):

```python
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler

df = spark.read.csv("kmeans_data.txt", header=True, inferSchema=True)   # 6 rows: f1, f2, f3
assembler = VectorAssembler(inputCols=['f1', 'f2', 'f3'], outputCol="features")
dataset = assembler.transform(df)

kmeans = KMeans().setK(2).setSeed(314).setMaxIter(10)
model = kmeans.fit(dataset)
predictions = model.transform(dataset)        # adds a 'prediction' column

evaluator = ClusteringEvaluator()
evaluator.evaluate(predictions)               # 0.9997530305375207
model.clusterCenters()                        # [array([9.1, 9.1, 9.1]), array([0.1, 0.1, 0.1])]
```

The three points near 0 got `prediction = 1` and the three near 9 got `prediction = 0`.

Three things to notice:

- **A sparse vector shows up.** The first row's features print as `(3,[],[])`: length 3, no nonzero positions. `VectorAssembler` chose the sparse format because every value is zero. It's Module 4's sparsity in action.
- **Cluster numbers are arbitrary labels.** **(My addition)** Cluster 0 is the high group here only because of how initialization happened to go. A different seed could swap them. Don't read meaning into the number itself.
- **The builder-style setters.** `KMeans().setK(2).setSeed(314).setMaxIter(10)` is the same as `KMeans(k=2, seed=314, maxIter=10)`. Both styles appear in the course.

**(My addition) Spark's defaults**, confirmed in the source, for anything you don't set: `k=2`, `initMode="k-means||"`, `initSteps=2`, `maxIter=20`, `tol=1e-4`, `distanceMeasure="euclidean"`. So the notebook's `setMaxIter(10)` *lowers* the cap from the default of 20. For `GaussianMixture`, the defaults are `k=2`, `maxIter=100`, `tol=0.01`.

**(My addition) What's distributed.** Each k-means iteration is one pass over the partitioned data. On each partition, every point is assigned to its nearest centroid, and the executor keeps a running **sum and count** of the points per cluster. Those small per-partition totals are combined, and the driver divides sum by count to get the new centroids. It's the combiner pattern again: only k sums and k counts travel over the network each iteration, never the points.

### The silhouette score

The silhouette score measures how consistently points are clustered. It ranges from −1 to 1:

- **Close to 1:** points are consistent. This is a good clustering.
- **Near 0:** clusters overlap.
- **Negative:** observations have been assigned to the wrong clusters.

**The algorithm:**

1. For each point, compute two averages:
   - **A**, the *mean intra-cluster distance*: the average distance from the point to the other points in its own cluster.
   - **B**, the *mean nearest-cluster distance*: the average distance from the point to the points in the **nearest other** cluster. You compute the average distance to each other cluster and take the smallest.
   - For a well-placed point, A should be much smaller than B.
2. For each point i, compute s(i) = (B − A) / max(A, B).
3. The silhouette score is the mean of s(i) over all points.

**The toy example** (the notebook's image). Point x₁ is in cluster C₁ with two neighbors at distances 3 and 5. Cluster C₂'s two points are at distances 6 and 8; cluster C₃'s are at 10 and 12.

- A = (3 + 5) / 2 = **4**
- B = min((6 + 8) / 2, (10 + 12) / 2) = min(7, 11) = **7**. C₂ is the nearest other cluster.
- s(x₁) = (7 − 4) / 7 = 3/7

**Heads up:** the image rounds 3/7 to 0.42. It's 0.4286, so **0.43**.

**Which distance does Spark use?** The output line says "Silhouette with squared euclidean distance": `ClusteringEvaluator` defaults to `metricName="silhouette"` (its only metric) and `distanceMeasure="squaredEuclidean"` (the alternative is `"cosine"`). **(My addition)** Squaring exaggerates the gap between near and far, so the score gets closer to 1 for well-separated data. I recomputed it on the six points: **0.99975** with squared distances, matching your output, but **0.985** with ordinary distances. Both say "excellent clustering," but keep the setting in mind if you compare scores across tools. scikit-learn's `silhouette_score` uses plain Euclidean by default.

### Gaussian mixture models (GMM)

Sometimes data isn't normally distributed as a whole, but it is a **mixture** of normal distributions. Bimodal or multimodal data is the example: each cluster might come from its own Gaussian. The notebook's image is a density curve with two equal humps, at about −2 and +1.5, and a dip between them. No single bell curve fits that shape, but two overlapping ones do.

A **Gaussian mixture model** is a weighted combination of Gaussian distributions. Each component k has:

- a **prior probability** π_k, its mixing weight. This is the "fixed probability" in the notebook's description: the chance that a random point came from component k.
- a **mean vector** μ_k
- a **covariance matrix** Σ_k

**(My addition) GMM vs. k-means.** k-means makes **hard** assignments: each point belongs to exactly one cluster. GMM makes **soft** ones: each point gets a *probability* of belonging to each component. Because each component has its own covariance matrix, GMM can also fit stretched, tilted, elliptical clusters, one of the k-means weaknesses from the spec table.

**Fitting it: expectation-maximization (EM).** At a high level:

1. **Initialize** the parameters, for example by randomly selecting observations.
2. **E-step (expectation):** for each point xᵢ, compute its posterior probability of belonging to each component, τ_ik = P(zᵢ = k | xᵢ). Every point is handled independently, so this is **embarrassingly parallel**: partition the data and send it to the workers.
3. **M-step (maximization):** workers use the τ_ik to compute **sufficient statistics** on their partitions:
   - effective count: N_k = Σᵢ τ_ik
   - component mean: μ_k = (1/N_k) Σᵢ τ_ik xᵢ, a weighted mean whose weights are the posteriors.

   Each worker computes partial sums for its own partition, and the partial results are aggregated into updated π_k, μ_k, and Σ_k.
4. Repeat the E- and M-steps until convergence.

**The overall strategy**, in the notebook's words: distribute the large workload (the calculations across observations), and aggregate statistics as a small job across workers. It's the same pattern as distributed gradient descent in Module 4 and k-means above.

**(My addition)** Notice k-means is a special case of this loop: its "E-step" gives each point probability 1 for its nearest cluster and 0 for the rest, and its "M-step" is the plain average.

**The example** (your run), reusing the k-means data:

```python
from pyspark.ml.clustering import GaussianMixture
gmm = GaussianMixture().setK(2).setSeed(314)
model = gmm.fit(dataset)
model.gaussiansDF.select("mean").show(truncate=False)
model.gaussiansDF.select("cov").show(truncate=False)
```

The component means come out at (0.1, 0.1, 0.1) and (9.1, 9.1, 9.1), agreeing with the k-means centroids to 13 decimal places (the notebook says "very close").

**Heads up:** **(my addition)** look at the covariance matrices. Every one of the nine entries is **0.006667** in both components. That's because every point in this dataset has f1 = f2 = f3: all six points lie on a single line through 3-D space. So the three features are perfectly correlated, and each covariance matrix is **singular** (it has rank 1). 0.006667 is the variance of (0, 0.1, 0.2) around 0.1: (0.01 + 0 + 0.01) / 3. It divides by n = 3, not n − 1, because EM uses the maximum-likelihood estimate. The fit works here, but a singular covariance matrix is a warning sign on real data: the Gaussian density isn't well defined in the directions with zero variance.

### Ungraded exercises 1 and 2

The cells are empty in your copy. **(My addition)** What to look for:

1. **Change the initialization:** `KMeans().setK(2).setSeed(314).setMaxIter(10).setInitMode("random")`. On six points in two obvious groups, you should get the same centroids either way. Initialization matters when clusters are less clear-cut or k is larger.
2. **Try different k:** with `setK(3)` and up, k-means has to split one of the two real groups. Watch the silhouette score fall from 0.9998 as it does.

## Part 2: Dimension reduction

This part covers the `dimensionality_reduction` notebook and the *SVD Summary* slides. It builds up linear algebra first (rank, eigenvectors), then PCA, then SVD, then how Spark computes SVD across a cluster.

### Rank of a matrix

The **rank** of a matrix A is the dimension of the vector space spanned by its columns (or rows): the maximum number of **linearly independent** columns. The **column rank** is the dimension of the column space and the **row rank** is the dimension of the row space. A fundamental result of linear algebra: **they are always equal.**

The notebook's two examples, with **(my addition)** why each has the rank it does:

```text
Rank 1:  [  1   1   0   2 ]      row 2 = -1 × row 1
         [ -1  -1   0  -2 ]      so only one independent row

Rank 2:  [  1   0   1 ]
         [ -2  -3   1 ]          row 3 = row 1 − row 2
         [  3   3   0 ]          so only two independent rows
```

**(My addition)** Why rank matters for this module: a matrix of rank r holds only r "directions" of information, however many columns it has. Dimension reduction bets that real data is *nearly* low-rank, so a few directions capture almost everything.

### Eigenvalues and eigenvectors

If multiplying a matrix A by a vector v just **scales** v by a constant λ,

$$
A v = \lambda v
$$

then λ is an **eigenvalue** and v is its **eigenvector**. The matrix doesn't rotate v; it only stretches or shrinks it (or flips it, if λ is negative).

The notebook's diagram labels A as an **n × n** matrix. **(My addition)** That label matters: eigenvectors only exist for **square** matrices, because Av has to land back in the same space as v. A data matrix is usually rectangular (rows ≠ columns). That's the gap SVD fills later.

The notebook's notes: eigenvalues have many practical uses, including data compression. The largest eigenvalue governs a system's long-term behavior. Efficiently estimating eigenvalues is a major focus of numerical analysis, which is where ARPACK comes in later.

### Why reduce dimensions?

For a data matrix with features along the columns, the **dimension is the number of features.** The notebook's reasons to reduce it:

- **Visualization.** You can plot two or three dimensions, not fifty.
- **The p ≫ n problem**, one form of the curse of dimensionality: more features (p) than observations (n). There aren't enough degrees of freedom to estimate a model.
- **Storage.** For example, regression's closed-form solution

  $$
  \hat\beta = (X^T X)^{-1} X^T Y
  $$

  needs the inverse of the **Gram matrix** XᵀX, which can be prohibitively large to store. It can be replaced with a lower-rank decomposition from SVD. **(My addition)** This is the normal-equation solver from Module 4's caching experiment. XᵀX is p × p, so it grows with the square of the number of features.
- **Denoising.** Even randomly generated data produces a correlation matrix with some extreme values, by chance. Compressing the information into fewer features reduces that noise. It's especially useful when the covariance matrix itself matters, as in mean-variance portfolio optimization in quantitative finance.

**(My addition)** That finance example connects directly to the lab. The Fidelity data has 1,731 features (trading days) and only 927 observations (funds), a textbook p > n case.

### Principal component analysis (PCA)

PCA is the primary dimension reduction technique. **It constructs new vectors that are linear combinations of the original ones**, hoping that a subset of them carries most of the signal (high signal-to-noise). The new vectors, the **principal components (PCs)**, have two special properties:

1. They form an **orthogonal basis**, so they're **uncorrelated** with each other.
2. They're ordered by variance. The first PC accounts for the most variability in the data; the second accounts for the next most while staying orthogonal to the first; and so on.

**Relationship to eigenvectors.** The PCs are the **eigenvectors of the data's covariance matrix.** They're usually computed by eigendecomposition of that matrix:

$$
\Sigma_{ij} = \frac{1}{n-1} \sum_{k=1}^{n} (x_{ki} - \bar{x}_i)(x_{kj} - \bar{x}_j)
\qquad\qquad
\Sigma = V \Lambda V^T
$$

The columns of V are the PCs, and the diagonal of Λ holds their eigenvalues. **(My addition)** Each eigenvalue *is* the variance of the data along its PC. So "the first PC has the most variance" means "the first PC has the largest eigenvalue," and the scree plot for PCA is just the eigenvalues in order.

**Heads up:** **(my addition)** the letter Σ does two jobs in this module. Here it's the **covariance matrix**. In SVD (below), Σ is the **diagonal matrix of singular values**. In the GMM section, Σ_k was a component's covariance. Check which one a formula means before reading it.

**The PCA picture** (`pca_img.gif`; despite the extension, it's a single still image, not an animation) shows PCA in two dimensions. About 100 points form a long diagonal cloud. The "1st dimension" arrow runs along the length of the cloud, the direction of greatest spread. The "2nd dimension" arrow crosses it at a right angle, along the short width.

**Heads up:** **(my addition)** the two axes use different scales (x from −10 to 6, y from −25 to 20), so angles on the plot are distorted. The arrows look perpendicular on screen, but in the data's actual units they wouldn't quite be. The idea is right; don't measure the angle off the picture.

**How many components?** There are as many PCs as original features, but you keep only the top k. **The top PCs make orthogonal, condensed features for a model.**

**Limitations of PCA:**

- **Numerical stability.** PCA forms the covariance matrix XᵀX. With big data, this can introduce rounding errors when computing the eigenvalues and eigenvectors. **(My addition)** Squaring the data squares the gap between large and small values, so small eigenvalues get swamped by rounding. Working with SVD directly avoids forming XᵀX.
- **It requires a covariance matrix**, which must be symmetric and positive semi-definite.
- **At high dimension, forming and storing the covariance matrix is impractical.** It's p × p.

**(My addition)** The notebook writes the covariance as XᵀX. That's true once X has been **centered** (each column's mean subtracted), and ignoring the 1/(n−1) scaling. Centering is part of the definition.

### PCA in Spark

The notebook runs the same example with both APIs. The data is three rows of length-5 vectors, and the first row is sparse:

```text
(0, 1, 0, 7, 0)   <- Vectors.sparse(5, {1: 1.0, 3: 7.0})
(2, 0, 3, 4, 5)
(4, 0, 0, 6, 7)
```

**RDD API** (`pyspark.mllib`), via a distributed `RowMatrix`:

```python
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg.distributed import RowMatrix

rows = sc.parallelize([Vectors.sparse(5, {1: 1.0, 3: 7.0}),
                       Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
                       Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)])
mat = RowMatrix(rows)                    # 3 rows, 5 columns
pc = mat.computePrincipalComponents(4)   # local dense matrix: 5 rows x 4 columns
projected = mat.multiply(pc)             # each row -> 4 new coordinates
```

The PC matrix is 5 × 4: one row per original feature, one column per component. Multiplying the 3 × 5 data by it gives 3 × 4, the data in the new coordinates.

**DataFrame API** (`pyspark.ml`), in the fit/transform pattern:

```python
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors as dfVectors

df = spark.createDataFrame(data, ["features"])
pca = PCA(k=4, inputCol="features", outputCol="pcaFeatures")
model = pca.fit(df)
result = model.transform(df)
```

**The alias.** Both APIs have a class called `Vectors`, so the notebook imports the DataFrame one as `dfVectors` to avoid a **namespace collision**. Mixing the two up is a common source of confusing type errors.

**The output** (identical from both APIs, up to the last decimal place):

```text
[ 1.6486, -4.0133, -1.0091, -5.2506]
[-4.6451, -1.1168, -1.0091, -5.2506]
[-6.4289, -5.3380, -1.0091, -5.2506]
```

**Heads up: the last two columns are meaningless.** **(My addition)** With only 3 observations, the centered data has rank at most 2, so only **2** components carry any variance. I checked: the covariance eigenvalues are 18.01 and 4.66, then zero. The first PC explains 79% of the variance and the second 21%. Asking for k = 4 makes Spark return two extra directions with zero variance, which is why columns 3 and 4 are the same number in every row. Their exact values are arbitrary, and a different machine could return different ones.

**(My addition) Two other details worth knowing:**

- **How Spark finds the PCs** (confirmed in `RowMatrix`'s source): its docstring notes that the rows "do not need to be centered first." For up to 65,535 columns, it computes the **covariance matrix** (which handles the centering) and decomposes that locally with LAPACK. Above that, it runs SVD on the data directly, skipping the covariance matrix. That's the "forming the covariance is impractical at high dimension" limitation, handled automatically.
- **Spark projects the *uncentered* data.** It uses the covariance to *find* the PCs, but `transform()` multiplies the original rows by them. Every value in a column is shifted by the same constant (the mean row's projection), so the distances between rows are unchanged. But the columns don't average to zero, as they would in a textbook PCA.
- **Signs are arbitrary.** If v is an eigenvector, so is −v. When I recomputed this in NumPy, the second column came out with the opposite sign. Both are correct.

### Singular value decomposition (SVD)

SVD is a **more general** factorization than eigendecomposition: it works for **any rectangular matrix**. It's one of the major accomplishments of linear algebra. It factors an m × n matrix into three matrices with special structure:

$$
A = U \Sigma V^T
$$

- **U** is an orthonormal m × m matrix. Its columns are the **left singular vectors**.
- **Σ** is a rectangular m × n **diagonal** matrix with nonnegative entries in descending order. Those diagonal entries are the **singular values** of A.
- **V** is an orthonormal n × n matrix. Its columns are the **right singular vectors**.

The singular values are the **square roots of the eigenvalues of AAᵀ**. **(My addition)** They're equally the square roots of the nonzero eigenvalues of AᵀA, which is what Spark actually uses (below). Both matrices share the same nonzero eigenvalues.

**The point: keep only the top k.** In red in the notebook: the purpose of SVD is to select only the top k singular values and their singular vectors. That gives an approximation of A:

$$
\hat A = \hat U \hat\Sigma \hat V^T
$$

| Matrix | Dimensions |
| --- | --- |
| Û | m × k |
| Σ̂ | k × k (and only its k diagonal values need storing) |
| V̂ᵀ | k × n |

These can be **substantially smaller** than A. The purposes: save storage, denoise, and recover the matrix's low-rank structure.

**(My addition) How much smaller.** A has m × n numbers. The truncated factors hold m × k + k + k × n = k(m + n + 1). For a 1,000,000 × 1,000 matrix kept at k = 10, that's about 10 million numbers instead of 1 billion: 1% of the original.

**(My addition) Why the top k is the right choice.** The **Eckart–Young theorem** says the truncated SVD is the **best possible** rank-k approximation of A: no other rank-k matrix is closer. And the approximation error has a simple formula: it's the square root of the sum of the squared singular values you dropped. That's the answer to the notebook's second exercise, below.

### PCA vs. SVD

Distinguishing them is a learning objective, and it's likely quiz material. **(My addition)** The notebook gives the pieces but never puts them side by side:

| | PCA | SVD |
| --- | --- | --- |
| What it is | A statistical technique: find the directions of greatest variance | A matrix factorization: A = UΣVᵀ |
| Works on | The covariance matrix (square, symmetric) | Any m × n matrix |
| Centering | Required: variance is measured around the mean | Not required: factors A as it is |
| Output | PCs (eigenvectors) and their variances (eigenvalues) | U, singular values, V |
| Main risk | Forming XᵀX loses precision; p × p can be huge | More general and more numerically stable |

**The connection:** run SVD on the **centered** data matrix X, and the columns of V are exactly the principal components. The squared singular values divided by (n − 1) are the PC variances. I checked this on the notebook's example: the centered data's singular values are 6.001 and 3.053, and 6.001² ÷ 2 = 18.01 and 3.053² ÷ 2 = 4.66, the covariance eigenvalues from the PCA section. So PCA is SVD on centered data, and computing it via SVD sidesteps the numerical stability problem.

Note the notebook's SVD example runs on the **uncentered** data, so its V is **not** the same as its PCs. That's expected: they answer different questions.

### SVD in Spark

The slides and notebook explain how Spark splits the work:

- The dataset is stored as a **`RowMatrix`** distributed across partitions (Module 4's "distribute by rows").
- **Large computations** (matrix–matrix and matrix–vector products across all the data) are **distributed**.
- **Small computations** (the eigenvalues and singular values themselves) are done **on the driver**.

The calculation splits into two cases. Here n is the number of **columns**.

**Special cases: n is small (n < 100), or k is large compared with n (k > n/2).**

1. **Compute the Gramian in a distributed way**, G = AᵀA, an n × n matrix. It's feasible because n is small. Each partition computes its own local AᵢᵀAᵢ, and those are summed. The dense Gramian is stored on the **driver**.
2. **On the driver**, eigendecompose it: G = VΛVᵀ. V holds the eigenvectors (the right singular vectors of A) and Λ holds the eigenvalues. The singular values are their square roots: Σ = diag(√λ₁, …, √λₙ).
3. **On the workers**, compute the left singular vectors: uᵢ = (1/σᵢ) A vᵢ. Now you have all the factors to reconstruct the matrix.

Cost: a **single pass** over the data, O(n²) storage on each executor, and O(n²k) time on the driver.

**Heads up:** **(my addition)** the Spark docs, which the notebook quotes, say O(n²) storage on each executor **and the driver**. The notebook drops "and driver." It matters: the dense n × n Gramian lives on the driver (slide 7 says so), and that's the whole reason this path is limited to small n.

**(My addition)** Step 1 is the combiner pattern once more. AᵀA is a sum of one term per row, so it splits across partitions exactly like the gradient sum in Module 4.

**General case: the Gramian won't fit in driver memory**, so the approach above won't work. Instead:

1. Start from the relationship AᵀAv = σ²v: the right singular vectors are eigenvectors of AᵀA, with eigenvalues σ².
2. Compute (AᵀA)v in a **distributed** way, **without ever forming AᵀA**. **(My addition)** Compute Av first (distributed across rows), then multiply Aᵀ by that result. Only vectors travel, never an n × n matrix.
3. Repeating that product builds a **Krylov subspace**, span{v, Mv, M²v, …, Mᵏ⁻¹v}. Projecting onto it gives a small **tridiagonal** matrix T.
4. Send T to **ARPACK** on the driver to compute the top eigenvalues and eigenvectors.

Cost: **O(k) passes** over the data, O(n) storage on each executor, and O(nk) storage on the driver (to hold V̂).

**ARPACK** is a Fortran library (the notebook says Fortran 77) for large-scale eigenvalue problems. It's highly optimized for sparse and large matrices.

**Heads up, two slide issues:**

1. **Slide 11** writes the Krylov subspace as 𝒦ₖ(A, v) with powers of A. **(My addition)** In this algorithm, the matrix being multiplied is **AᵀA**, not A (slide 10's own equation). The slide uses the generic textbook notation, where "A" stands for whatever matrix you're working with. Read it as M = AᵀA.
2. **Slide 12's code image is not ARPACK.** Its comments say it's a Fortran example of the **LAPACK** routine **DGESVD**, which computes a full dense SVD in one go, the opposite of ARPACK's iterative approach for huge matrices.

**(My addition) What Spark actually does, from `RowMatrix.computeSVD`'s source.** The two-case story is a simplification. The code chooses among **three** modes:

| Mode | When | What runs on the driver |
| --- | --- | --- |
| `local-svd` | Special case, and k ≥ n/3 | Full decomposition of the Gramian with **LAPACK** |
| `local-eigs` | Special case, and k < n/3 | Top k eigenvectors of the Gramian with **ARPACK** |
| `dist-eigs` | General case | **ARPACK**, driving distributed (AᵀA)v products |

(The source's special-case test is n < 100, or k > n/2 with n ≤ 15,000.) So slide 12's LAPACK image isn't irrelevant: LAPACK really is used, just in the special case, not the general one. The notebook's own example (n = 5, k = 4) takes the `local-svd` path.

The `WARN LAPACK: Failed to load implementation` lines in the notebook's output mean Spark couldn't find a native (compiled) LAPACK library and fell back to a slower Java version. They're harmless.

### The SVD example

```python
mat = RowMatrix(rows)                          # the same 3 x 5 data as the PCA example
svd = mat.computeSVD(4, computeU=True)
U = svd.U    # RowMatrix (distributed), 3 rows x 4
s = svd.s    # local dense vector
V = svd.V    # local dense matrix, 5 x 4
```

The singular values: **[13.029, 5.369, 2.533, 6.3 × 10⁻⁸]**.

**(My addition)** Which pieces are distributed matches the slides. U has one row per data row, so it stays a distributed `RowMatrix`; s and V are small, so they're local on the driver. With only 5 columns, this example falls under the **special case** (n < 100).

**Heads up:** **(my addition)** the fourth singular value, 6.3 × 10⁻⁸, is really **zero**. A 3 × 5 matrix has rank at most 3, so it has at most 3 nonzero singular values. The tiny value is rounding error, and the fourth column of U (entries around 10⁻⁸) is noise too. I recomputed the first three values in NumPy and they match to six decimal places.

### Ungraded exercises

**1. PCA scree plot.** Build a small matrix, compute its PCs, plot the variance explained by each, and pick the elbow. **(My addition)** Spark's DataFrame `PCAModel` has an `explainedVariance` attribute that gives you the scree values directly.

**2. SVD approximation error.** For k = 2, 3, 4, build the rank-k approximation and compute its **Frobenius norm** distance from the original, ‖M_act − M_approx‖_F: the square root of the sum of squared element-wise differences.

**(My addition)** On the notebook's own matrix, the distances come out as:

| k | ‖A − Â‖_F | √(sum of dropped σ²) |
| --- | --- | --- |
| 1 | 5.936 | √(5.369² + 2.533²) = 5.936 |
| 2 | 2.533 | √(2.533²) = 2.533 |
| 3 or 4 | 0 | nothing left to drop |

**What you should notice:** the error falls as k grows, and it's exactly the size of the singular values you left out. That's Eckart–Young in action. Once k reaches the rank (3), the approximation is exact.

## Module 5 lab: Clustering Fidelity mutual funds

The lab (Lab 4 on the agenda, due Oct 9) is worth **10 points**. It clusters mutual funds by how their prices actually **moved**, rather than by how they're described. Your copy is still the blank template, so this section maps each requirement to the module material and flags the traps, without solving it for you.

**The data** (`fido_returns_funds_on_rows.csv`):

- **927 rows**, one per mutual fund.
- **1,731 columns**, one per trading day from 2007-01-03 to 2013-11-08. The header row is the dates.
- Each value is the fund's **daily return**: the change in price from the previous trading day.

| Requirement | Points | Where it's covered |
| --- | --- | --- |
| Load modules; read the data into a Spark DataFrame | — | Module 3 reading; the k-means example |
| Assemble the features into one column; show the first five rows of **only** that column | 2 | `VectorAssembler` (k-means in Spark) |
| Train k-means: **k = 3**, **maxIter = 10**, **seed = 314** | 1 | k-means in Spark |
| Compute and print the silhouette score | 2 | `ClusteringEvaluator` |
| Define `kmeans_range(lower, upper, df)`: fit k-means for every k from lower to upper **inclusive**, same other parameters; return a **pandas** DataFrame of k and silhouette score | 2 | The k-means workflow, step 3 |
| Call it for k = 2 to 10 and print the result | 1 | — |
| Plot k (x-axis) against silhouette score (y-axis) | 1 | Choosing k |
| The silhouette score's time complexity | 1 | The silhouette score |

**Traps to watch for (my addition):**

1. **1,731 feature columns.** Don't type them out; build the `inputCols` list from `df.columns`. `inferSchema=True` has to scan 1,731 columns, so the read is slower than you're used to, but it works: every column is a number, with no missing values. The date names contain hyphens but no dots, so they don't need backtick escaping.
2. **Show *only* the features column**, and use `truncate` sensibly. Each vector has 1,731 entries, so a full print is unreadable.
3. **"Inclusive" upper bound.** Python's `range(lower, upper)` stops *before* `upper`, so an off-by-one would silently drop k = 10.
4. **Silhouette needs predictions.** `ClusteringEvaluator` evaluates the output of `model.transform()` (it needs the `prediction` column), not the model itself. Inside `kmeans_range`, each k needs its own fit, transform, and evaluate.
5. **Collect small results only.** Build the pandas DataFrame from a Python list of (k, score) pairs. The scores are just numbers, so there's no need to convert Spark DataFrames.
6. **Scaling isn't asked for here, and that's worth thinking about.** The workflow says to standardize, but every feature here is already in the same unit (a daily return). Standardizing each *day* would give a calm day the same weight as a crash day. The lab's instructions don't include a scaling step, so follow them. It's a good question to be ready to discuss.
7. **The complexity question is about the lecture's definition.** Work it out from the algorithm steps: for each point, how many distances do you compute? **(My addition)** Spark's `ClusteringEvaluator` doesn't compute it the textbook way. I confirmed this in its source (`ClusteringMetrics.scala`). For squared Euclidean distance, it precomputes a few summary statistics per cluster: the sum of the cluster's vectors, the sum of their squared lengths, and the count. From those it gets each point's A and B algebraically, without ever measuring point-to-point distances. The result is **exact**, not an approximation. So the complexity of the definition and the cost of Spark's implementation are different questions. Answer the one the lab asks, and say which you mean. The silhouette Wikipedia page linked in the notebook discusses the cost of the standard definition, if you want to check your reasoning.

**Things I noticed in the data (my addition):**

- **"Percentage change" is really a fraction.** The first fund's return on 2007-01-05 is −0.0104, which is a −1.04% move. The values range from about −0.26 to +0.22.
- **Four columns are all zeros**: 2007-01-03, 2010-01-18, 2011-01-17, and 2012-07-04. The first has no previous day to compare to; the other three are market holidays (MLK Day twice, and July 4). They add nothing to the distances but do no harm.
- **The most volatile day is 2008-10-13**, in the middle of the 2008 financial crisis. Days like that pull hard on the Euclidean distances.
- **There's no fund name or ticker column**, so you can say *how many* funds are in each cluster but not *which* funds. Step 6 of the workflow, "enrich with domain knowledge," isn't possible with this file alone.
- **One row is an exact duplicate** of another. It won't change much with 927 funds.
- **p > n:** 1,731 features, 927 observations. It's the dimension-reduction problem from Part 2, and the reason the module pairs the two topics. Running PCA before k-means would be a natural extension, but the lab doesn't ask for it.

**Heads up about the template:**

- `fido_kmeans.png` in the lab folder is a **WSSE** plot for k = 2 to 9. The lab asks you to plot **silhouette** scores for k = 2 to 10. The image looks like it's left over from an older version of the lab; don't mistake it for the expected output.
- The template is dated August 2023, and its data description predates the file's actual format (the "percentage" wording above).

## TL;DR: what to know for Quiz 5 and Lab 4

**Clustering**

- **The k-means loop:** assign each point to its nearest centroid, then move each centroid to the mean of its points, and repeat. It always stops, but possibly at a **local minimum**, which is why initialization matters.
- **WSS** is the sum of squared distances from points to their centroids. **TSS = WSS + BSS.** WSS reaches 0 at k = n, which is useless.
- **Choosing k:** the elbow of a scree plot, or the silhouette score.
- **Silhouette:** s = (B − A) / max(A, B), where A is the mean distance to your own cluster and B is the mean distance to the nearest other cluster. Average s over all points. Near 1 is good, near 0 means clusters overlap, and negative means points are misassigned. Be able to do the toy example (3/7) by hand.
- **Initialization:**

  | Method | How it picks centers | Drawback |
  | --- | --- | --- |
  | Random | Any k points | Can give poor results |
  | k-means++ | One at a time, favoring far points (probability ∝ D²) | k passes over the data |
  | k-means\|\| | Oversamples many candidates per round, over a few rounds; then weights them and reclusters down to k | Spark's default |

- **GMM:** each point gets a probability for each component (soft assignment), versus k-means' single cluster per point (hard). Each component has a weight π, a mean μ and a covariance Σ. It's fit with **EM**: the E-step handles each point independently, and the M-step aggregates partial sums from each partition.
- **Spark:** `VectorAssembler` → `KMeans(k, seed, maxIter)` → `fit` / `transform` → `clusterCenters()`. `ClusteringEvaluator` computes the silhouette with **squared Euclidean** distance by default.

**Dimension reduction**

- **Rank** is the number of linearly independent columns; row rank always equals column rank. **Eigenvectors** satisfy Av = λv, and only square matrices have them.
- **Why reduce dimensions:** visualization, p ≫ n, storage, and denoising.
- **PCA:** the PCs are **eigenvectors of the covariance matrix**. They're orthogonal (uncorrelated) and ordered by variance; each eigenvalue is the variance along its PC. Limitations: forming the covariance loses precision, and a p × p matrix is impractical when p is large.
- **SVD:** A = UΣVᵀ works for **any** m × n matrix. The singular values are square roots of the eigenvalues of AAᵀ (or AᵀA). Keep the top k: Û is m × k, Σ̂ is k × k, V̂ᵀ is k × n. That's the **best possible** rank-k approximation, and its error is exactly the size of the singular values you dropped.
- **PCA vs. SVD:** PCA is a statistical question (which directions have the most variance?); SVD is a factorization of any matrix. **PCA is SVD run on centered data.**
- **SVD in Spark:** the data is a `RowMatrix` across partitions. Big matrix products run on the executors; small eigenvalue work runs on the driver.
  - **Special case** (n < 100 or k > n/2): build the Gramian AᵀA across the partitions, then decompose it on the driver. One pass over the data.
  - **General case:** compute (AᵀA)v across the partitions, with **ARPACK** on the driver. O(k) passes.
- **Two APIs:** `RowMatrix.computePrincipalComponents` / `computeSVD` (RDD) and `PCA(k=…)` with fit/transform (DataFrame). Import the DataFrame one as `dfVectors` to avoid the name clash.

**The pattern behind all of it:** do the heavy per-row work on each partition, send small summaries (sums, counts) back, and combine them. k-means, GMM, the SVD Gramian and Spark's silhouette all work this way, like gradient descent in Module 4.

**Lab 4 checklist:**

- Build the input columns from `df.columns` (there are 1,731).
- Use k = 3, maxIter = 10, seed = 314.
- Evaluate `transform()` output, not the model.
- `kmeans_range` must include its upper bound.
- Return a pandas DataFrame.
- Plot silhouette score against k for k = 2 to 10.
- Answer the complexity question from the lecture's definition, not Spark's implementation.

**Know the notebook's errors, in case a question uses its wording:**

- The "WSS" formula actually gives WSS / TSS.
- It says "global maximum"; k-means minimizes WSS, so it's a minimum.
- The spec table says "random" initialization, but Spark's default is k-means||.
