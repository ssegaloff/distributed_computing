# DS 7200 Study Guide — Module 6

Oct 16, 2026 · @Sabine

## How to use this guide

Everything here comes from your repo's `06_recommender_feature_utils_and_pipelines` folder (your `sabine` branch as of 10/16): the three lecture notebooks (`recommender_systems`, `ml_feature_utilities`, `ml_pipelines`), every image they use, `reading.md`, the lab template and data files, and agendas 12–13. The notebook outputs quoted below are the **instructor's saved outputs**; you haven't rerun these notebooks yet, and your `autoscrobbler_sabine.ipynb` is still the blank template.

Same conventions as the earlier guides. Where I added an explanation or example that isn't in the course material, it's marked **(my addition)**. Where the source material itself has a problem, it's marked **Heads up**.

**What I checked, and how:**

- **Spark 4.2.0 source code** (the version in your `uv.lock`), for every claim about Spark internals or defaults that the notebooks don't state. Those are marked *confirmed in source*.
- **The Spark 4.2.0 docs** for collaborative filtering and pipelines.
- **Every link** in the notebooks and `reading.md` (table below).
- **Numbers I added** were recomputed in NumPy. **I could not run Spark itself for this guide** (I couldn't install PySpark in my workspace), so four notebook cells that have no saved output are worked out by hand from Spark's documented behavior, not from a Spark run. They're marked where they appear. Rerun those cells to confirm.

## Module 6: Recommenders, Feature Utilities & Pipelines

Module 5 was unsupervised learning on a feature matrix. Module 6 starts with a different kind of data: a sparse **user × item** matrix of interactions, and the matrix-factorization method (ALS) Spark uses to fill it in. It then steps back to the general toolkit for turning raw columns into features, and the `Pipeline` object that chains those steps with a model.

**Learning outcomes** (from `intro_and_learning_outcomes.md`):

- Implement recommender systems in PySpark using collaborative filtering.
- Execute the MLlib feature utilities package.
- Construct machine learning pipelines.
- Progress toward an end-to-end predictive modeling project on a large dataset.

**What's due (agendas 12–13).** **Tonight, Friday Oct 16, 11:59pm ET:** Lab 5 (Recommender Systems Using Music Tracks from Audioscrobbler), Quiz 6 (ML pipelines, Transformations, and Recommender Systems), and Journaling for Modules 1–6. Next: read *Large Scale Distributed Deep Networks* for the **Oct 19** discussion, and Quiz 7 (Distributed Model Training) is due **Oct 23**.

**In class (agenda 13):** the recommender notebook, the lab kickoff, the pipelines notebook, and the feature utilities notebook with emphasis on **`MaxAbsScaler`** and **`OneHotEncoder`**. Those two are likely quiz material.

### Readings and links

**The module reading** (`reading.md`) is Kevin Liao's *Prototyping a Recommender System Step by Step Part 2: Alternating Least Square (ALS) Matrix Factorization in Collaborative Filtering*.

**Heads up: the link in `reading.md` now opens a different article.** When I fetched the `towardsdatascience.com` URL on 10/16, it served *Recommender Systems – A Complete Guide to Machine Learning Models* (Casalegno, 2022), which has no Spark code. The article the reading describes is still on Medium at [medium.com/data-science/prototyping-a-recommender-system-step-by-step-part-2-…](https://medium.com/data-science/prototyping-a-recommender-system-step-by-step-part-2-alternating-least-square-als-matrix-4a76c58714a1). Its Spark code is embedded from GitHub, which my fetch tool couldn't render, so I can only confirm what the text says: it uses **explicit** movie ratings, wraps tuning in a function, and reports best settings of rank = 20, maxIter = 10, regParam = 0.05. Keep in mind that your lab is **implicit** feedback (below), so its tuning metric and some of its choices won't carry over.

**Textbook pointers.** The feature utilities and pipelines notebooks cite ***Learning Spark*, Ch. 11: Machine Learning with MLlib**. Same issue as Module 5: that's the **1st** edition's chapter. In the required 2nd edition, MLlib (including pipelines and feature transformers) is **Ch. 10**. The recommender notebook cites ***Advanced Analytics with Spark*, Ch. 3**, which isn't one of the course textbooks. **(My addition, general knowledge, not checked against the book)** That chapter builds an ALS recommender on this same Audioscrobbler dataset, which is likely where the lab came from.

**Link check (10/16):**

| Link | Where | Status |
| --- | --- | --- |
| Liao, *Prototyping … Part 2* (TDS URL) | `reading.md` | **Redirects to a different article.** Use the Medium URL above. |
| [Amazon Personalize + Glue blog](https://aws.amazon.com/blogs/architecture/automating-recommendation-engine-training-with-amazon-personalize-and-aws-glue/) | recommender nb | Works. Spivak, Jan 2021. It's the source of the architecture diagram. |
| [Personalize recipe docs](https://docs.aws.amazon.com/personalize/latest/dg/native-recipe-new-item-USER_PERSONALIZATION.html) | recommender nb | Works. Documents the exploration settings (below). |
| [Non-negative matrix factorization (Wikipedia)](https://en.wikipedia.org/wiki/Non-negative_matrix_factorization) | recommender nb | Not fetched; it's Wikipedia. |
| [Collaborative filtering (Wikipedia)](https://en.wikipedia.org/wiki/Collaborative_filtering) | recommender nb | Not fetched; it's Wikipedia. |
| [Spark feature docs](https://spark.apache.org/docs/latest/ml-features.html) | feature utils nb | Works (Spark 4.2.0). Most of the notebook's examples come from here. |
| [Spark pipeline docs](https://spark.apache.org/docs/latest/ml-pipeline.html) | pipelines nb | Works (Spark 4.2.0). |
| [*Spark Pipelines: Elegant Yet Powerful*](http://blog.insightdatalabs.com/spark-pipelines-elegant-yet-powerful/) | pipelines nb | Works. Includes how to write a custom transformer. |

**(My addition) One more doc page worth having:** [Spark collaborative filtering docs](https://spark.apache.org/docs/latest/ml-collaborative-filtering.html). The notebook doesn't link it, but it's the official description of Spark's ALS, its implicit-feedback mode, and the cold-start options. It links the implicit-feedback paper the lab's model is based on: *Collaborative Filtering for Implicit Feedback Datasets* (ICDM 2008).

---

## Part 1: Recommender systems

### The three datasets

A recommender needs three tables:

| Table | Columns |
| --- | --- |
| **User data** | `USER_ID` plus metadata |
| **Item data** | `ITEM_ID` plus metadata |
| **Interaction data** | `USER_ID`, `ITEM_ID`, `TIMESTAMP` |

The underlying assumption: **historical interactions will be useful in the future.**

**(My addition)** In the lab these are `artist_data.txt` (items), `user_artist_data.txt` (interactions, with a play count instead of a timestamp), and no user metadata at all. Collaborative filtering only needs the interactions table; the other two are for interpreting results and for cold start.

### Implicit vs. explicit feedback

- **Explicit feedback**: the user rates the item (stars). Harder to collect, and it may not match what the user actually does. The notebook's example: a user *says* they prefer classical but *listens* to hard rock.
- **Implicit feedback**: inferred from activity (listens, clicks, purchases), often collected automatically with **event triggers**. Easy to collect.

In practice, implicit feedback often works better. **The lab is implicit**: play counts, not ratings.

**(My addition) Why this changes the math, not just the data.** With explicit ratings, a missing cell means "unknown." With implicit data, a missing cell means "never played," which is weak evidence of *not* liking it. So the implicit model treats **every** cell as data, as a 0/1 preference with a confidence weight (below). That's also why RMSE, the usual explicit-rating metric, doesn't fit the lab, and why it asks for Recall@K.

### Collaborative filtering and cosine similarity

The idea: use the pattern of user–item preferences to recommend items the user is likely to prefer. The interaction matrix is generally **sparse** (most users touch few items). You compute a similarity metric and, for item *i*, return the top *k* most similar items. A popular metric is **cosine similarity**:

$$
\cos\theta = \frac{\mathbf A \cdot \mathbf B}{\lVert \mathbf A \rVert \, \lVert \mathbf B \rVert}
= \frac{\sum_i A_i B_i}{\sqrt{\sum_i A_i^2}\,\sqrt{\sum_i B_i^2}}
$$

Smaller angle → higher cosine → more similar.

**(My addition) Hand example.** Treat each user's play counts over three artists as a vector:

| Pair | Vectors | cos θ | Reading |
| --- | --- | --- | --- |
| A vs. B | (3, 0, 4) vs. (6, 0, 8) | **1.00** | Same taste, B just listens twice as much |
| A vs. C | (3, 0, 4) vs. (0, 5, 0) | **0.00** | No artists in common |
| D vs. E | (5, 4, 0) vs. (4, 0, 2) | 0.70 | Overlap on artist 1 only |

The first row is the point of cosine: it ignores **magnitude** (how much someone listens) and measures only **direction** (what they listen to). Euclidean distance would call A and B far apart.

**(My addition) User-based vs. item-based.** The notebook's prose describes comparing *items*, while its concept list says "finding similar *users*." Both are standard: compare rows (users) or columns (items) of the same matrix.

**Why it doesn't scale** (the notebook's note, with my addition on the reason): comparing every user with every other user is O(m²) pairs, each a length-n dot product. With millions of users, that's the expensive part ALS avoids.

### Alternating least squares (ALS)

ALS is popular because it **scales well**. It uses **latent factors**: a small number of unobservable dimensions that compress the data, "think PCA."

**The setup** (the notebook's images). The user–item matrix R is m × n (m users, n items). ALS approximates it with two thin matrices:

$$
R \approx U V^T, \qquad U \in \mathbb R^{m\times k}, \quad V \in \mathbb R^{n\times k}
$$

Each user is a length-k vector (a row of U), each item is a length-k vector (a row of V), and the predicted value for user i and item j is their dot product:

$$
\hat R_{ij} = U_i \cdot V_j^T
$$

**The objective** (the notebook's image): squared error on the **observed** cells only, plus a regularization penalty.

$$
\min_{U,V} \sum_{(i,j)\,\in\,\text{observed}} \left(R_{ij} - U_i V_j^T\right)^2 + \lambda\left(\lVert U\rVert^2 + \lVert V\rVert^2\right)
$$

**The strategy:**

1. Specify that objective.
2. **Fix V, solve for U** with ridge regression.
3. **Fix U, solve for V** with ridge regression.
4. Repeat 2–3 until convergence or max iterations.

**(My addition) Why alternating works.** Solving for U and V at once is hard: the product U·Vᵀ makes the problem non-convex. But if V is fixed, the objective is ordinary ridge regression for each user separately: user i's row is the coefficient vector, the fixed item vectors are the "features," and user i's observed values are the "targets." Ridge regression has a closed-form answer. So each half-step is easy, and each one can only lower the objective, the same "each step can only improve" logic as k-means in Module 5. Like k-means, it can stop at a local minimum.

**(My addition) Hand example, rank 1.** Three users, three artists, three cells missing:

```text
         a1   a2   a3
user 1 [  5    4    ? ]
user 2 [  ?    2    1 ]
user 3 [  4    ?    2 ]
```

With k = 1, each user and artist is a single number. Start with every artist factor at 1 (Spark starts randomly; 1s keep the arithmetic readable) and use λ = 0.1. With rank 1, fixing V makes user i's ridge solution

$$
u_i = \frac{\sum_j r_{ij}\, v_j}{\sum_j v_j^2 + \lambda\, n_i}
$$

where the sums run over user i's observed artists and n_i is how many there are. (The λ·n_i is Spark's scaling, explained below.)

- **User 1** (observed a1, a2): (5·1 + 4·1) / (1 + 1 + 0.1·2) = 9 / 2.2 = **4.09**
- **User 2** (a2, a3): (2 + 1) / 2.2 = **1.36**
- **User 3** (a1, a3): (4 + 2) / 2.2 = **2.73**

Then fix those and solve each artist the same way: a1 = 1.29, a2 = 1.02, a3 = 0.72. That's one iteration. Running it on in NumPy, the total objective falls every iteration (6.56, 5.51, 5.06, … 3.73) and settles by about iteration 50. The filled-in cells come out as:

| Missing cell | Prediction |
| --- | --- |
| user 1, a3 | 2.29 |
| user 2, a1 | 2.29 |
| user 3, a2 | 3.15 |

Worth trying one half-step by hand yourself before the quiz.

**Heads up: Spark scales λ, and the objective image doesn't show it.** Confirmed in source (`ALS.scala`: the solve uses `numExplicits * regParam`) and in the Spark docs: each user's penalty is multiplied by the number of ratings that user has, and each item's by the number it received. The docs call this **ALS-WR** ("weighted-λ-regularization") and say it makes `regParam` less dependent on dataset size. So the notebook's λ(‖U‖² + ‖V‖²) is the textbook form; Spark's effective penalty is λ(Σᵢ nᵢ‖Uᵢ‖² + Σⱼ nⱼ‖Vⱼ‖²).

**(My addition) Why regularization is needed at all.** The factorization isn't unique: multiply every user factor by 2 and divide every item factor by 2, and every prediction is unchanged. Without a penalty, the factors can drift to extreme sizes. With a penalty, the balanced solution wins.

### Implicit-feedback ALS (what the lab uses)

**(My addition, confirmed in Spark's source and docs.)** With `implicitPrefs=True`, Spark changes what it fits. For each user–item cell with raw value r (play count):

- **Preference** p = 1 if r > 0, else 0. This is the target being predicted.
- **Confidence** c = 1 + α·r. Unplayed cells get preference 0 with confidence 1; played cells get preference 1 with more confidence the more they were played.

The objective weights each cell's squared error by its confidence, and it runs over **all** cells, not just the observed ones. Spark makes that affordable with a trick from the paper: it precomputes VᵀV once per iteration (`computeYtY` in the source) and corrects only for the observed cells.

**The α the lab gives you matters.** Here's c = 1 + α·r for a few play counts:

| α | 1 play | 10 plays | 100 plays | 1,000 plays |
| --- | --- | --- | --- | --- |
| **0.01** (the lab) | 1.01 | 1.1 | 2 | 11 |
| 1.0 (Spark's default) | 2 | 11 | 101 | 1,001 |

With α = 0.01, a single play barely counts more than "never played." That's a real modeling choice baked into the lab, and worth a sentence in Task 8.

**The prediction is a preference score, not a play count.** `model.transform()` and the recommendation methods return numbers near 0–1 (they can fall outside it). Rank by them; don't read them as predicted plays.

### Cold start

A **new user** is a missing row; a **new item** is a missing column. ALS has no latent factors for either. The notebook's image shows both cases on a 5-star matrix: a new user with all `?`, and a new item column with all `?`.

Common strategies:

- Global averages (e.g., the mean rating) for unseen users or items.
- **Content-based features** (item metadata or user attributes) to estimate their latent factors.
- **Hybrid** approaches: ALS plus content-based or popularity-based recommendations.

**In Spark:** it does not generate factors for unseen users or items. Spark's default `coldStartStrategy` is **`"nan"`** (predictions for unseen ids come back NaN); **`"drop"`** removes those rows (confirmed in source and docs). The docs give the reason: with NaN in the predictions, an evaluation metric would come out NaN too. **(My addition)** This matters for the lab's validation and test sets: a user who appears there but not in training can't get recommendations.

### Evaluating: Recall@K and Precision@K

Recall in general is true positives / all positives. For a recommender that returns K items:

$$
\text{Recall@}K = \frac{\#\text{ relevant items in the top }K}{\text{total }\#\text{ relevant items}}
\qquad
\text{Precision@}K = \frac{\#\text{ relevant items in the top }K}{K}
$$

The notebook's two examples, both with K = 10 and 9 of the 10 recommendations relevant:

| | Total relevant | Recall@10 | Precision@10 |
| --- | --- | --- | --- |
| Example 1 | 15 | 9/15 = **0.60** | 9/10 = **0.90** |
| Example 2 | 100 | 9/100 = **0.09** | 9/10 = **0.90** |

Precision asks "how many recommendations were good?" Recall asks "how much of what the user likes did we find?" When the user has far more relevant items than K, Recall@K is capped low no matter how good the model is: in Example 2 it can't exceed 10/100.

**(My addition) What "relevant" means in the lab.** There are no ratings, so you have to define it. The natural choice is: an artist is relevant to a user if the user played it in the held-out set (validation or test). That's a decision you'll want to state in Task 4.

**(My addition) Spark has this metric built in.** `pyspark.ml.evaluation.RankingEvaluator` (since 3.0) supports `metricName="recallAtK"` and `"precisionAtK"`, with `k` defaulting to 10 (confirmed in source). Two things the docs make easy to miss:

- It needs **two array-of-double columns**: the recommended ids in rank order (`predictionCol`) and the relevant ids (`labelCol`). Artist ids are integers, so you'd cast. Confirmed in source.
- It returns the **average** recall across users. For a user with no relevant items, it uses 0 and logs a warning.

Whether you use it or compute recall with Python sets is your call. Sets are easy to check by hand for Test User 1; the evaluator scales to every user in the test set.

### ALS in Spark

From the notebook:

```python
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

als = ALS(userCol="user_id", itemCol="item_id", ratingCol="rating",
          coldStartStrategy="drop")
model = als.fit(ratings_df)
predictions = model.transform(ratings_df)
```

Both the RDD API (`pyspark.mllib.recommendation`) and the DataFrame API (`pyspark.ml.recommendation`) have ALS. The lab requires the DataFrame one.

**Hyperparameters** (the notebook's list, plus Spark 4.2.0's defaults, confirmed in source):

| Param | Meaning | Default |
| --- | --- | --- |
| `rank` | Number of latent factors k (columns of U and V) | 10 |
| `maxIter` | Number of alternating iterations | 10 |
| `regParam` | λ. Higher resists overfitting; too high hurts accuracy | 0.1 |
| `alpha` | Weight of observed vs. unobserved interactions (implicit only) | 1.0 |
| `coldStartStrategy` | `"nan"` or `"drop"` | `"nan"` |
| **`implicitPrefs`** | Explicit (False) or implicit (True) model | **False** |
| `nonnegative` | Constrain factors to be ≥ 0 | False |

**Heads up: `implicitPrefs` isn't in the notebook's list, and the lab needs it.** It defaults to **False**. If you leave it out, Spark fits an explicit model to play counts, treating "played 900 times" as a 900-star rating. The lab's "train the ALS implicit model" means `implicitPrefs=True`.

**Heads up: the example imports `RegressionEvaluator`.** That computes RMSE-style metrics on predicted vs. actual values, which suits explicit ratings. For the lab's implicit model and Recall@K, it's not the right tool.

**(My addition) Other things confirmed in Spark's source that the lab will hit:**

- **ids must be integers.** User and item columns must be numeric with no fractional part and fit in a 32-bit integer; otherwise `fit` raises an error.
- **Recommendation methods:** `recommendForAllUsers(n)` and `recommendForUserSubset(df, n)`. The second takes a DataFrame containing the user column and returns, per user, an array of (item, score) structs.
- **They do not filter out items the user already has.** If a user played an artist in training, it can still appear in their top 10. Whether to exclude those before computing recall is a real design choice; see the lab section.

**How it's distributed** (the notebook): each row of U can be solved independently across workers, and so can each row of V. Spark uses **block partitioning** to reduce network traffic and scales to millions of users and items.

**(My addition) What block partitioning means.** Users are split into `numUserBlocks` groups and items into `numItemBlocks` groups (10 each by default, confirmed in source). To update a block of users, an executor needs the item factors for only the items those users touched. Spark works out once which item factors each user block needs and ships only those each iteration, rather than broadcasting all of V. It's the same principle as Modules 4–5: move small factor vectors, never the full matrix.

### Amazon Personalize and the architecture diagram

Amazon Personalize is a fully managed recommender. Its algorithms are called **recipes**. It has a parameter controlling **exploration vs. exploitation**:

- **Explore**: recommend items with less interaction data more often, to learn about them.
- **Exploit**: recommend what's known to be relevant.

**(My addition, from the linked recipe page)** That's `explorationWeight`, default **0.3** on a 0–1 scale (0 = no exploration), plus `explorationItemAgeCutOff` (default 30 days), which limits exploration to newer items. The page now recommends the newer *User-Personalization-v2* recipe.

**The diagram** (retail example): three source stores (order management, product catalog, profile management) feed **AWS Glue**, which produces the three datasets from the top of this section (interactions, item metadata, user metadata). Those land in **Amazon S3**, train **Amazon Personalize**, and are served through a recommendation API. **(My addition)** The linked blog adds that Glue runs Spark ETL jobs, and that AWS Step Functions and Lambda kick off retraining. Module 8 covers AWS services.

**Heads up:** the folder includes `nnf.png` (W × H ≈ V, a non-negative matrix factorization diagram), and the sources link the NMF Wikipedia page, but the notebook never uses either. **(My addition)** The connection: NMF is matrix factorization with factors constrained to be ≥ 0, which is what ALS's `nonnegative=True` does. Note the letters: there, V is the *data* matrix, not the item factors.

---

## Part 2: ML feature utilities

The notebook groups MLlib's feature tools into three families:

| Family | What it does | Covered in the notebook |
| --- | --- | --- |
| **Extractors** | Build features from raw data | Listed (TF-IDF, Word2Vec, CountVectorizer) but not demonstrated. `HashingTF` appears in the pipelines notebook. |
| **Transformers** | Change existing features | Tokenizer, StopWordsRemover, NGram, Binarizer, OneHotEncoder, Normalizer, StandardScaler, MaxAbsScaler, Bucketizer, VectorAssembler, Imputer |
| **Selectors** | Pick a subset of features | ChiSqSelector |

**Heads up:** this notebook was last updated **Aug 2023**, and four of its cells (Normalizer, Bucketizer, Imputer, ChiSqSelector) have **no saved output**. I worked those out by hand below; rerun them to confirm.

### Text: Tokenizer, StopWordsRemover, NGram

- **`Tokenizer`** splits text into terms: "Hi I heard about Spark" → `[hi, i, heard, about, spark]`. **(My addition, confirmed in source)** It lowercases and splits on whitespace only, so punctuation stays attached: "tacos!!" stays one token. `RegexTokenizer` gives more control.
- **`StopWordsRemover`** drops uninformative words using a default list (or yours): `[I, saw, the, red, balloon]` → `[saw, red, balloon]`. Note it kept the capitalized "Mary": matching is case-insensitive by default, and "Mary" isn't a stop word.
- **`NGram(n=2)`** makes sequences of n adjacent tokens: `[Hi, I, heard, …]` → `[Hi I, I heard, heard about, …]`.

### Binarizer

Values **greater than** the threshold become 1.0; values **equal to or less** become 0.0. With threshold 0.5: 0.1 → 0, 0.8 → 1, 0.2 → 0. Since Spark 3.0 it takes multiple columns with one threshold each. In the notebook's example, 0.62 vs. threshold 0.61 → 1, and 0.51 → 0.

### StringIndexer and OneHotEncoder (emphasized in class)

**`StringIndexer`** maps string labels to indices in [0, numLabels), ordered by **frequency**: the most frequent label gets 0. In the example, "a" appears 3 times → 0, "c" twice → 1, "b" once → 2. **(My addition, confirmed in source)** Ties are broken alphabetically, and `stringOrderType` can switch to alphabetical ordering.

**`OneHotEncoder`** maps each index to a binary vector with **at most one** 1, which is dummy coding. It lets models that expect numbers use categorical features.

The output:

```text
a  0.0  (2,[0],[1.0])     -> [1, 0]
c  1.0  (2,[1],[1.0])     -> [0, 1]
b  2.0  (2,[],[])         -> [0, 0]
```

**(My addition) Why three categories give length-2 vectors.** `dropLast` defaults to **True** (confirmed in source), so the last index gets the all-zeros vector. That's the reference category of dummy coding. Keeping all three columns would make them sum to 1 in every row, which is perfectly collinear with a model's intercept. The cost: the reference category is whichever one got the last index, here the *least frequent* label.

**(My addition) Two more things to know:**

- **The output is sparse**, the Module 4 format: `(size, [indices], [values])`. One-hot vectors are mostly zeros, so this is efficient.
- **`OneHotEncoder` is an Estimator** (since Spark 3.0, confirmed in source): `encoder.fit(indexed)` learns how many categories there are, then the fitted model transforms. That's why the notebook calls `.fit()` before `.transform()`. It matters in the pipelines section.

### Scaling: Normalizer, StandardScaler, MaxAbsScaler

**(My addition) The key distinction:** `Normalizer` works on each **row**; `StandardScaler` and `MaxAbsScaler` work on each **column** (feature).

**`Normalizer`** rescales each feature vector (row) to unit p-norm, default p = 2. The example uses p = 1 (sum of absolute values = 1). No saved output; by hand:

| Input row | L¹ norm | Output |
| --- | --- | --- |
| [1.0, 0.5, −1.0] | 2.5 | [0.4, 0.2, −0.4] |
| [2.0, 1.0, 1.0] | 4 | [0.5, 0.25, 0.25] |
| [4.0, 10.0, 2.0] | 16 | [0.25, 0.625, 0.125] |

**Heads up:** the notebook says the Normalizer is "standardizing the features" to put them "on the same scale." That describes column scaling. Normalizer makes each *row* the same length, so it compares rows by their pattern, not their size. **(My addition)** That's the cosine-similarity idea from Part 1: after L² normalization, a plain dot product *is* the cosine.

**`StandardScaler`** gives each feature mean 0 and standard deviation 1. Parameters: `withStd=True` and **`withMean=False`** by default (confirmed in source). So by default it only **divides by the standard deviation and does not center.** The notebook explains why: centering subtracts the mean from every entry, turning the zeros into nonzeros, and the sparse vector becomes dense. No example is given.

**`MaxAbsScaler`** divides each feature by its maximum absolute value, so values land in [−1, 1]. It does **not** center, so zeros stay zero and **sparsity is preserved**. That's why it's the notebook's recommendation for sparse data. The example, column by column:

| Column | Values | max \|x\| | Scaled |
| --- | --- | --- | --- |
| 1 | 1, 2, 4 | 4 | 0.25, 0.5, 1.0 |
| 2 | 0.1, 1, 10 | 10 | 0.01, 0.1, 1.0 |
| 3 | −8, −4, 6 | 8 | −1.0, −0.5, 0.75 |

The printed `0.010000000000000002` is floating-point rounding of 0.1 / 10.

**(My addition) Tradeoff:** MaxAbsScaler is driven by one extreme value per column, so a single outlier squashes everything else toward 0. StandardScaler uses the standard deviation, which is less sensitive to a single point, but centering costs sparsity. Like `fit`/`transform` throughout MLlib, both learn their statistics in `fit()`; Module 5's combiner pattern applies (per-partition max or sum, then combine).

### Bucketizer

Turns a continuous column into bucket indices. n + 1 **splits** make n buckets. Each bucket is [x, y), except the last, which also includes its upper end. Splits must be strictly increasing. Values outside the splits are errors unless you include −∞ and +∞.

The example uses splits [−∞, −0.5, 0, 0.5, ∞], so 4 buckets. No saved output; by hand:

| Value | Bucket | Why |
| --- | --- | --- |
| −999.9 | 0 | in [−∞, −0.5) |
| −0.5 | 1 | lower bound is included: [−0.5, 0) |
| −0.3 | 1 | |
| 0.0 | 2 | [0, 0.5) |
| 0.2 | 2 | |
| 999.9 | 3 | [0.5, ∞] |

The boundary cases (−0.5 and 0.0) are the quiz-worthy ones.

### VectorAssembler

Combines numeric, boolean, and vector columns into one vector column, in the order listed. `hour=18`, `mobile=1.0`, and `userFeatures=[0, 10, 0.5]` become `[18, 1, 0, 10, 0.5]`: the vector column is flattened in place. You used this in Module 5.

### Imputer

Fills missing values (NaN by default) with the column **mean** (default) or median. It doesn't support categorical features. No saved output; by hand, the means of the non-missing values are a: (1 + 2 + 4 + 5) / 4 = **3.0** and b: (3 + 4 + 5) / 3 = **4.0**, so `out_a` = [1, 2, 3, 4, 5] and `out_b` = [4, 4, 3, 4, 5].

**(My addition)** `Imputer` is an Estimator: the means are learned in `fit()`. If you fit it on all your data before splitting, the test set's values leak into the training means. Pipelines (Part 3) are the standard guard against that.

### ChiSqSelector

Uses the chi-squared test of independence between each feature and the label to select features. It needs labeled data with **categorical** features. `numTopFeatures` keeps the top n by the test. The other modes are percentile, fpr, fdr, and fwe.

The example keeps 2 of 4 features. No saved output; by hand (I computed the chi-squared statistics in NumPy), features 1 and 2 have p ≈ 0.39 and features 3 and 4 have p ≈ 0.22, so it keeps **features 3 and 4**: `[18.0, 1.0]`, `[12.0, 0.0]`, `[15.0, 0.1]`. That matches the output in Spark's own docs for this example.

**Heads up, two things:**

1. **`ChiSqSelector` is deprecated** since Spark 3.1.1 in favor of `UnivariateFeatureSelector` (confirmed in source). It still works in 4.2.0.
2. **(My addition)** Look at *why* features 3 and 4 win. The test treats each distinct value as a category. In features 3 and 4, all three rows have different values, so each value perfectly identifies one row, and therefore its label. That's overfitting, not signal. On three rows, any all-distinct column looks maximally "dependent" on the label.

### Ungraded exercises

1. **Chain Tokenizer → StopWordsRemover → NGram** on your own sentences. Each stage's `outputCol` is the next one's `inputCol`. **(My addition)** This is a pipeline by hand; after Part 3 you could redo it as one.
2. **Combine a scaled numeric feature with a text feature in `VectorAssembler`.** **(My addition)** One catch: `VectorAssembler` takes numeric, boolean, or vector columns, not arrays of strings. The text has to become a vector first (for example, Tokenizer then `HashingTF`). And `StandardScaler` needs a vector input, so wrap the numeric column with its own `VectorAssembler` first.

---

## Part 3: ML pipelines

### The vocabulary

| Term | Meaning | Examples |
| --- | --- | --- |
| **Transformer** | Turns one DataFrame into another with `.transform()` | `Tokenizer`, `HashingTF`, `VectorAssembler`, any fitted model |
| **Estimator** | An algorithm that is **fit** on a DataFrame; `.fit()` returns a Transformer | `LogisticRegression`, `StringIndexer`, `OneHotEncoder`, `StandardScaler`, `ALS` |
| **Parameter** | A property of an estimator, set in the constructor or with setters | `lr.setMaxIter(10).setRegParam(0.01)` |
| **Pipeline** | A sequence of Transformers and Estimators that specifies a workflow | |

**(My addition)** Every fitted model is itself a Transformer: `ALSModel`, `KMeansModel`, `OneHotEncoderModel`. That's the fit/transform pattern you've used since Module 4, given names.

### Why pipelines

A pipeline keeps every step, from preprocessing and feature extraction to model fitting, together from start to finish, similar to scikit-learn's. Tracking those steps by hand is error-prone; for example, you could **train on the test set by accident**.

Pipelines can be **saved and loaded** and applied to any dataset with the right columns, in training mode or **scoring** mode (making predictions, also called **inference**). The scoring code shrinks to "load the pipeline, pass it data."

**(My addition) How "training on the test set" actually happens.** Many feature steps are Estimators that learn statistics: StringIndexer learns category frequencies, StandardScaler and MaxAbsScaler learn column statistics, Imputer learns means. If you fit those on the full dataset and then split, the test rows have shaped the training features. With a pipeline you call `pipeline.fit(train_df)` once, so *every* learned statistic comes from training data only, and `model.transform(test_df)` reuses them unchanged.

### What `pipeline.fit()` does

The schematic (`ml_pipeline_graph.png`) shows a fitted `PipelineModel`: Tokenizer → HashingTF → Logistic Regression Model, carrying raw text → words → feature vectors → predictions. The cylinders are DataFrames.

**(My addition, confirmed in `pipeline.py`'s source and the Spark docs.)** `fit()` walks the stages in order:

- A **Transformer** stage: call `.transform()` and pass the result on.
- An **Estimator** stage: call `.fit()` on the current DataFrame, keep the fitted model, and transform the data with it before the next stage. (The very last estimator is fit but not used to transform, since nothing follows it.)

The result is a `PipelineModel`, which is a **Transformer** with the same stages, every Estimator replaced by its fitted model. A `Pipeline` itself is an Estimator.

### The pipeline example

Restaurant reviews: `label` (1 = like), `review` (text), `rating` (integer stars).

```python
tok = Tokenizer(inputCol="review", outputCol="words")
htf = HashingTF(inputCol="words", outputCol="tf", numFeatures=200)
ohe = OneHotEncoder(inputCol="rating", outputCol="rc")
va  = VectorAssembler(inputCols=["tf", "rc"], outputCol="features")
lr  = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10, regParam=0.01)

pipeline = Pipeline(stages=[tok, htf, ohe, va, lr])
model = pipeline.fit(train_df)
prediction = model.transform(test_df)
```

The two branches, text → `tf` and rating → `rc`, meet at the assembler. **(Spark docs)** Stages can form a DAG like this, defined implicitly by the column names, as long as they're listed in an order where each input exists before it's used.

**The output:** all three test reviews are predicted 1 (like), with probability about 0.97. The first one ("ZERO STARS") is really a 0. The notebook's comment is that the training set is tiny (three rows). **I don't know** exactly why the rating feature didn't pull the first one toward 0; I couldn't rerun Spark to investigate.

**Heads up, four things in this notebook:**

1. **"Each of the preprocessing steps is a Transformer"** isn't right in Spark 3+. `OneHotEncoder` is an **Estimator** (it learns how many categories there are). So this pipeline has **two** estimators, `ohe` and `lr`, and `fit()` fits both.
2. **(My addition) The rating encoding is odd.** Confirmed in source: given a numeric input, `OneHotEncoder` sizes the vector as max value + 1. The ratings are 1 and 5, so there are 6 categories (0–5), and `dropLast` makes the vectors length 5. Rating **5 is the last category, so it's encoded as all zeros**, and slots 0, 2, 3, 4 never occur. It works, but `StringIndexer` first would be the usual approach, or treating rating as numeric.
3. **Unused RDD-API import.** The first cell imports `pyspark.mllib.linalg.Vectors` (the RDD API) and never uses it. Module 5's namespace-collision warning applies.
4. **`from pyspark.ml.feature import *`.** **(My addition)** A wildcard import is convenient for a demo, but best practice in real code is to import names explicitly: a reader can see where `Tokenizer` comes from, and you can't silently shadow another `Vectors` or `PCA`. The cost is a longer import line.

**(My addition) `HashingTF` and its tradeoff.** It turns a list of words into a fixed-length count vector by hashing each word to an index (MurmurHash3 mod `numFeatures`, confirmed in source; default 2¹⁸ = 262,144). There's no vocabulary to store or learn, so it's a Transformer, not an Estimator, and it works on any new word. The cost: different words can hash to the same slot (**collisions**), and you can't map a slot back to its word. `numFeatures=200` makes collisions likely; the source recommends a power of two. `CountVectorizer` is the alternative: it learns a vocabulary in `fit()` (an Estimator), with no collisions but more memory.

### Custom transformers

A custom Transformer must implement `transform`, specify an `inputCol` and `outputCol`, and accept and return a DataFrame. The *Elegant Yet Powerful* blog has a worked example.

### Saving and loading

Useful for returning to development later, and for scoring in production. The notebook doesn't show the code. **(From the Spark docs)** In Python:

```python
from pyspark.ml import PipelineModel

model.write().overwrite().save("models/review_lr")    # the fitted pipeline
same_model = PipelineModel.load("models/review_lr")
```

You can also save the unfitted `Pipeline`. **(My addition)** Like `saveAsTextFile` in your learnings file, `save` writes a **directory**, and re-saving without `.overwrite()` fails because the path exists.

**(My addition) Where tuning fits.** The notebook lists "model tuning" as a pipeline step, but in Spark it isn't a stage: `CrossValidator` or `TrainValidationSplit` wraps the whole pipeline as its estimator and refits it per parameter setting. That way every fold also refits the feature steps, so no fold's statistics leak.

### Ungraded exercise

Copy the pipeline, pass data, show predictions, and measure accuracy. **(My addition)** For accuracy, `MulticlassClassificationEvaluator(metricName="accuracy")` works on the `prediction` column, or compare `label` to `prediction` directly.

---

## Lab 5: Music recommendation (Audioscrobbler)

Worth **10 points**, due **tonight (Oct 16, 11:59pm ET)**. You build an implicit ALS recommender on user listening data, using the **DataFrame API** throughout. This section maps the tasks to the module and flags the traps, without solving them.

### The data

| File | Columns | Where |
| --- | --- | --- |
| `user_artist_data.txt` | userid, artistid, playcount | **Not in the repo.** On Rivanna: `/standard/ds7200-apt4c/large_datasets/user_artist_data.txt` |
| `artist_data.txt` | artistid, artist_name | In the lab folder |
| `artist_alias.txt` | badid, goodid (misspelled artist → correct id) | In the lab folder |

The README says the dump is from 6 May 2005, covers about 150,000 users, and that the alias file isn't fully merged yet.

**A choice to make before you start: where to run.** The interaction file only lives on Rivanna.

| Option | Pros | Cons |
| --- | --- | --- |
| Copy the file to Beans-Legion (e.g., `scp`) and run locally | Your usual setup and environment; no queue | One large transfer; local mode on one machine |
| Run on Rivanna | Data is already there | Your rule is Rivanna for genuine multi-node work only; this lab doesn't require multi-node |

**(General knowledge, not verified: I can't see the file.)** I believe the full file is roughly 24 million rows and a few hundred MB, so 20% is about 5 million rows: a reasonable size for local mode on 47 GB. Check `wc -l` once you have it. Also check the delimiter yourself before writing the parser; the README doesn't say what separates the columns.

**What I found in the two files I could check (my addition):**

- **`artist_data.txt` is tab-separated** and has **104,700** lines, but **12 stray carriage returns** split some names across lines, leaving **8 lines with no tab** (e.g., `Aya Hisakawa`). The max artist id is 10,599,847, which fits in a 32-bit integer, so ALS will accept it.
- **`artist_alias.txt`** has **193,027** lines; **2,135** of them have an **empty badid**. If you use the alias file, drop those first.
- Neither file is needed for the graded steps except for showing **artist names** in Task 8. If you join names in, those malformed lines are the ones that will break a naive parse.

### Preprocessing (in this order)

1. **Sample 20%** of the interactions with **seed = 314** (and use 314 everywhere).
2. **Remove users with fewer than 10 distinct artists.** Ignore aliasing in this step.
3. **Split 60 / 20 / 20** into train / validation / test.

### The tasks

| Task | Points | Where it's covered |
| --- | --- | --- |
| 1. Show the first row of the training set | 1 | — |
| 2. Show the first row of the test set ("Test User 1") | 1 | — |
| **ALS Model 1:** implicit, rank = 8, maxIter = 5, regParam = 0.02, alpha = 0.01 | — | ALS in Spark; implicit ALS |
| 3. 10 recommendations for Test User 1 | 2 | Recommendation methods |
| 4. Explain Recall@K for recommenders | 1 | Evaluating |
| 5. Recall@K for Test User 1 | 1 | Evaluating |
| **ALS Model 2:** tune (rank, maxIter, regParam) on validation to maximize Recall@K; alpha fixed at 0.01 | — | Hyperparameters |
| 6. Report the best values and their validation Recall@K | 2 | — |
| Retrain the best configuration on train + validation | — | — |
| 7. Recall@K on the test set | 1 | Evaluating |
| 8. Recall@K for Test User 1; justify with the recommendations and the user's activity. Did tuning help this user? | 1 | Everything above |

### Traps to watch for (my addition)

1. **`implicitPrefs=True`.** It defaults to False, and the notebook's list doesn't mention it. Without it, you're fitting an explicit model to play counts.
2. **`sample(fraction=0.2, seed=314)` is approximate.** It keeps each row with probability 0.2, so you'll get *about* 20%. That's expected; don't try to force an exact count.
3. **Count *distinct* artists in step 2.** "Listened to fewer than 10 artists" means distinct artists per user in your 20% sample, not rows or total plays. Keep users with 10 or more.
4. **Make the split reproducible.** Spark's own ALS source has an error message about this: if the input to `sample` or `randomSplit` isn't deterministic, the splits can change between actions, and it suggests sorting before splitting or checkpointing. A practical pattern is to cache the preprocessed DataFrame before `randomSplit([0.6, 0.2, 0.2], seed=314)`. The tradeoff: caching costs memory (and needs `unpersist()` later, per your learnings file), but without it, Spark may recompute the sample and filter for each split and each action.
5. **"First row" has no guaranteed order.** `show(1)` returns whatever row the first partition yields. That's fine as long as you identify Test User 1 **once**, store the user id in a variable, and reuse it; don't call `first()` again later.
6. **Test User 1 might be a cold-start user.** With a row-level 60/20/20 split, a user who appears in the test set almost always has training rows too, but not guaranteed. Check before Task 3. Set `coldStartStrategy="drop"` either way, so unseen users don't produce NaN.
7. **Already-heard artists.** `recommendForUserSubset` doesn't exclude artists the user played in training (confirmed in source). So the top 10 may be artists the user already knows. That's a design decision for you, and it changes Recall@K:

   | Option | What it measures | Cost |
   | --- | --- | --- |
   | Keep them | Whether the model ranks the user's known artists highly | Can inflate recall when train and test share artists |
   | Exclude training artists | Whether the model finds *new* artists the user went on to play | More code: request more than 10, filter, then take 10 |

   Either is defensible; say which you chose in Task 4 or 8.
8. **Define "relevant" for Recall@K**: e.g., the artists the user played in the evaluation set. Pick K = 10 to match Task 3 unless you have a reason not to, and say so.
9. **`RankingEvaluator` needs `array<double>`** for both columns. Artist ids are integers inside structs in the recommendation output, so you'd extract and cast. Computing Recall@K with Python sets is fine for one user; for the whole validation and test sets, the evaluator (or a DataFrame join) avoids collecting millions of rows to the driver.
10. **Tuning cost.** Each grid point is a full ALS fit plus recommendations for every validation user. A 3 × 3 × 3 grid is 27 fits. Start with a small grid, time one fit, then decide how many values to try. `ParamGridBuilder` can generate the combinations, but `CrossValidator` evaluates `model.transform()` output, which doesn't fit a ranking metric well, so a manual loop over the grid is the simpler fit here.
11. **"Retrain on train + validation"** means `train.union(validation)` and a fresh `fit()`; the validated model isn't updated in place.
12. **`alpha = 0.01` is a very low confidence weight** (see the table in Part 1: 100 plays → confidence 2). It's fixed by the lab, but it's relevant context for Task 8.
13. **Local-mode memory.** In local mode, the driver *is* the executor, so it's `spark.driver.memory` that limits you, and it must be set when the session is created. **(General knowledge)** The default is small (1 GB) relative to your 47 GB.

**Heads up about the template:** the lab's header and README say **Audioscrobbler**; the notebook's filename and agenda title say "Autoscrobbler." Same dataset.

---

## TL;DR: what to know for Quiz 6 and Lab 5

**Recommenders**

- **Three tables:** users, items, interactions. Collaborative filtering only needs interactions.
- **Explicit** = ratings (hard to collect, can mismatch behavior). **Implicit** = activity (easy, often better). The lab is implicit.
- **Cosine similarity** = A·B / (‖A‖‖B‖). It measures direction, not magnitude: (3, 0, 4) and (6, 0, 8) have cosine 1. All-pairs comparison scales poorly.
- **ALS:** R (m × n) ≈ U Vᵀ, with U m × k and V n × k (k = `rank`). Predict R̂ᵢⱼ = Uᵢ · Vⱼ. Fix V and solve each user by ridge regression; fix U and solve each item; repeat. Each row solves independently, so it parallelizes. Spark scales λ by each user's or item's count (**ALS-WR**).
- **Implicit ALS:** preference p = 1 if played; confidence c = 1 + α·r. Every cell counts. Predictions are preference scores, not play counts. `implicitPrefs` defaults to **False**.
- **Cold start:** new user = missing row; new item = missing column. Strategies: global averages, content features, hybrids. Spark's `coldStartStrategy` is `"nan"` (default) or `"drop"`.
- **Recall@K** = relevant in top K / all relevant. **Precision@K** = relevant in top K / K. Be able to do the notebook's examples (9/15, 9/100, 9/10).
- **Exploration** (try less-known items) vs. **exploitation** (recommend what's known to work); Personalize's `explorationWeight` defaults to 0.3.

**Feature utilities**

- **Row vs. column:** `Normalizer` makes each **row** unit length; `StandardScaler` and `MaxAbsScaler` scale each **column**.
- **StandardScaler:** `withStd=True`, `withMean=False` by default. Centering destroys sparsity.
- **MaxAbsScaler:** divide by the column's max |x|, into [−1, 1]. No centering, so **sparsity is preserved**. Sensitive to one outlier.
- **StringIndexer → OneHotEncoder:** most frequent label → 0; one-hot with **`dropLast=True`**, so k categories → k − 1 columns, and the last index is the all-zeros reference. OHE is an **Estimator**.
- **Binarizer:** > threshold → 1, ≤ threshold → 0. **Bucketizer:** n + 1 splits → n buckets, each [x, y), last includes its top; include ±∞ to avoid errors.
- **Imputer:** column mean or median, learned in `fit()`. **VectorAssembler:** numeric/boolean/vector columns → one vector. **ChiSqSelector:** chi-squared test, categorical features; deprecated for `UnivariateFeatureSelector`.

**Pipelines**

- **Transformer** = `.transform()`; **Estimator** = `.fit()` → Transformer; **Pipeline** = ordered stages; `pipeline.fit()` returns a **PipelineModel** (a Transformer).
- `fit()` transforms through Transformer stages and fits each Estimator, then uses its fitted model for the next stage.
- **Why:** reproducible workflows, no accidental test leakage (every learned statistic comes from training data), save/load for scoring (inference).
- `model.write().overwrite().save(path)`, `PipelineModel.load(path)`. Saves a directory.
- **HashingTF** (no vocabulary, collisions possible) vs. **CountVectorizer** (learns a vocabulary, an Estimator).

**The pattern behind it all:** ALS is the Module 4–5 pattern again. Heavy per-row work (one ridge regression per user or item) runs on the partitions; only small factor vectors move between them.

**Lab 5 checklist**

- Decide where to run (copy the file locally vs. Rivanna); check the delimiter.
- Sample 0.2 with seed 314 → keep users with ≥ 10 **distinct** artists → cache → `randomSplit([0.6, 0.2, 0.2], seed=314)`.
- Record Test User 1's id once.
- Model 1: `implicitPrefs=True`, rank 8, maxIter 5, regParam 0.02, alpha 0.01, `coldStartStrategy="drop"`.
- Decide and state: K, what counts as relevant, and whether to exclude already-heard artists.
- Tune on validation, retrain on train ∪ validation, report test Recall@K.
- Task 8: compare Test User 1's recommendations with their actual listening (artist names help here; watch the malformed lines).

**Know the notebooks' errors, in case a question uses their wording:**

- The ALS objective image omits Spark's per-user/per-item λ scaling.
- `implicitPrefs` is missing from the hyperparameter list.
- "Each preprocessing step is a Transformer": `OneHotEncoder` is an Estimator.
- The Normalizer is described as standardizing features; it scales rows.
- The reading link opens a different article.
