# DS 7200 Study Guide — Module 4

Sep 28, 2026 · @Sabine

## How to use this guide

Everything here comes from your repo's `04_mllib_intro_and_supervised_learning` folder: the three lecture notebooks (sparsity, regression, classification), the regression-with-caching notebook, the MLlib method summary slides, and the lab. It follows the same conventions as the Modules 1–3 guide. Where I added an explanation or example that isn't in the course material, it's marked **(my addition)**. Where the source material itself has a problem, it's marked **Heads up**.

The assigned readings (*Learning PySpark* Ch. 5 and *Learning Spark* Ch. 10) aren't in the repo, so this guide doesn't summarize them.

## Module 4: MLlib Intro & Supervised Learning

Module 4 moves from data handling to machine learning. It starts with sparse vectors and matrices, which Spark uses heavily to cut processing and storage, then introduces Spark's MLlib package and uses it for regression and classification.

**Learning outcomes.** By the end you should be able to:

- Apply the basics of the MLlib library in PySpark.
- Implement classification models in MLlib.
- Implement linear regression models in MLlib.
- Understand the benefits of sparse vectors and matrices.
- Progress toward an end-to-end predictive modeling project on a large dataset.

**MLlib has two interfaces.** The module intro stresses this point:

- **RDD API** (`pyspark.mllib`): the original interface. Still supported, but no longer under active development.
- **DataFrame API** (`pyspark.ml`): the newer interface, and the one being actively developed.

**What's due.** Per agendas 08–09, Lab 3 (Supervised Learning) and Quiz 4 (Sparsity, MLlib Classification and Regression) are due Friday, Oct 2 at 11:59pm ET. The quiz title is a good guide to what matters most here.

**Slides.** Agenda 08 lists *MLlib Implementation Details* slides and agenda 09 lists *MLlib Method Summary* slides. In the repo they're a single deck, `mllib_method_summary.pdf`, titled *MLlib Method Summary and Implementation Details*. Slides 2–6 are the method summary and slides 7–11 are the implementation details. Both halves are covered below.

### Sparsity

A **sparse** object contains mostly zeros; the opposite is **dense**. Sparse vectors and matrices assume every element is zero unless stated otherwise, so they only store the nonzero parts. Spark uses them whenever it can, because they save both storage and compute.

**Where sparsity shows up in ML** (the notebook's examples):

- An **interaction matrix**: Amazon customers on rows, products on columns, each element the number of times that customer bought that product.
- A **term-document matrix**: documents on rows, vocabulary words on columns, each element the number of times that word appears in that document.

In both, almost every element is zero.

### Sparse vectors

**Storage.** A Spark sparse vector stores three things: the vector's **length**, the **positions** of the nonzero elements, and their **values**. Positions and values have to stay paired, and there are two ways to write that:

```python
SparseVector(100000, {0: 1.0, 1162: 3.0})        # dict of position: value
SparseVector(100000, [0, 1162], [1.0, 3.0])      # two parallel lists: positions, then values
```

Both mean: a length-100,000 vector with 1.0 at position 0 and 3.0 at position 1162, and zeros everywhere else.

**Compute.** Methods that know about sparsity skip the zeros. For a dot product, you only multiply where **both** vectors are nonzero:

```python
v = SparseVector(100000, [0, 5, 1162], [1.0, 3.0, 6.0])
w = SparseVector(100000, [2, 5, 1162], [4.0, 1.0, 2.0])
```

The only shared nonzero positions are 5 and 1162, so v · w = (3 × 1) + (6 × 2) = **15**. Two multiplications instead of one per position.

**Exercise 1 (with the notebook's answer):** two length-1-million vectors, one nonzero only in the first position, the other only in the last. Their dot product is **0**, because the nonzero positions never overlap.

**Heads up:** the notebook's intro talks about vectors of length 1 million and says a dense dot product would take "1 million multiplications and 999,999 additions," but the code example right above it defines vectors of length 100,000. The point is the same either way: a dense product costs one multiply per position.

### Sparse matrices and CSC format

Spark's `SparseMatrix` is stored in **Compressed Sparse Column (CSC)** format, which is column-oriented. Benefits: efficient storage, efficient **column slicing**, and fast matrix–vector products.

CSC uses three arrays. With NNZ = the number of nonzero elements:

- **`val`**: every nonzero value, read **column by column**, top to bottom (length NNZ).
- **`row_ind`**: the row each of those values sits in (length NNZ).
- **`col_ptr`**: for each column, the position in `val` where that column **starts**. **(My addition)** It has one extra final entry marking where the last column ends, so it has (number of columns + 1) entries. That's how you know where each column stops.

**The worked example** (the notebook's image, from netlib):

```text
      c1  c2  c3  c4  c5  c6
r1  [ 10   0   0   0  -2   0 ]
r2  [  3   9   0   0   0   3 ]
r3  [  0   7   8   7   0   0 ]
r4  [  3   0   8   7   5   0 ]
r5  [  0   8   0   9   9  13 ]
r6  [  0   4   0   0   2  -1 ]
```

**(My addition)** Walking it column by column (1-based, as in the image):

| Column | Nonzeros (value @ row) | Starts at `val` position |
| --- | --- | --- |
| 1 | 10 @ 1, 3 @ 2, 3 @ 4 | 1 |
| 2 | 9 @ 2, 7 @ 3, 8 @ 5, 4 @ 6 | 4 |
| 3 | 8 @ 3, 8 @ 4 | 8 |
| 4 | 7 @ 3, 7 @ 4, 9 @ 5 | 10 |
| 5 | −2 @ 1, 5 @ 4, 9 @ 5, 2 @ 6 | 13 |
| 6 | 3 @ 2, 13 @ 5, −1 @ 6 | 17 |

So `col_ptr = [1, 4, 8, 10, 13, 17, 20]`. The final 20 is NNZ + 1: there are 19 nonzeros. Storing 19 values + 19 row indices + 7 pointers = 45 numbers, versus 36 for the dense 6 × 6 matrix. **(My addition)** For a matrix this small and this full (19 of 36 elements nonzero), CSC is actually *bigger* than dense. It pays off when the matrix is mostly zeros, which is the case for the interaction and term-document matrices above.

**Heads up:** the image uses **1-based** indexing (first row is 1). Spark is **0-based**. **(My addition)** In Spark, this matrix would be `SparseMatrix(6, 6, colPtrs=[0, 3, 7, 9, 12, 16, 19], rowIndices=[0, 1, 3, 1, 2, 4, 5, ...], values=[10, 3, 3, 9, 7, 8, 4, ...])`: every index is one lower.

**Exercise 2:** name another way to store a sparse matrix and compare it with CSC. The notebook's answer is just "CSR format." **(My addition)** **CSR (Compressed Sparse Row)** is the mirror image: values are read row by row, you store each value's *column*, and a `row_ptr` marks where each row starts. CSR is fast for slicing rows; CSC is fast for slicing columns. Pick the one that matches how you'll access the data. A third option is **COO (coordinate)** format: a plain list of (row, column, value) triples. It's the simplest to build, but it's larger and slower for arithmetic.

### Machine learning in Spark: the MLlib basics

MLlib is Spark's machine learning library. This section pulls together the general material from the classification notebook's opening and the *MLlib Method Summary and Implementation Details* slides.

**Supervised vs. unsupervised.** In **supervised learning**, each observation has a **label** (the ground truth, the correct answer). You train on labeled examples, then predict labels for new ones. **Unsupervised learning** has no label; clustering methods like k-means are the example. The notebook points out that most data in the wild has no label.

**The two APIs, in more detail:**

- **RDD API** (`pyspark.mllib`): older, maintained but not growing. For supervised tasks it bundles each label with its predictors in a **`LabeledPoint`** object: a label plus a feature vector, e.g. `LabeledPoint(1.0, [0.0, 2.52, 0.0, ...])`. Unsupervised tasks have no label, so they don't use `LabeledPoint`.
- **DataFrame API** (`pyspark.ml`): newer, actively developed, and the more common approach. There's no `LabeledPoint`. Instead you pack all the predictor columns into **one vector column**, then tell the model that column's name and the label column's name.
- Some functionality exists only in the RDD API, so the course covers both.

### fit, transform, evaluate

The slides' main point: the DataFrame API makes it easy to experiment across models because every model follows the same three steps.

1. **`fit()`** trains a model. Import the model from the `ml` library, instantiate it with the features column name, the label column name, and hyperparameters, then call `fit()` on a DataFrame.
2. **`transform()`** makes predictions. It returns the DataFrame with a `prediction` column appended.
3. **`evaluate()`** assesses predictive performance. Import an evaluator, give it the prediction and label column names, and call `evaluate()`.

The methods are analogous across tasks. Regression and classification call different libraries and functions, but the ideas are the same. The slides show them side by side:

```python
# Regression                                      # Classification
from pyspark.ml.regression import LinearRegression  from pyspark.ml.classification import LogisticRegression
lr = LinearRegression(featuresCol='features',       lr = LogisticRegression(labelCol='high_price',
                      labelCol='median_house_value',                        featuresCol='scaledFeatures',
                      maxIter=10, regParam=0.3,                             maxIter=10, regParam=0.3,
                      elasticNetParam=0.8)                                  elasticNetParam=0.8)
lrModel = lr.fit(tr)                                lrModel = lr.fit(scaledData)
```

**(My addition)** Spark's own vocabulary for this: anything with a `transform()` method is a **Transformer** (it maps one DataFrame to another), and anything with a `fit()` method is an **Estimator** (it learns from data and *returns* a Transformer). So `LogisticRegression` is an Estimator, and the `lrModel` that `fit()` returns is a Transformer. That's why you predict with `transform()`, not a `predict()` method. The same holds for `StandardScaler` (below): `fit()` returns a `StandardScalerModel`, and you call `transform()` on that.

### Two transformations to know right away

The notebook's example uses `sample_housing_data.csv`, whose target is `high_price` (0 or 1).

**`VectorAssembler`** packages DataFrame predictor columns into a single vector column. `inputCols` takes a list of column names; `outputCol` is any name you like, usually `features`.

```python
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=["median_income", "total_rooms"], outputCol="features")
tr = assembler.transform(training)   # adds features = [8.5552, 880.0], ...
```

It's a plain Transformer: there's nothing to learn, so there's no `fit()`.

**`StandardScaler`** scales the features. Apply it after `VectorAssembler`, because it works on the vector column.

```python
from pyspark.ml.feature import StandardScaler
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
scalerModel = scaler.fit(tr)          # compute each feature's statistics
scaledData = scalerModel.transform(tr) # apply them to every row
```

The first row's `[8.5552, 880.0]` becomes `[4.544, 0.364]`.

**Heads up:** the notebook's comment says `fit()` computes "mean, standard deviation of each feature," which suggests the data is centered. **(My addition)** By default it isn't: `StandardScaler` has `withMean=False` and `withStd=True`, so it only **divides by the standard deviation**. I checked this against your output. The sample standard deviation of the six `median_income` values is about 1.8826, and 8.5552 ÷ 1.8826 = 4.544, exactly the scaled value. Pass `withMean=True` if you want true z-scores.

### How the statistics get computed across partitions

The rows are partitioned, so no single machine sees a whole column. Each executor computes **partial (local) statistics** for each column on its own partition. Those partial results are then **aggregated** into the global statistics (means μ₁, μ₂, … and standard deviations σ₁, σ₂, …).

```text
Partition 1      Partition 2      Partition 3
rows 1-3         rows 4-6         rows 7-9
    |                |                |
    +------ local statistics ---------+
                     |
              global statistics
```

**(My addition)** This is the combiner pattern from MapReduce again. A mean can't be averaged from per-partition means, so each partition actually sends back a count, a sum, and a sum of squares. Those *can* be added up, and the global mean and standard deviation are computed from the totals.

**Reading the plan.** `scaledData.explain('formatted')` shows just three steps: `Scan csv` → `Project` (adds `features`) → `Project` (adds `scaledFeatures`). Both ML transformations appear as **user-defined functions**: `UDF(struct(median_income, ..., total_rooms, ...)) AS features#134` and `UDF(features#134) AS scaledFeatures#201`. (The notebook's text quotes `#65/#132`; the IDs change from run to run, as in Module 3.)

**(My addition)** There's no `Exchange` (shuffle) in the plan. The statistics were already computed as a separate job when you called `fit()`, and the fitted scaler just stores them. Applying them is a per-row operation, so `transform()` is narrow.

### Implementation details: how MLlib distributes the work

The second half of the slides (from Reza Zadeh's Strata 2015 talk) explains what happens under the hood.

**Matrix operations are foundational.** There are three ways to distribute a matrix across machines:

- **By entries:** `CoordinateMatrix`. **(My addition)** Each element is stored as a (row, column, value) triple, the COO format from the sparsity section.
- **By rows:** `RowMatrix`. Each row is a vector, and rows are spread across partitions.
- **By blocks:** `BlockMatrix`, added in Spark 1.3. The matrix is cut into sub-matrix tiles.

**MLlib data handling.** Data is stored in distributed collections partitioned across the cluster's nodes. Training algorithms are written to **aggregate statistics from partitions** and compute parameters efficiently from those. Two examples:

- **Linear models** use **distributed gradient descent**. Each worker computes **partial gradients** on its partition, and the partial gradients are aggregated.
- **Tree-based models** compute feature statistics (for example, **node impurity**) in a distributed way.

**Logistic regression as the worked example.** The update rule is:

```latex
w \leftarrow w - \alpha \cdot \sum_{i=1}^{n} g(w; x_i, y_i)
```

w is the weight vector, α is the step size (learning rate), and g is the gradient contributed by one data point. The slide's Scala code:

```scala
val points = spark.textFile(...).map(parsePoint).cache()   // cache: every iteration reuses the data
var w = Vector.zeros(d)
for (i <- 1 to numIterations) {
  val gradient = points.map { p =>
    (1 / (1 + exp(-p.y * w.dot(p.x))) - 1) * p.y * p.x     // each point's gradient, computed on its partition
  }.reduce(_ + _)                                           // aggregate partial gradients across partitions
  w -= alpha * gradient                                     // update the weights on the driver
}
```

The two annotations on the slide are the lesson:

- **`cache()`**: every iteration reads the whole training set again. Without caching, Spark would rebuild `points` from the text file on every loop (Module 2's lineage replay). Caching it in memory is a big speedup.
- **`reduce(_ + _)`**: this is where the partial gradients from each partition are summed into one gradient.

**(My addition)** The sum over i splits cleanly across partitions: the total gradient is just the sum of each partition's sum. That's why gradient descent parallelizes so well. Only the small gradient vector travels over the network each iteration, never the data. The caching experiment later in this module tests the `cache()` claim directly.

**Your class note (9/21):** "When we see things that combine gradients we can consider if we can parallelize." That's the general lesson of this slide: any algorithm whose update is a *sum* of per-row pieces can be split across partitions and recombined with a `reduce`.

### Classification

Classification is a common form of supervised learning. What makes a problem classification is the type of the Y variable: it's **discrete**. **Binary classification** is the most common kind: fraud or not, default, survival, claim filing, spam. A **continuous** Y makes it a regression problem instead (next section).

**Label convention** (both APIs):

- Binary classification: labels **0** and **1**.
- Multiclass classification: labels **0, 1, …, C − 1**, where C is the number of classes.

**Models Spark supports for classification:** logistic regression, Naive Bayes, tree methods (decision tree, random forest), and support vector machines.

### Logistic regression

Logistic regression is currently the most popular method for binary classification. It's a **generalized linear model** that uses a linear plane to separate positive and negative examples. The model is relatively simple, but its results can be very competitive. The notebook's picture: probability of passing (Y, which is 0 or 1) as an S-shaped curve against hours studied per week (X). The curve rises from near 0 to near 1 between about 4 and 8 hours.

**Multiclass.** The notebook says the algorithm outputs a multinomial model made of **K − 1 binary logistic regression models**, each regressed against the first class. For a new point, all K − 1 models run and the class with the largest probability wins.

**Heads up:** that description comes from the older RDD API's documentation. **(My addition)** In the DataFrame API, `LogisticRegression(family="multinomial")` fits one softmax model with a set of coefficients for every class, rather than K − 1 models against a reference class. I'm fairly confident of this, but check the current Spark docs if it comes up on the quiz.

**With the RDD API** (data: `sample_svm_data.txt`, 322 lines; each line is a label followed by 16 space-separated feature values):

```python
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.regression import LabeledPoint

def parsePoint(line):
    values = [float(x) for x in line.split(' ')]
    return LabeledPoint(values[0], values[1:])   # first value is the label, the rest are features

parsedData = sc.textFile('sample_svm_data.txt').map(parsePoint)
model = LogisticRegressionWithLBFGS.train(parsedData)
labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
# [(1.0, 1), (0.0, 1), (0.0, 0)]  -- (true label, prediction); the second one is wrong
```

**Heads up:** the comment above `train()` says "using the stochastic gradient descent optimizer," but the model is `LogisticRegressionWithLBFGS`. **(My addition)** L-BFGS is a different optimizer (a quasi-Newton method). The comment is left over from the older `LogisticRegressionWithSGD`. Also, these predictions are made on the **training** data, so they say nothing about how the model does on new data.

**With the DataFrame API** (the more common approach, using `scaledData` from the MLlib basics section):

```python
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(labelCol='high_price', featuresCol='scaledFeatures',
                        maxIter=10, regParam=0.3, elasticNetParam=0.8)
lrModel = lr.fit(scaledData)
lrModel.coefficients   # [0.0703, 0.0]
lrModel.intercept      # -0.9547
```

**(My addition)** The second coefficient (`total_rooms`) is exactly **0.0**. That's the lasso part of the elastic net penalty at work (`elasticNetParam=0.8` is mostly L1, explained in the regression section): L1 can push coefficients all the way to zero, which is built-in feature selection.

**The loss curve.** `lrModel.summary` holds training info. `summary.totalIterations` is 5, and `summary.objectiveHistory` holds the loss at each step, falling from 0.63651 to 0.63591 and then flattening. Plotting it shows whether training has **converged**.

**(My addition)** The history has 6 values but only 5 iterations, because the first value is the loss at the starting point, before any iteration. The optimizer stopped before `maxIter=10` because the loss had stopped changing.

**Measuring the fit:**

```python
from pyspark.ml.evaluation import BinaryClassificationEvaluator
lrPred = lrModel.transform(scaledData)          # appends rawPrediction, probability, prediction
evaluator = BinaryClassificationEvaluator(rawPredictionCol="prediction",
                                          labelCol="high_price", metricName="areaUnderPR")
evaluator.evaluate(lrPred)                       # 0.3333
```

`transform()` adds a `probability` column holding \[P(class 0), P(class 1)\] for each row. Every row in the output has P(class 0) around 0.65–0.68, so every `prediction` is **0.0**.

**Heads up: this example doesn't tell you anything about the model, for three reasons.** **(My addition)**

1. **The dataset has 6 rows.** `sample_housing_data.csv` in the classification folder holds just 6 records, 2 with `high_price = 1`. It's a syntax demo, not a real fit.
2. **It's evaluated on the training data.** There's no train/test split.
3. **The evaluator is given hard predictions.** `rawPredictionCol="prediction"` passes the 0/1 predictions, not scores. Area under a PR or ROC curve is built by sweeping a threshold across *scores*. Since every prediction is 0, all rows tie, and the area collapses to the fraction of positives: 2 ÷ 6 = **0.333**. That's exactly the number printed. Leave `rawPredictionCol` at its default (`"rawPrediction"`) to get a real curve.

**Try-it exercise:** change the `VectorAssembler` input columns, refit, and print the **area under the ROC curve**. **(My addition)** Use `metricName="areaUnderROC"` (that's also the default) with the default `rawPredictionCol`. The notebook's exercise cell is empty in your repo.

### Naive Bayes

Naive Bayes (NB) is a relatively simple model, but it can perform quite well, which made it popular. It does **multiclass** classification and is common in **text classification**, where the input features are counts.

**The intuition:** the count of a word on a page shifts the probability of which class the page belongs to. The word "tacos" makes **restaurant** more likely than **florist**.

**How it works:** it computes the probability distribution of each feature *given* a label, then applies **Bayes' theorem** to flip that into the probability of each label given an observation.

**Why "naive":** it assumes every pair of features is **independent**. That assumption simplifies the model enormously and is often reasonable enough.

**In Spark:** the model type is set with an optional parameter: `"multinomial"` (the default), `"complement"`, `"bernoulli"`, or `"gaussian"`. For document classification, the feature vectors should usually be **sparse** vectors.

```python
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

data = spark.read.format("libsvm").load("./sample_libsvm_data.txt")
train, test = data.randomSplit([0.6, 0.4], 314)           # 60/40 split, seed 314
nb = NaiveBayes(labelCol='label', featuresCol='features', smoothing=1.0, modelType="multinomial")
model = nb.fit(train)
predictions = model.transform(test)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")
evaluator.evaluate(predictions)                             # 0.9722
```

**Test-set accuracy: 0.972.** Unlike the logistic example, this one is evaluated on held-out data.

**Reading the output.** The `rawPrediction` values are huge negative numbers (about −225,289 for the first row), and every `probability` is exactly `[1.0, 0.0]` or `[0.0, 1.0]`. One misclassification is visible in the first 20 rows: a row with label 1.0 predicted as 0.0. **(My addition)** For Naive Bayes, `rawPrediction` holds each class's **log-probability** score. With 692 pixel-count features in the hundreds, those scores are enormous and far apart. Converted back to probabilities, the losing class's probability rounds to zero, so the model always looks 100% certain even when it's wrong.

**(My addition)** Two connections worth noticing:

- **libsvm is a sparse format.** Each line of `sample_libsvm_data.txt` is a label followed by `index:value` pairs for only the nonzero features, like `0 128:51 129:159 130:253 ...`. Spark reads that straight into sparse vectors, which is why the `features` column prints as `(692,[121,122,123,...],[...])`: the length, the nonzero positions, then the values. It's the sparsity section in practice. (The file's indices start at 1; Spark shifts them to start at 0.)
- **`smoothing=1.0`** is Laplace smoothing. It adds 1 to every count, so a word that never appeared with a class in training doesn't force that class's probability to zero.

The warning `'numFeatures' option not specified` means Spark had to scan the whole file once just to find the largest feature index. On a big file you'd pass `numFeatures` to skip that extra pass, the same argument as giving a schema instead of inferring one in Module 3.

### Tree methods

Tree methods work for both classification and regression. The simplest is the **decision tree**, which is intuitively appealing because it's a series of binary decisions (male/female, age over 30 or not). The notebook's list of properties:

- Can handle **missing values** (in many implementations), **categorical** data, and **continuous** data. Minimal preprocessing.
- **Feature selection is part of the algorithm:** the best feature is used first, then the next best, and so on.
- **No scaling required.**
- Handles **non-linear interactions**.
- Handles **multiclass** classification.

Code examples (like the random forest classifier) are in the Spark docs: [ml-classification-regression](https://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-classifier).

**(My addition)** Why no scaling: a tree only asks "is this feature above or below some threshold?" Dividing a feature by its standard deviation moves the threshold but doesn't change which rows fall on which side. Logistic regression, in contrast, is sensitive to scale, especially with a regularization penalty, which is why the logistic example scaled first.

### Regression

Regression is supervised learning where the response variable is **quantitative (continuous)**, in contrast to classification's discrete Y.

Several classification models have **regression counterparts**, including support vector machines and tree-based methods like random forests and gradient-boosted trees. To use the regression version, you load the same package but call a different method.

**Heads up:** **(my addition)** as far as I know, Spark's DataFrame API has **no support vector machine for regression**. `pyspark.ml.classification` has `LinearSVC` (an SVM classifier), but `pyspark.ml.regression` has no SVM counterpart. The notebook's own list of "other regression models" (below) doesn't include one either. Tree methods are the counterpart that actually exists in both.

The notebook highlights that Spark's distributed processing is **particularly helpful for random forests**: the trees can be built on different nodes and their results aggregated. **(My addition)** Each tree in a forest is independent of the others, so this is naturally parallel work.

### Linear regression

Linear regression is the most fundamental regression model. It assumes a **linear relationship** between a set of explanatory variables X (also called features, factors, predictors, or independent variables) and a scalar response Y. It's most often fit with **ordinary least squares (OLS)**.

**The example** (data: `sample_housing_data.csv` in the regression folder; target `median_house_value`):

```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

assembler = VectorAssembler(inputCols=["median_income", "total_rooms"], outputCol="features")
tr = assembler.transform(training)

lr = LinearRegression(featuresCol='features', labelCol='median_house_value',
                      maxIter=10, regParam=0.3, elasticNetParam=0.8)
lrModel = lr.fit(tr)
lrModel.coefficients   # [19866.98, -9.836]
lrModel.intercept      # 261023.87

lrPred = lrModel.transform(tr)                    # first row: actual 452,600, predicted 417,765
ev = RegressionEvaluator(predictionCol="prediction", labelCol="median_house_value")
ev.evaluate(lrPred, {ev.metricName: "mse"})       # 703,796,169
ev.evaluate(lrPred, {ev.metricName: "r2"})        # 0.603
```

**How to get a metric:** pass the evaluator a DataFrame that has labels and predictions, plus a dictionary choosing the metric by key: `{ev.metricName: "mse"}`. **(My addition)** The other `RegressionEvaluator` metrics are `"rmse"` (the default), `"mae"`, `"r2"`, and `"var"`. RMSE is the square root of MSE and is in the target's units. Here, √703,796,169 ≈ **$26,529**, which is far easier to read than the MSE.

**Heads up:** as with the classification example, this dataset is tiny. The regression folder's `sample_housing_data.csv` has **5 rows**, and the model is evaluated on the same 5 rows it was trained on. The R² of 0.60 shows the syntax, not a real result.

### Regularization: ridge, lasso, elastic net

A **regularization** term is often added to the loss function to help the model **generalize** to new data by reducing **overfitting**. That's what `regParam` and `elasticNetParam` control.

- **Ridge regression:** an L2-norm penalty (sum of squared coefficients).
- **Lasso:** an L1-norm penalty (sum of absolute coefficients).
- **Elastic net:** a blend of ridge and lasso.

The regularization term Spark uses:

```latex
\lambda \left[ \frac{1}{2}(1-\alpha)\,\|\beta\|_2^2 \;+\; \alpha\,\|\beta\|_1 \right]
```

| Symbol | Spark parameter | What it does |
| --- | --- | --- |
| λ (lambda) | `regParam` | How much the penalty counts against prediction error. 0 means no regularization. |
| α (alpha) | `elasticNetParam` | The mix: 0 = pure ridge (L2), 1 = pure lasso (L1), in between = elastic net. |
| β (beta) | (the coefficients) | What gets penalized. |

**(My addition)** Reading the formula: when α = 0 the L1 term vanishes, leaving only the ridge (L2) term. When α = 1 the L2 term vanishes, leaving only lasso. The notebook's `elasticNetParam=0.8` is 80% lasso, 20% ridge. The practical difference: **lasso can shrink coefficients to exactly zero** (built-in feature selection, which is what zeroed out `total_rooms` in the logistic example), while **ridge shrinks them toward zero but never all the way**.

**Try-it exercise 1** (the cells are empty in your repo). **(My addition)** Only the two parameters change:

```python
lasso = LinearRegression(featuresCol='features', labelCol='median_house_value',
                         maxIter=10, regParam=0.3, elasticNetParam=1.0)   # alpha = 1: L1 only
ridge = LinearRegression(featuresCol='features', labelCol='median_house_value',
                         maxIter=10, regParam=0.3, elasticNetParam=0.0)   # alpha = 0: L2 only
```

`regParam` has to be above 0 for either penalty to do anything. With `regParam=0`, `elasticNetParam` is ignored and you get plain OLS.

**(My addition)** Why the regression example didn't scale its features even though a penalty is involved: Spark's `LinearRegression` and `LogisticRegression` have a `standardization` parameter that defaults to `True`. They standardize features internally before applying the penalty, then report coefficients on the original scale. So the penalty treats `median_income` and `total_rooms` fairly even though their scales differ by a factor of about 100.

### Other regression models in the DataFrame API

- Generalized linear regression
- **Decision tree regression.** Its implementation partitions data by rows, which allows distributed training on millions or even billions of instances.
- Random forest regression
- Gradient-boosted tree regression

Code examples are in the [Spark docs](https://spark.apache.org/docs/latest/ml-classification-regression.html).

### Try-it exercise 2: which task is which?

The notebook asks for a real-world example of each task type and points to [scikit-learn's page](https://scikit-learn.org/stable/modules/multiclass.html) on multiclass vs. multilabel. **(My addition)** The examples are mine:

| Task | What Y looks like | Example |
| --- | --- | --- |
| Regression | A continuous number | Predicting a home's sale price |
| Binary classification | One of two classes | Is this transaction fraud? |
| Multiclass classification | Exactly one of C > 2 classes | Which of 10 digits is in this image? |
| Multilabel classification | Any subset of several labels | Which topics (politics, sports, economy) does this article cover? It can be several at once. |

The key distinction: in **multiclass**, each observation gets exactly **one** class. In **multilabel**, each observation can get **several** labels (or none), so it's really a set of yes/no questions asked together.

### The caching experiment

This in-class notebook (`mllib_regression_with_caching.ipynb`, from agenda 09) builds a data pipeline, fits a linear regression, and times the same training loop **with and without caching** the training data. It tests the slides' claim that iterative ML benefits from `cache()`. Your completed copy has your notes throughout; this section follows them.

**Setup.** The data is `california_housing_spark_10000.csv`, 10,000 records with target `median_house_value` and 8 features (median income, housing median age, total rooms, total bedrooms, population, households, latitude, longitude). You ran it locally with the CSV copied from Rivanna.

### Pipelines

This notebook introduces **`Pipeline`**, which bundles preprocessing steps into one object you can fit and reuse. It runs its stages in order, and it saves you from calling `fit()` on each step separately. Your note compares it to tidymodels' `workflow()`.

```python
from pyspark.ml import Pipeline
assembler = VectorAssembler(inputCols=features, outputCol="raw_features")
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=False)
pipeline = Pipeline(stages=[assembler, scaler])

pipeline_model = pipeline.fit(df)
processed = pipeline_model.transform(df).select("features", "median_house_value")
```

**Your design note on column names** is worth remembering. DataFrames are immutable, so a transformer can never overwrite its input column: `outputCol` must always be a new name. Naming the assembler's output `raw_features` leaves the name `features` free for the **scaled** vector. Everything downstream then uses `features` and gets scaled data. If the assembler had taken `features`, the scaled output would need another name, and any code still pointing at `features` would silently train on unscaled data.

**(My addition)** `Pipeline` is itself an Estimator, and `pipeline.fit()` returns a `PipelineModel`, which is a Transformer. During `fit()`, each stage that needs fitting (here, the scaler) is fitted in turn on the output of the stages before it.

### Train/test split

```python
train, test = processed.randomSplit([0.8, 0.2], seed=42)   # 8,079 train / 1,921 test
```

`randomSplit` takes a list of weights, and there can be more than two. `[.7, .15, .15]` gives train, validation, and test sets in one call. The seed makes the split reproducible.

**(My addition)** The split came out 80.8% / 19.2%, not exactly 80/20. `randomSplit` assigns each row independently at random with those probabilities, so the sizes are only approximately the weights.

### The experiment

The model is `LinearRegression(featuresCol="features", labelCol=LABEL_COLUMN, maxIter=MAX_ITER)` with `MAX_ITER = 20`. With no `regParam`, it defaults to 0 (no regularization), which triggers a warning about numerical instability and overfitting. That's fine here: the point is caching, not model quality.

Each loop runs 5 times: fit the model on `train`, predict on `train`, and compute training RMSE with an action (`collect()`).

**Without cache.** Spark is lazy, so `train` isn't stored data. It's a recipe: read CSV → assemble → scale → split. Every action that touches `train` replays the whole recipe from scratch.

**With cache:**

```python
train_cached = train.cache()
train_cached.count()        # materialize the cache BEFORE starting the timer
# ... same 5-iteration loop on train_cached ...
train_cached.unpersist()    # free the memory when done
```

`cache()` is lazy too: it only marks `train` to be kept after its first computation. The `count()` before the timer is what actually computes and stores it. After that, each iteration only redoes the fit and prediction.

**Your fair-comparison summary:**

- No cache: 5 × (read + assemble + scale + split + fit + predict)
- Cached: 1 × (read + assemble + scale + split) + 5 × (fit + predict)

**Results.** Without cache: **1.89 s**. With cache: **0.89 s**, a **2.11× speedup**. Training RMSE was **57,768.96** in every iteration, both ways. Caching changes *how* the data gets computed, not *what* the data or model is.

The notebook's closing line: **only cache a dataset if you're going to use it again.** **(My addition)** Caching costs memory (on a cluster, executor memory that other work could use), and the first computation is slightly slower because Spark also has to store the result.

**Your note on caching vs. parallelization vs. distributed computing:**

- **Parallelization** is how much work happens *at once*. **Caching** is how many *times* the same work happens. They combine: cached data is still processed in parallel when it's used.
- **Parallel on one machine** (like R's `doParallel`) is bounded by that machine's RAM and CPU and usually evaluates eagerly, so there's no separate caching concept. **Distributed (Spark)** spreads data over machines with no shared memory. Data too big for one machine still fits, lost partitions can be recomputed from lineage, and operations like `groupBy` and joins need network shuffles, which single-machine parallelism doesn't have.

**Heads up: two caveats about the numbers.** **(My addition)**

1. **The timing is a single run on 10,000 rows**, so treat 2.11× as illustrative. The no-cache loop also ran *first*, so it may have absorbed one-time warm-up costs (JVM code compilation, file system caching) that the cached loop didn't pay. Running the cached version first, or repeating both a few times, would make the comparison cleaner.
2. **Your comment on step 7 says Spark's `LinearRegression` uses iterative solvers by default.** As far as I know, that's not quite right. The default `solver="auto"` uses the **normal equation** (a closed-form solve) when it can, which includes this case: 8 features and no L1 penalty. It only falls back to an iterative optimizer (L-BFGS) when it has to, for example with an L1 penalty. With the normal equation, `maxIter` has no effect. That also means each `fit()` here probably makes a single pass over the data, which fits the roughly 2× speedup. I'm fairly confident of this; you can check it with `model.summary.totalIterations`, which I'd expect to be 0 for the normal-equation solver.

### Module 4 lab: supervised learning

The lab (Lab 3 on the agenda, due Oct 2) is worth 10 points in two parts. Your `lab_supervised_learning_sabine.ipynb` is still identical to the blank template, so this section maps each requirement to the module material and flags the traps, without solving it for you.

**Part I, classification (5 points).** Logistic regression on the Wisconsin Breast Cancer dataset (`wisc_breast_cancer_w_fields.csv`: 569 rows; columns `id`, `diagnosis`, and features `f1`–`f30`).

| Requirement | Points | Where it's covered |
| --- | --- | --- |
| Target is `diagnosis`; predictors `f1`, `f2` | 1 | `VectorAssembler` (MLlib basics) |
| Split 60% train / 40% test, `seed=314` | — | `randomSplit` (Naive Bayes example; caching notebook) |
| Standardize the predictors | 1 | `StandardScaler` (MLlib basics) |
| Fit logistic regression **with an intercept** | 1 | Logistic regression (DataFrame API) |
| Area under the ROC curve **on the test set** | 2 | `BinaryClassificationEvaluator` |

**Part II, regression (5 points).** Linear regression on the California Housing data (`cal_housing_data_preproc_w_header.txt`: 20,640 rows, same 9 columns as the caching notebook; no missing values).

| Requirement | Points | Where it's covered |
| --- | --- | --- |
| Divide `median_house_value` by 100,000 | 1 | `withColumn` (Module 3) |
| Split 80% train / 20% test, `seed=314` | 1 | `randomSplit` |
| Add a new predictor, `rooms_per_household` | — | `withColumn` (Module 3) |
| Standardize the 5 features **in the training set** | 1 | `StandardScaler`, or a `Pipeline` |
| Fit with `maxIter=10`, `regParam=0.3`, `elasticNetParam=0.8` | — | Linear regression; regularization table |
| MSE on the **test** set | 2 | `RegressionEvaluator` with `{ev.metricName: "mse"}` |

The five features: `total_bedrooms`, `population`, `households`, `median_income`, `rooms_per_household`.

**Traps to watch for (my addition):**

1. **`diagnosis` is a string**, `"M"` (malignant, 212 rows) or `"B"` (benign, 357 rows). Logistic regression needs a numeric 0/1 label (the label convention from the classification section). You have to convert it before fitting. Decide which class counts as 1 on purpose, since it changes how you read the model. If you use `StringIndexer`, it assigns 0 to the *most frequent* value, so B would become 0 and M would become 1.
2. **Fit the scaler on the training set only**, then use that fitted scaler to transform the test set. If you fit it on all the data, test-set statistics leak into training. Part II's wording ("in the training set") says this outright. The safe order is split, then fit on train, then transform both. A `Pipeline` fitted on `train` handles this cleanly.
3. **AUC needs scores, not hard predictions.** Don't copy the notebook's `rawPredictionCol="prediction"`: on a real dataset that gives a wrong, flattened AUC (see the Heads up in the logistic regression section). Leave it at the default `"rawPrediction"`.
4. **"With an intercept"** is already the default (`fitIntercept=True`), but it's worth setting explicitly since it's graded.
5. **`rooms_per_household`** isn't defined in the lab. The natural definition is `total_rooms / households`. There are no zero-household rows, so there's no divide-by-zero. State your definition in a comment.
6. **Scale the target before splitting**, so the train and test sets use the same units. Your MSE will then be in units of ($100,000)².

**Heads up about the template itself:**

- The first code cell in Part I calls `SparkSession.builder` **before** anything imports `SparkSession`. The import only appears later, in Part II's first cell. Run Part I first in a fresh kernel and you'll get a `NameError`. Add the import at the top.
- The Part II cell has old saved output from March 2023 (a Python 3.7 conda environment, `ps: command not found`). It's left over from the template, not from your run.
- **(My addition)** `median_house_value` tops out at exactly 500,001 in this dataset. The original California Housing data capped home values there, so the most expensive districts all share one value. It won't break anything for this lab, but it's worth knowing if a residual plot looks odd at the top end.

## TL;DR

The one-page version, organized around what Quiz 4 covers: sparsity, MLlib classification, and regression.

**Sparsity**

- Sparse objects store only the nonzeros. A sparse vector stores its length, the nonzero positions, and their values: `SparseVector(100000, [0, 1162], [1.0, 3.0])`.
- A sparse dot product only multiplies where **both** vectors are nonzero. No overlap means the product is 0.
- Spark's `SparseMatrix` uses **CSC** (column-oriented) format with three arrays. `val` holds the nonzeros column by column, `row_ind` holds each value's row, and `col_ptr` marks where each column starts in `val`. It's good for storage, column slicing, and matrix–vector products.
- CSR is the row-oriented mirror image. Spark indexes from 0; the netlib example indexes from 1.

**MLlib basics**

- There are two APIs. The RDD API (`pyspark.mllib`, uses `LabeledPoint`) is maintained but frozen. The DataFrame API (`pyspark.ml`) is the one under active development.
- In the DataFrame API, every model follows the same pattern: **`fit()`** trains, **`transform()`** predicts by appending a `prediction` column, and an evaluator's **`evaluate()`** scores the result.
- **`VectorAssembler`** packs predictor columns into one vector column. **`StandardScaler`** scales that vector, and by default it only divides by the standard deviation; it doesn't center.
- A **`Pipeline`** chains preprocessing stages into one reusable object you can fit.
- Under the hood, each partition computes **partial statistics or partial gradients**, and those get aggregated. That's the combiner idea again.

**Classification (discrete Y; labels 0 to C − 1)**

- **Logistic regression** is the most popular binary classifier: a generalized linear model. Evaluate it with `BinaryClassificationEvaluator` (area under ROC or PR), fed *scores*, not hard predictions.
- **Naive Bayes** is multiclass, a good fit for text or count data, and assumes features are independent. `modelType` defaults to multinomial. Evaluate it with `MulticlassClassificationEvaluator`.
- **Trees** need no scaling. They handle categorical data, missing values, non-linear interactions, and multiclass problems, and they select features as part of the algorithm.

**Regression (continuous Y)**

- Linear regression is usually fit with OLS. Evaluate it with `RegressionEvaluator`: `mse`, `rmse`, `mae`, or `r2`.
- The regularization penalty is λ\[½(1 − α)‖β‖₂² + α‖β‖₁\]. `regParam` = λ and sets the strength. `elasticNetParam` = α and sets the mix: 0 is ridge, 1 is lasso, in between is elastic net.
- Lasso can zero out coefficients (built-in feature selection). Ridge only shrinks them.
- Random forests parallelize especially well because each tree is independent.

**Caching**

- Iterative training rereads the data on every pass, so **cache the training set**. `cache()` is lazy: call an action to materialize it.
- In your experiment, caching gave **1.89 s → 0.89 s (2.11×)** with identical RMSE. Only cache data you'll reuse, and `unpersist()` when you're done.

**For the lab**

- Turn `diagnosis` (M/B) into 0/1 before fitting.
- Split first, then fit the scaler on the training set only.
- Evaluate on the test set, with `seed=314` everywhere.
