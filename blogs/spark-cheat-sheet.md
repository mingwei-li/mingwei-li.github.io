# Spark SQL Query Cheat Sheet - Java Version

## 0. My Frustration

![frustration](../images/spark-cheat-sheet/frustration.jpg)

I'm definitely not an expert in Spark SQL, or in Java, and struggled so much when reading or writing complex Spark SQL codes especially when they are wrapped in Java / Scala DataFrame APIs. Personally, I always prefer Java over Scala because Java provides strong typing and makes syntax or semantic errors way easier to discover before compiling. However, Spark SQL is written natively in Scala and made available in Java through the library, which makes Spark SQL codes in Java look like a freak. I need a cheat sheet for myself, to ultimately list all weird syntax and be free from those "free formatted" confusing Scala API definitions. Hopefully, it could help you in some way as well. Sometimes, I just miss cumbersome yet definitive old school Java code that can explain itself after you read it 10 times end to end.

## 1. Environment

### Runtime environment

Java: 1.8.0_222
Scala: 2.11.8
Spark dependency pom:

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-core_2.11</artifactId>
    <version>2.4.7</version>
</dependency>
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.11</artifactId>
    <version>2.4.7</version>
</dependency>
```

Spark library version from Maven needs to align with the Scala version: they are both **2.11** in my setup.

### Files / data used in this blog

users.csv:

```csv
id,name,age,registered,category,createAt
1,jr smith,30,false,good,2020-09-12 08:09:33
2,john smith,22,true,good,2020-03-12 08:09:33
3,john doe,45,true,bad,1986-09-12 08:09:33
4,han meimei,23,true,good,1999-04-12 08:09:33
5,li lei,59,false,good,1999-09-12 08:09:33
6,the carrot,1,true,bad,2020-11-22 08:09:33
```

user_history.csv:

```csv
id,action,timestamp
1,open,2020-09-12 08:09:33
1,click,2020-09-12 08:29:33
1,close,2020-09-12 08:39:33
2,open,1920-11-09 12:03:34
3,close,2020-05-12 23:39:01
4,click,2020-09-11 08:39:33
4,close,2020-09-12 19:39:33
5,double click,1989-09-12 08:39:33
6,right click,2020-09-12 08:39:33
```

### Initialize Spark in local

```java
SparkSession spark = SparkSession.builder().appName("Blog Cheat Sheet").master("local").getOrCreate();
spark.sparkContext().setLogLevel("ERROR");
```

## II. Creating DataFrames / Dataset

### 2.1 Create from CSV file

#### .csv() call

```java
String csvFile = "src/main/resources/users.csv";
Dataset usersDataSet = spark.read().option("header", "true").csv(csvFile);
```

#### More generic .format() call

```java
Dataset usersDataSet2 = spark.read().format("csv").option("header", "true").load(csvFile);
```

#### And you can do this because of its not so well thought yet free form API design

```java
Dataset usersDataSet3 = spark.read().format("parquet").option("header", "true").csv(csvFile);
```

### 2.2 Create from CSV file with schema definition

Easiest way to construct StructField that I found is to have a constructor call of `StructField("name of column", DataTypes.SupportedType, can be null boolean, Metadata placeholder)`

#### Cascading calls from StructType

```java
StructType schema = new StructType()
    .add(new StructField("id", DataTypes.StringType, false, Metadata.empty()))
    .add(new StructField("name", DataTypes.StringType, true, Metadata.empty()))
    .add(new StructField("age", DataTypes.IntegerType, true, Metadata.empty()))
    .add(new StructField("registered", DataTypes.BooleanType, true, Metadata.empty()))
    .add(new StructField("category", DataTypes.StringType, true, Metadata.empty()))
    .add(new StructField("createdAt", DataTypes.TimestampType, true, Metadata.empty()));
```


#### Pass in an array of StructField to StructType

```java
StructType schema2 = new StructType(new StructField[] {
    new StructField("id", DataTypes.StringType, false, Metadata.empty()),
    new StructField("name", DataTypes.StringType, true, Metadata.empty()),
    new StructField("age", DataTypes.IntegerType, true, Metadata.empty()),
    new StructField("registered", DataTypes.BooleanType, true, Metadata.empty()),
    new StructField("category", DataTypes.StringType, true, Metadata.empty()),
    new StructField("createdAt", DataTypes.TimestampType, true, Metadata.empty()),
    new StructField("extraCol", DataTypes.TimestampType, true, Metadata.empty())
});
```

When loaded with schema2, the `extralCol` will be appended to the generated Dataset with **null** value

#### Load CSV with schema definition

```java
Dataset usersDataSet4 = spark.read().format("csv").option("header", "true").schema(schema).load(csvFile);
```

> If the schema definition doesn't align in order with the header definition, all values of the generated Dataset will have null values.

### 2.3 Create it manually

#### From RowFactory API call

Have a List of rows container ready, and fill it out with rows created by `RowFactory.create(...)` call where `.create()` can accept arbitrary number of parameters with any type of objects.

```java
ArrayList<Row> rows = new ArrayList<Row>();
rows.add(RowFactory.create("1", "hello kitty", 23, false, "good", Timestamp.valueOf("2020-09-12 08:09:33")));
Dataset manualDatSet = spark.createDataFrame(rows, schema);
manualDatSet.show(false);
```

#### From POJO class as row/schema definition

User.java:

```java
public class User implements Serializable {
 
    private String id;
    private String name;
    private int age;
    private boolean registered;
    private String category;
    private Timestamp createdAt;
 
    public User() {}
 
    public User(String _id, String _name, int _age, boolean _registered, String _category, Timestamp _createdAt) {
        this.id = _id;
        this.name = _name;
        this.age = _age;
        this.registered = _registered;
        this.category = _category;
        this.createdAt = _createdAt;
    }
 
    public String getId() {
        return id;
    }
 
    public void setId(String id) {
        this.id = id;
    }
 
    public String getName() {
        return name;
    }
 
    public void setName(String name) {
        this.name = name;
    }
 
    public int getAge() {
        return age;
    }
 
    public void setAge(int age) {
        this.age = age;
    }
 
    public boolean isRegistered() {
        return registered;
    }
 
    public void setRegistered(boolean registered) {
        this.registered = registered;
    }
 
    public String getCategory() {
        return category;
    }
 
    public void setCategory(String category) {
        this.category = category;
    }
 
    public Timestamp getCreatedAt() {
        return createdAt;
    }
 
    public void setCreatedAt(Timestamp createdAt) {
        this.createdAt = createdAt;
    }
}
```

Spark call:

```java
ArrayList<User> users = new ArrayList<User>();
users.add(new User("1", "hello kitty", 23, false, "good", Timestamp.valueOf("2020-09-12 08:09:33")));
Dataset datasetFromPojo = spark.createDataFrame(users, User.class);
```

## III. Select

#### with free formatted string as column

```java
usersDataSet4.select("id", "age").show();
```

#### With Dataset.col() call to be stronger typing

```java
usersDataSet4.select(usersDataSet4.col("id"), usersDataSet4.col("age"));
```

#### With static col() call without dataset handle

```java
import static org.apache.spark.sql.functions.*;
usersDataSet4.select(col("id"), col("age"));
```

#### Select when, otherwise

```java
usersDataSet.select(
     when(col("registered").equalTo(true), "isTrue")
    .when(col("registered").equalTo(false), "isFalse")
    .otherwise("NotPossible")
).show();
```

`when()` call will eventually return a `Column`, so that its return value can be put or used as a regular column object among other API calls.

#### Select with alias, columnRenamed or withColumn

```java
usersDataSet4
    .select(col("id").as("the_id_column"), col("name"), col("category"))  // alias
    .withColumn("first_name", split(col("name"), " ").getItem(0))         // withColumn() call
    .withColumnRenamed("category", "good_bad")                            // withColumnRenamed() call
.show();
```

## IV. Filter, Where

> Filters and where are equivalent in terms of functionality. 

#### Use col() and functions

```java
usersDataSet4.filter(col("id").gt(3)).show();
```

#### With logical operators

```java
usersDataSet4.filter(  col("id").gt(3).and( col("id").lt(5) )  )
```

#### Use literal string expressions

```java
usersDataSet4.filter("id > 3 and id < 5")
```

## V. Join

Before joining, the second dataset needs to be ready:

```java
StructType historySchema = new StructType()
    .add(new StructField("id", DataTypes.StringType, false, Metadata.empty()))
    .add(new StructField("action", DataTypes.StringType, false, Metadata.empty()))
    .add(new StructField("timestamp", DataTypes.TimestampType, false, Metadata.empty()));
 
Dataset historyDataset = spark.read()
    .format("csv")
    .option("header", "true")
    .schema(historySchema).load("src/main/resources/user_history.csv");
```

#### Join with column operator calls

```java
usersDataSet4.join(historyDataset, usersDataSet4.col("id").equalTo(historyDataset.col("id")), "inner").show();
```

#### Join on the same column

The last query can be replaced by this with a simpler syntax:

```java
usersDataSet4.join(historyDataset, "id").show(false);
```

### Join on more complex condition

```java
usersDataSet4.join(historyDataset, usersDataSet4.col("id").equalTo(historyDataset.col("id")).and(usersDataSet4.col("category").equalTo("good"))).show(false);
```

## VI. Aggregate

#### Aggregate on full set without group by

```java
usersDataSet4.agg(sum(col("age").as("sum"))).show(false);
```

#### Group by and do a count

```java
usersDataSet4.groupBy(col("category")).count().show(false);
```

Equivalent SQL query:

```sql
SELECT category, count(*)
FROM usersDataSet4
GROUP BY category
```

#### Group by and do an aggregate operation

```java
usersDataSet4.groupBy(col("category")).agg(max(col("age"))).show(false);
// or:
usersDataSet4.groupBy(col("category")).max("age").show(false);
```

Equivalent SQL query:

```sql
SELECT category, max(age)
FROM usersDataSet4
GROUP BY category
```

#### Group by transformed column

```java
usersDataSet4.groupBy(year(col("createdAt")).as("year")).avg("age").show(false);
```

Equivalent SQL query:

```sql
SELECT year(createdAt) AS year, avg(age)
FROM usersDataSet4
GROUP BY year(createdAt)
```

## VII. Combined real world complex example

```java
usersDataSet4.as("users")
    .join(historyDataset.as("history"), "id")
    .where(col("age").lt(100))
    .groupBy("users.id")
    .agg(max("timestamp").as("max_timestamp"))
    .select(col("users.id"), col("max_timestamp"))
    .orderBy("id")
    .show(false);
```

Equivalent SQL query:

```sql
SELECT users.id, max(timestamp) as max_timestamp
FROM usersDataSet4 users
JOIN historyDataset history ON users.id = history.id
WHERE age < 100
GROUP BY users.id
ORDER BY users.id
```

## VIII. Summary

The key to master flexible yet complex programming models like Spark SQL is to explore all options, browse its official documentations, constant trial and error, and eventually we will learn the idealogy of its API design. 

