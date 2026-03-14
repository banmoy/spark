# Spark Parquet Schema 读取机制详解

## 目录

- [1. 概述](#1-概述)
- [2. 三种 Schema 获取模式](#2-三种-schema-获取模式)
  - [2.1 用户显式指定 Schema](#21-用户显式指定-schema)
  - [2.2 自动推断（默认行为）](#22-自动推断默认行为)
  - [2.3 合并推断](#23-合并推断)
- [3. Schema 推断详解](#3-schema-推断详解)
  - [3.1 Parquet 文件中的 Schema 信息](#31-parquet-文件中的-schema-信息)
  - [3.2 文件选择策略](#32-文件选择策略)
  - [3.3 Schema 提取优先级](#33-schema-提取优先级)
  - [3.4 Parquet 到 Spark 的类型映射](#34-parquet-到-spark-的类型映射)
- [4. Schema 合并详解](#4-schema-合并详解)
  - [4.1 何时触发](#41-何时触发)
  - [4.2 合并流程](#42-合并流程)
  - [4.3 合并算法](#43-合并算法)
  - [4.4 异构 Schema 的处理](#44-异构-schema-的处理)
  - [4.5 合并后的读取行为](#45-合并后的读取行为)
- [5. 常见场景与行为对照表](#5-常见场景与行为对照表)
- [6. 配置参考](#6-配置参考)
- [7. 性能与最佳实践](#7-性能与最佳实践)

---

## 1. 概述

Spark 读取 Parquet 文件时，需要先确定数据的 Schema（列名、列类型、是否可空等）。Schema 的确定方式直接影响读取性能和正确性。

Spark 支持三种方式获取 Parquet 数据的 Schema：

| 方式 | 读取文件数 | 性能 | 适用场景 |
|------|-----------|------|---------|
| 用户指定 `.schema(...)` | 0 | 最快 | Schema 已知，追求最优性能 |
| 自动推断（默认） | 1 | 快 | Schema 稳定不变 |
| 合并推断 `mergeSchema=true` | 全部 | 慢 | Schema 随时间演化 |

## 2. 三种 Schema 获取模式

### 2.1 用户显式指定 Schema

通过 `.schema()` 方法直接提供 Schema，**完全跳过**文件 footer 读取和推断过程。

**Python:**

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
])
df = spark.read.schema(schema).parquet("/path/to/data")
```

**Scala:**

```scala
val schema = new StructType()
  .add("name", StringType)
  .add("age", IntegerType)
val df = spark.read.schema(schema).parquet("/path/to/data")
```

**DDL 字符串形式:**

```python
df = spark.read.schema("name STRING, age INT").parquet("/path/to/data")
```

**行为特点：**
- 不读取任何文件的 footer，性能最优
- `mergeSchema` 选项会被忽略
- 如果指定的列在文件中不存在，读出来的值为 `null`
- 如果文件中存在但未在 Schema 中指定的列，会被忽略（列裁剪）

### 2.2 自动推断（默认行为）

不指定 Schema 时，Spark 从 Parquet 文件的 footer 中读取 Schema 信息。默认只读取**一个**文件，假设所有文件的 Schema 一致。

```python
df = spark.read.parquet("/path/to/data")
```

### 2.3 合并推断

启用 `mergeSchema` 后，Spark 读取**所有**数据文件的 Schema 并合并为一个超集。

```python
df = spark.read.option("mergeSchema", "true").parquet("/path/to/data")
```

## 3. Schema 推断详解

### 3.1 Parquet 文件中的 Schema 信息

每个 Parquet 文件的尾部（footer）包含两套 Schema 信息：

1. **Parquet 原生 Schema**（`MessageType`）：Parquet 格式自身的类型系统，始终存在
2. **Spark SQL Schema**（JSON 字符串）：Spark 写入时嵌入到文件的 key-value metadata 中（key 为 `org.apache.spark.sql.parquet.row.metadata`），仅 Spark 写出的文件包含

此外，目录中可能存在两种摘要文件：

| 文件名 | 说明 |
|--------|------|
| `_common_metadata` | 仅包含合并后的 Schema 信息，体积小 |
| `_metadata` | 包含合并后的 Schema + 所有行组（row group）信息，体积较大 |

> 注意：摘要文件在新版本 Spark 中默认不再生成（`parquet.summary.metadata.level=NONE`），主要存在于老版本数据中。

### 3.2 文件选择策略

Spark 在推断前会先将目录下的文件分为三类，然后根据是否开启合并选择不同策略：

**不合并时（默认）——只需要读一个文件：**

```
优先级: _common_metadata > _metadata > 第一个数据文件（按路径字典序）
```

找到优先级最高的文件后只读取该文件的 footer。

**合并时——需要读取所有文件：**

```
需要读取: 所有数据文件 + _metadata + _common_metadata
```

当配置 `spark.sql.parquet.respectSummaryFiles=true` 时，信任摘要文件中的 Schema，可以跳过所有数据文件的 footer 读取，仅需读取摘要文件。

### 3.3 Schema 提取优先级

从单个文件中提取 Schema 时，优先尝试使用 Spark SQL Schema：

1. 尝试读取 key-value metadata 中的 Spark SQL Schema JSON 字符串
2. 尝试解析为 JSON（当前格式）
3. 回退到旧版 `DataType.fromCaseClassString` 解析
4. 若都失败，回退到将 Parquet 原生 `MessageType` 转换为 Spark `StructType`

使用 Spark SQL Schema 的好处是可以保留 Spark 特有的精确类型信息（如 `ByteType`、`ShortType`），因为 Parquet 只有 `INT32`，无法区分这些类型。

### 3.4 Parquet 到 Spark 的类型映射

当使用 Parquet 原生 Schema 进行转换时，类型映射规则如下：

**基本类型：**

| Parquet 物理类型 | 逻辑注解 | Spark 类型 |
|-----------------|---------|-----------|
| BOOLEAN | — | BooleanType |
| FLOAT | — | FloatType |
| DOUBLE | — | DoubleType |
| INT32 | 无 | IntegerType |
| INT32 | INT(8, signed) | ByteType |
| INT32 | INT(16, signed) | ShortType |
| INT32 | DATE | DateType |
| INT32 | DECIMAL(p,s) | DecimalType(p,s)，p ≤ 9 |
| INT64 | 无 | LongType |
| INT64 | DECIMAL(p,s) | DecimalType(p,s)，p ≤ 18 |
| INT64 | TIMESTAMP(MICROS, adjusted=true) | TimestampType |
| INT64 | TIMESTAMP(MICROS, adjusted=false) | TimestampNTZType（需开启配置） |
| INT64 | TIME(MICROS) | TimeType |
| INT96 | — | TimestampType（需 `int96AsTimestamp=true`） |
| BINARY | UTF8 / ENUM / JSON | StringType |
| BINARY | 无 | BinaryType（或 StringType，取决于 `binaryAsString`） |
| BINARY | DECIMAL(p,s) | DecimalType(p,s) |
| FIXED_LEN_BYTE_ARRAY | DECIMAL(p,s) | DecimalType(p,s) |

**复合类型：**

| Parquet 结构 | Spark 类型 |
|-------------|-----------|
| GROUP + LIST 注解 | ArrayType |
| GROUP + MAP 注解 | MapType |
| GROUP（无注解） | StructType |
| GROUP + VARIANT 注解 | VariantType |

**影响类型推断的配置：**

| 配置 | 默认值 | 影响 |
|------|--------|------|
| `spark.sql.parquet.binaryAsString` | false | 为 true 时，无注解的 BINARY 推断为 StringType |
| `spark.sql.parquet.int96AsTimestamp` | true | 为 true 时，INT96 推断为 TimestampType |
| `spark.sql.parquet.inferTimestampNTZ.enabled` | true | 为 true 时，未标记 `adjustedToUTC` 的时间戳推断为 TimestampNTZType |

## 4. Schema 合并详解

### 4.1 何时触发

Schema 合并在以下任一条件满足时触发：

```python
# 方式一：读取时指定（优先级高于全局配置）
df = spark.read.option("mergeSchema", "true").parquet(path)

# 方式二：全局配置
spark.conf.set("spark.sql.parquet.mergeSchema", "true")
```

默认值为 `false`，即默认不合并。

注意：如果用户通过 `.schema(...)` 显式指定了 Schema，即使设置了 `mergeSchema=true`，合并也不会发生——用户指定的 Schema 优先级最高。

### 4.2 合并流程

合并使用 Spark 的分布式计算能力，分为两轮：

```
           文件列表 [f1, f2, f3, ..., fn]
                       │
                 parallelize 分发
                       │
       ┌───────────────┼───────────────┐
       ▼               ▼               ▼
  Partition 1     Partition 2     Partition 3
  ┌──────────┐   ┌──────────┐   ┌──────────┐
  │读 footer  │   │读 footer  │   │读 footer  │
  │提取 schema│   │提取 schema│   │提取 schema│
  │局部 merge │   │局部 merge │   │局部 merge │
  └────┬─────┘   └────┬─────┘   └────┬─────┘
       │               │               │
       └───────┬───────┴───────┬───────┘
               ▼               ▼
          collect() 回 Driver
               │
               ▼
        Driver 端最终 merge
               │
               ▼
         最终 StructType
```

- 并行度 = `min(文件数, spark.default.parallelism)`
- Footer 读取使用 8 线程并行，且跳过行组信息（`SKIP_ROW_GROUPS`），只读取元数据
- 先在 Executor 端做局部合并，再在 Driver 端做全局合并

### 4.3 合并算法

合并的核心是 `StructType.merge` 方法，它是一个递归的、基于字段名匹配的并集算法。

**Struct 字段级合并：**

```
merge({A: T1, B: T2}, {B: T3, C: T4})
→ {A: T1, B: merge(T2, T3), C: T4}
```

规则：
1. **同名字段**：递归合并类型，nullable 取 OR
2. **左侧独有字段**：保留原样
3. **右侧独有字段**：追加到结果末尾

**递归类型合并规则：**

| 类型组合 | 合并规则 |
|---------|---------|
| StructType + StructType | 递归按字段名合并（如上） |
| ArrayType + ArrayType | 递归合并元素类型，`containsNull` 取 OR |
| MapType + MapType | 分别递归合并 key 和 value 类型，`valueContainsNull` 取 OR |
| DecimalType + DecimalType | scale 必须相同，precision 取 max |
| 完全相同的类型 | 直接返回 |
| 其他不兼容类型 | **抛出异常** |

**字段顺序：** 所有文件按路径字典序排序后，第一个文件的字段顺序决定最终结果中这些字段的位置，后续文件新增的字段按出现顺序追加到末尾。

### 4.4 异构 Schema 的处理

#### 4.4.1 列数不同——正常合并

```
file1: {name: STRING, age: INT}
file2: {name: STRING, age: INT, email: STRING}

合并结果: {name: STRING, age: INT, email: STRING}
读取 file1 时: email 列的值为 null
```

#### 4.4.2 列名不同——取并集

```
file1: {x: INT, y: INT}
file2: {y: INT, z: INT}

合并结果: {x: INT, y: INT, z: INT}
读取 file1 时: z = null
读取 file2 时: x = null
```

#### 4.4.3 嵌套 Struct 字段不同——递归合并

```
file1: {info: {city: STRING}}
file2: {info: {city: STRING, zip: INT}}

合并结果: {info: {city: STRING, zip: INT}}
读取 file1 时: info.zip = null
```

#### 4.4.4 Array 内嵌套的 Struct 不同——递归合并元素类型

```
file1: {tags: Array<{k: STRING}>}
file2: {tags: Array<{k: STRING, v: INT}>}

合并结果: {tags: Array<{k: STRING, v: INT}>}
```

#### 4.4.5 Map 的 value 类型不同——递归合并 value 类型

```
file1: {props: Map<STRING, {a: INT}>}
file2: {props: Map<STRING, {a: INT, b: STRING}>}

合并结果: {props: Map<STRING, {a: INT, b: STRING}>}
```

#### 4.4.6 Decimal 精度不同——取更大精度

```
file1: {amount: DECIMAL(10, 2)}
file2: {amount: DECIMAL(15, 2)}

合并结果: {amount: DECIMAL(15, 2)}
```

注意：scale（小数位数）必须相同，否则抛出异常。

#### 4.4.7 不兼容的类型——抛出异常

以下情况会抛出 `CANNOT_MERGE_SCHEMAS` 异常：

```
file1: {id: LONG}
file2: {id: INT}         → 错误：不做隐式类型提升

file1: {col: STRING}
file2: {col: INT}         → 错误：类型不兼容

file1: {val: DECIMAL(10, 2)}
file2: {val: DECIMAL(10, 3)} → 错误：scale 不同
```

**重要：Spark 的 Schema 合并不会做隐式类型提升（如 INT → LONG），类型必须严格匹配或属于上述可合并的特殊情况。**

#### 4.4.8 Nullable 差异——取 OR

```
file1: {name: STRING (not null)}
file2: {name: STRING (nullable)}

合并结果: {name: STRING (nullable)}
```

任意一侧为 nullable，结果即为 nullable。

#### 4.4.9 大小写敏感性

```
file1: {col: INT}
file2: {COL: INT}
```

- `spark.sql.caseSensitive=true`（默认 false）：视为两个不同的列，合并结果为 `{col: INT, COL: INT}`
- `spark.sql.caseSensitive=false`（默认）：视为同一列，合并为一个

### 4.5 合并后的读取行为

合并产生的超集 Schema 作为 "requested schema" 传递给每个文件的 reader。读取每个文件时，Spark 通过 **Schema Clipping** 机制调和差异：

1. **文件中有的列、Schema 也要求的列**：正常读取
2. **Schema 要求但文件中缺失的列**：该列的值填充为 `null`
3. **文件中有但 Schema 未要求的列**：跳过（列裁剪），不影响性能

这个机制对嵌套类型同样适用——缺失的嵌套字段也会被填充为 `null`。

## 5. 常见场景与行为对照表

| 场景 | mergeSchema=false | mergeSchema=true |
|------|-------------------|-----------------|
| 所有文件 Schema 一致 | 正常工作 | 正常工作（但额外读取所有 footer） |
| 新文件新增了列 | 读到新文件的行可能报错或丢数据 | 自动合并，旧文件中新列值为 null |
| 不同文件有完全不同的列 | 以第一个文件的 Schema 为准，其他列丢失 | 所有列取并集 |
| 同名列类型不同（INT vs LONG） | 读取时可能出现运行时异常 | 合并阶段直接抛出异常 |
| 嵌套 struct 内部字段不同 | 按第一个文件的 struct schema 读 | 递归合并 struct 内部字段 |
| Decimal 精度不同但 scale 相同 | 按第一个文件精度读 | 自动取更大精度 |
| 使用了 `.schema(...)` | 按用户指定 Schema 读 | 按用户指定 Schema 读（mergeSchema 被忽略）|

## 6. 配置参考

### 核心配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.sql.parquet.mergeSchema` | false | 是否开启 Schema 合并 |
| `spark.sql.parquet.respectSummaryFiles` | false | 开启 merge 时，是否信任摘要文件而跳过数据文件读取 |

### 类型推断相关

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.sql.parquet.binaryAsString` | false | 将无注解的 BINARY 推断为 STRING |
| `spark.sql.parquet.int96AsTimestamp` | true | 将 INT96 推断为 Timestamp |
| `spark.sql.parquet.inferTimestampNTZ.enabled` | true | 未标记 adjustedToUTC 的时间戳推断为 TimestampNTZ |
| `spark.sql.parquet.fieldId.read.enabled` | true | 使用 Parquet field ID 匹配列 |

### 读取选项（Data Source Option）

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `mergeSchema` | 取全局配置值 | 覆盖全局 `spark.sql.parquet.mergeSchema` 配置 |

## 7. 性能与最佳实践

### 7.1 Schema 稳定时：使用默认设置

如果数据的 Schema 不会变化，保持 `mergeSchema=false`（默认值）即可。Spark 只读取一个文件的 footer，开销极小。

### 7.2 Schema 已知时：显式指定

对于生产环境中 Schema 已知的场景，使用 `.schema(...)` 显式指定是最优选择：

```python
df = spark.read.schema("id LONG, name STRING, ts TIMESTAMP").parquet(path)
```

好处：
- 零 footer 读取开销
- 避免推断过程中的歧义
- 明确控制列类型

### 7.3 Schema 演化时：按需开启合并

仅在确实需要读取不同 Schema 文件时开启 `mergeSchema`。建议在读取层面通过 option 开启，而非修改全局配置：

```python
df = spark.read.option("mergeSchema", "true").parquet(path)
```

### 7.4 文件数量巨大时：利用摘要文件

如果目录下有大量 Parquet 文件且需要合并 Schema，逐个读取 footer 会很慢（尤其在 S3 等对象存储上）。可以：

1. 写入时生成摘要文件：

   ```python
   spark.conf.set("parquet.summary.metadata.level", "ALL")
   ```

2. 读取时信任摘要文件：

   ```python
   spark.conf.set("spark.sql.parquet.respectSummaryFiles", "true")
   df = spark.read.option("mergeSchema", "true").parquet(path)
   ```

这样 Spark 只需读取摘要文件，而不用扫描每个数据文件的 footer。

### 7.5 注意事项

- Schema 合并**不做隐式类型提升**。如果同名列在不同文件中类型不同（如 INT vs LONG），合并会失败。写入时应确保类型一致。
- 合并结果中的列顺序取决于文件路径的字典序排列，不同环境可能不同。如果对列顺序有要求，建议显式指定 Schema 或在读取后做 `select`。
- 读取 Parquet 时，所有列默认被标记为 nullable（无论文件中的定义如何），这是为了兼容性。
