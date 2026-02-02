# 4_3 Window Functions: Ranking and Deduplication

## 1. What is a Window Function?

A **Window Function** performs a calculation across a set of table rows that are somehow related to the current row. Unlike `groupBy`, Window functions do not collapse rows; they allow you to keep the original row detail while adding an aggregate or ranking value.

## 2. Core Components of a Window

To define a "Window," you need:

* **partitionBy()**: Defines how to group the rows (e.g., group by `tail_number`).
* **orderBy()**: Defines the sequence within the group (e.g., sort by `timestamp` descending).
* **rowsBetween() / rangeBetween()**: (Optional) Defines the boundaries of the frame.

## 3. Common Ranking Functions

* **row_number()**: Assigns a unique, sequential number to rows (1, 2, 3...). Ideal for deduplication.
* **rank()**: Assigns the same rank to ties, but leaves gaps (1, 2, 2, 4...).
* **dense_rank()**: Assigns the same rank to ties without gaps (1, 2, 2, 3...).

## 4. Use Case: Getting the Latest Record

The most common DE pattern: Partition by ID, Order by Date Descending, and filter for `row_number == 1`.
