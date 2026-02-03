# 4_4 Advanced Windowing: Frames and Trend Analysis

## 1. Window Framing (Rows Between)

By default, a window with `orderBy` ranges from the "start of partition" to the "current row". You can limit this using:

* **rowsBetween(start, end)**:
  * `Window.unboundedPreceding`: The very first row.
  * `Window.currentRow`: The row you are on.
  * `-n`: n rows before.
  * `n`: n rows after.

## 2. Moving Aggregates (Sliding Windows)

Useful for smoothing out noisy sensor data. For example, a 3-point moving average of sensor values to filter out temporary spikes.

## 3. Gaps and Islands Problem

A classic data challenge to identify "sessions" or "continuous streaks".

* **Scenario**: Is this "Error" a new event, or a continuation of the same persistent issue from the previous log?
