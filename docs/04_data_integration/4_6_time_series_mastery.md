# 4_6 Date & Time Mastery in Data Engineering

## 1. Timestamp vs. Date

* **Timestamp**: Includes date, time, and precision (e.g., `2026-02-03 14:30:00`). Essential for logging events.
* **Date**: Only includes the calendar day (`2026-02-03`). Used for grouping daily summaries or partition keys.

## 2. Essential Time Functions

* **date_trunc('unit', col)**: Truncates a timestamp to the specified level (hour, day, month). This is the standard for time-series aggregation.
* **unix_timestamp()**: Converts a timestamp to seconds since epoch. Essential for performing mathematical operations (like finding the duration between two events).
* **from_unixtime()**: Converts seconds back into a readable timestamp format.

## 3. Handling Timezones (Crucial for Airlines)

Airline data often arrives in UTC but needs to be analyzed in local airport time.

* **from_utc_timestamp()**: Adjusts UTC data to a specific timezone.
* **to_utc_timestamp()**: Normalizes local data back to UTC for global storage.

## 4. Business Logic Patterns

* **Shift Analysis**: Using `hour(col)` to categorize logs into "Morning", "Afternoon", or "Night" shifts.
* **Interval Calculation**: Using `last_day()` or `months_between()` to track maintenance deadlines.
