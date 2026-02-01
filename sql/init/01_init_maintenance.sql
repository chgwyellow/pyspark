CREATE DATABASE maintenance_db;
-- 1. Create Tables
CREATE TABLE aircraft_master (
    tail_number VARCHAR(10) PRIMARY KEY,
    aircraft_type VARCHAR(20),
    delivery_date DATE,
    total_flight_hours INT
);
CREATE TABLE maintenance_work_orders (
    wo_id SERIAL PRIMARY KEY,
    tail_number VARCHAR(10),
    issue_category VARCHAR(50),
    issue_description TEXT,
    priority VARCHAR(10),
    status VARCHAR(20),
    reported_at TIMESTAMP
);
-- 2. Insert Expanded Aircraft Master Data
INSERT INTO aircraft_master
VALUES ('B-58201', 'A321neo', '2021-10-21', 4500),
    ('B-58202', 'A321neo', '2021-12-05', 4200),
    ('B-58301', 'A330neo', '2022-05-15', 3100),
    ('B-58302', 'A330neo', '2022-07-20', 2800),
    ('B-58501', 'A350-900', '2022-10-28', 1500),
    ('B-58502', 'A350-900', '2023-01-12', 1200);
-- 3. Insert Batch Maintenance Records (Simulating a larger volume)
-- Generating 15 random records for the demo
INSERT INTO maintenance_work_orders (
        tail_number,
        issue_category,
        issue_description,
        priority,
        status,
        reported_at
    )
VALUES (
        'B-58201',
        'Hydraulic',
        'Hydraulic leak check on left wing',
        'High',
        'Closed',
        '2025-01-10 08:30:00'
    ),
    (
        'B-58201',
        'Avionics',
        'Radar signal interference reported',
        'Medium',
        'Closed',
        '2025-01-15 14:20:00'
    ),
    (
        'B-58202',
        'Cabin',
        'Seat 12A tray table broken',
        'Low',
        'Open',
        '2025-02-01 09:00:00'
    ),
    (
        'B-58301',
        'Engine',
        'Engine sensor #2 calibration',
        'High',
        'Open',
        '2025-01-20 18:45:00'
    ),
    (
        'B-58301',
        'Tire',
        'Nose gear tire pressure low',
        'Medium',
        'Closed',
        '2025-01-22 10:15:00'
    ),
    (
        'B-58302',
        'Exterior',
        'Fuselage paint chip repair',
        'Low',
        'Pending',
        '2025-02-05 11:30:00'
    ),
    (
        'B-58501',
        'Systems',
        'In-flight entertainment system reboot',
        'Low',
        'Closed',
        '2025-01-05 20:00:00'
    ),
    (
        'B-58502',
        'Hydraulic',
        'Fluid level replenishment',
        'Medium',
        'Open',
        '2025-02-08 07:45:00'
    ),
    (
        'B-58201',
        'Engine',
        'Oil filter replacement',
        'High',
        'Pending',
        '2025-02-10 13:00:00'
    ),
    (
        'B-58301',
        'Cabin',
        'Galley light replacement',
        'Low',
        'Closed',
        '2025-01-28 22:10:00'
    ),
    (
        'B-58202',
        'Avionics',
        'GPS module firmware update',
        'Medium',
        'Open',
        '2025-02-12 16:30:00'
    ),
    (
        'B-58501',
        'Tire',
        'Main gear brake pad inspection',
        'High',
        'Open',
        '2025-02-11 08:00:00'
    ),
    (
        'B-58302',
        'Systems',
        'Oxygen mask system test',
        'High',
        'Closed',
        '2025-01-30 09:30:00'
    ),
    (
        'B-58201',
        'Exterior',
        'Landing gear door inspection',
        'Medium',
        'Open',
        '2025-02-14 10:00:00'
    ),
    (
        'B-58502',
        'Avionics',
        'Cockpit display flickering',
        'High',
        'Pending',
        '2025-02-14 15:20:00'
    );