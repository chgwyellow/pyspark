CREATE DATABASE maintenance_db;
-- 1. Drop Tables if Exist and Create New Ones
DROP TABLE IF EXISTS fact_maintenance_work_orders;
DROP TABLE IF EXISTS dim_aircraft;
CREATE TABLE dim_aircraft (
    tail_number VARCHAR(10) PRIMARY KEY,
    aircraft_type VARCHAR(20),
    delivery_date DATE,
    total_flight_hours INT
);
CREATE TABLE fact_maintenance_work_orders (
    wo_id SERIAL PRIMARY KEY,
    tail_number VARCHAR(10),
    issue_category VARCHAR(50),
    issue_description TEXT,
    priority VARCHAR(10),
    status VARCHAR(20),
    reported_at TIMESTAMP
);
-- 2. Insert Fleet Data
INSERT INTO dim_aircraft
VALUES -- Airbus A321neo (13 aircraft)
    ('B-58201', 'A321neo', '2021-10-21', 4500),
    ('B-58202', 'A321neo', '2021-12-05', 4200),
    ('B-58203', 'A321neo', '2022-01-15', 3900),
    ('B-58204', 'A321neo', '2022-03-10', 3700),
    ('B-58205', 'A321neo', '2022-05-20', 3500),
    ('B-58206', 'A321neo', '2022-07-08', 3300),
    ('B-58207', 'A321neo', '2022-09-12', 3100),
    ('B-58208', 'A321neo', '2022-11-25', 2900),
    ('B-58209', 'A321neo', '2023-02-14', 2600),
    ('B-58210', 'A321neo', '2023-04-30', 2400),
    ('B-58211', 'A321neo', '2023-07-18', 2100),
    ('B-58212', 'A321neo', '2023-10-05', 1800),
    ('B-58213', 'A321neo', '2024-01-20', 1500),
    -- Airbus A330-900 (6 aircraft)
    ('B-58301', 'A330-900', '2022-05-15', 3100),
    ('B-58302', 'A330-900', '2022-07-20', 2800),
    ('B-58303', 'A330-900', '2023-03-10', 2200),
    ('B-58304', 'A330-900', '2023-08-15', 1900),
    ('B-58305', 'A330-900', '2024-02-28', 1400),
    ('B-58306', 'A330-900', '2024-06-12', 1100),
    -- Airbus A350-900 (10 aircraft)
    ('B-58501', 'A350-900', '2022-10-28', 1500),
    ('B-58502', 'A350-900', '2023-01-12', 1200),
    ('B-58503', 'A350-900', '2023-04-05', 1000),
    ('B-58504', 'A350-900', '2023-07-22', 850),
    ('B-58505', 'A350-900', '2023-10-18', 700),
    ('B-58506', 'A350-900', '2024-01-30', 550),
    ('B-58507', 'A350-900', '2024-05-14', 400),
    ('B-58508', 'A350-900', '2024-08-20', 280),
    ('B-58509', 'A350-900', '2024-11-10', 150),
    ('B-58510', 'A350-900', '2025-02-25', 80),
    -- Airbus A350-1000 (1 aircraft, first delivery)
    ('B-58551', 'A350-1000', '2026-01-15', 20);
-- 3. Insert Comprehensive Maintenance Records for All Aircraft
-- Ensuring every tail number has at least one maintenance record
INSERT INTO fact_maintenance_work_orders (
        tail_number,
        issue_category,
        issue_description,
        priority,
        status,
        reported_at
    )
VALUES -- A321neo Fleet Maintenance
    (
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
        'B-58201',
        'Engine',
        'Oil filter replacement',
        'High',
        'Pending',
        '2025-02-10 13:00:00'
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
        'B-58202',
        'Cabin',
        'Seat 12A tray table broken',
        'Low',
        'Open',
        '2025-02-01 09:00:00'
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
        'B-58202',
        'Tire',
        'Main landing gear tire replacement',
        'High',
        'Closed',
        '2025-01-25 11:00:00'
    ),
    (
        'B-58203',
        'Engine',
        'Fan blade inspection',
        'High',
        'Closed',
        '2025-01-18 07:45:00'
    ),
    (
        'B-58203',
        'Systems',
        'APU maintenance check',
        'Medium',
        'Open',
        '2025-02-08 15:30:00'
    ),
    (
        'B-58204',
        'Cabin',
        'Overhead bin latch repair',
        'Low',
        'Pending',
        '2025-02-03 10:20:00'
    ),
    (
        'B-58204',
        'Hydraulic',
        'Brake system pressure test',
        'High',
        'Closed',
        '2025-01-12 09:15:00'
    ),
    (
        'B-58205',
        'Avionics',
        'TCAS system calibration',
        'High',
        'Open',
        '2025-02-11 14:00:00'
    ),
    (
        'B-58205',
        'Exterior',
        'Winglet paint touch-up',
        'Low',
        'Closed',
        '2025-01-28 16:45:00'
    ),
    (
        'B-58206',
        'Engine',
        'Thrust reverser inspection',
        'Medium',
        'Closed',
        '2025-01-22 08:00:00'
    ),
    (
        'B-58206',
        'Cabin',
        'Lavatory door mechanism repair',
        'Low',
        'Open',
        '2025-02-09 11:30:00'
    ),
    (
        'B-58207',
        'Systems',
        'Pressurization system check',
        'High',
        'Pending',
        '2025-02-13 09:45:00'
    ),
    (
        'B-58207',
        'Tire',
        'Nose gear tire inspection',
        'Medium',
        'Closed',
        '2025-01-30 13:20:00'
    ),
    (
        'B-58208',
        'Avionics',
        'Flight management system update',
        'Medium',
        'Open',
        '2025-02-07 10:15:00'
    ),
    (
        'B-58208',
        'Hydraulic',
        'Landing gear actuator service',
        'High',
        'Closed',
        '2025-01-19 14:30:00'
    ),
    (
        'B-58209',
        'Cabin',
        'Emergency exit light replacement',
        'Medium',
        'Closed',
        '2025-01-26 08:45:00'
    ),
    (
        'B-58209',
        'Engine',
        'Oil quantity sensor check',
        'Low',
        'Open',
        '2025-02-10 15:00:00'
    ),
    (
        'B-58210',
        'Exterior',
        'Fuselage skin crack inspection',
        'High',
        'Pending',
        '2025-02-12 07:30:00'
    ),
    (
        'B-58210',
        'Systems',
        'Anti-ice system test',
        'Medium',
        'Closed',
        '2025-01-24 12:00:00'
    ),
    (
        'B-58211',
        'Avionics',
        'Weather radar alignment',
        'Medium',
        'Open',
        '2025-02-08 09:20:00'
    ),
    (
        'B-58211',
        'Cabin',
        'Passenger oxygen mask inspection',
        'High',
        'Closed',
        '2025-01-17 11:45:00'
    ),
    (
        'B-58212',
        'Engine',
        'Compressor blade cleaning',
        'Low',
        'Closed',
        '2025-01-29 14:15:00'
    ),
    (
        'B-58212',
        'Tire',
        'Wheel bearing lubrication',
        'Medium',
        'Open',
        '2025-02-11 10:30:00'
    ),
    (
        'B-58213',
        'Systems',
        'Fire detection system test',
        'High',
        'Pending',
        '2025-02-14 08:00:00'
    ),
    (
        'B-58213',
        'Hydraulic',
        'Reservoir fluid level check',
        'Low',
        'Closed',
        '2025-01-31 16:20:00'
    ),
    -- A330-900 Fleet Maintenance
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
        'B-58301',
        'Cabin',
        'Galley light replacement',
        'Low',
        'Closed',
        '2025-01-28 22:10:00'
    ),
    (
        'B-58301',
        'Avionics',
        'Autopilot system diagnostic',
        'High',
        'Open',
        '2025-02-09 13:30:00'
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
        'B-58302',
        'Systems',
        'Oxygen mask system test',
        'High',
        'Closed',
        '2025-01-30 09:30:00'
    ),
    (
        'B-58302',
        'Engine',
        'Fuel nozzle inspection',
        'Medium',
        'Open',
        '2025-02-13 15:45:00'
    ),
    (
        'B-58303',
        'Hydraulic',
        'Flight control hydraulic leak check',
        'High',
        'Closed',
        '2025-01-16 07:20:00'
    ),
    (
        'B-58303',
        'Cabin',
        'Business class seat recline issue',
        'Medium',
        'Pending',
        '2025-02-06 10:00:00'
    ),
    (
        'B-58304',
        'Avionics',
        'ILS receiver calibration',
        'High',
        'Open',
        '2025-02-10 14:15:00'
    ),
    (
        'B-58304',
        'Tire',
        'Main gear tire tread depth check',
        'Medium',
        'Closed',
        '2025-01-27 09:45:00'
    ),
    (
        'B-58305',
        'Engine',
        'Bleed air system inspection',
        'High',
        'Closed',
        '2025-01-21 11:30:00'
    ),
    (
        'B-58305',
        'Systems',
        'Cabin pressure controller test',
        'Medium',
        'Open',
        '2025-02-12 08:20:00'
    ),
    (
        'B-58306',
        'Exterior',
        'Wing leading edge de-ice boot check',
        'Medium',
        'Pending',
        '2025-02-14 13:00:00'
    ),
    (
        'B-58306',
        'Cabin',
        'In-flight entertainment screen flickering',
        'Low',
        'Closed',
        '2025-01-23 17:30:00'
    ),
    -- A350-900 Fleet Maintenance
    (
        'B-58501',
        'Systems',
        'In-flight entertainment system reboot',
        'Low',
        'Closed',
        '2025-01-05 20:00:00'
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
        'B-58501',
        'Engine',
        'Trent XWB engine oil analysis',
        'Medium',
        'Closed',
        '2025-01-19 12:45:00'
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
        'B-58502',
        'Avionics',
        'Cockpit display flickering',
        'High',
        'Pending',
        '2025-02-14 15:20:00'
    ),
    (
        'B-58502',
        'Cabin',
        'Premium economy seat power outlet repair',
        'Low',
        'Closed',
        '2025-01-26 14:30:00'
    ),
    (
        'B-58503',
        'Engine',
        'Fan cowl door latch inspection',
        'Medium',
        'Closed',
        '2025-01-18 09:15:00'
    ),
    (
        'B-58503',
        'Systems',
        'Environmental control system check',
        'High',
        'Open',
        '2025-02-09 11:00:00'
    ),
    (
        'B-58504',
        'Avionics',
        'Flight control computer software update',
        'High',
        'Pending',
        '2025-02-13 10:30:00'
    ),
    (
        'B-58504',
        'Exterior',
        'Composite panel damage assessment',
        'Medium',
        'Closed',
        '2025-01-24 16:00:00'
    ),
    (
        'B-58505',
        'Tire',
        'Brake temperature sensor replacement',
        'Medium',
        'Open',
        '2025-02-10 13:45:00'
    ),
    (
        'B-58505',
        'Cabin',
        'Galley oven heating element failure',
        'Low',
        'Closed',
        '2025-01-29 08:20:00'
    ),
    (
        'B-58506',
        'Hydraulic',
        'Landing gear retraction test',
        'High',
        'Closed',
        '2025-01-15 14:00:00'
    ),
    (
        'B-58506',
        'Engine',
        'Thrust reverser actuator service',
        'Medium',
        'Pending',
        '2025-02-11 09:30:00'
    ),
    (
        'B-58507',
        'Systems',
        'Fuel quantity indication system check',
        'Medium',
        'Open',
        '2025-02-08 15:15:00'
    ),
    (
        'B-58507',
        'Avionics',
        'Satellite communication antenna alignment',
        'Low',
        'Closed',
        '2025-01-22 11:45:00'
    ),
    (
        'B-58508',
        'Cabin',
        'First class suite privacy door malfunction',
        'Medium',
        'Pending',
        '2025-02-12 10:00:00'
    ),
    (
        'B-58508',
        'Exterior',
        'Radome inspection for bird strike damage',
        'High',
        'Closed',
        '2025-01-20 07:30:00'
    ),
    (
        'B-58509',
        'Engine',
        'Engine vibration monitoring system test',
        'High',
        'Open',
        '2025-02-13 14:20:00'
    ),
    (
        'B-58509',
        'Tire',
        'Nose landing gear steering check',
        'Medium',
        'Closed',
        '2025-01-28 12:15:00'
    ),
    (
        'B-58510',
        'Avionics',
        'Head-up display calibration',
        'Medium',
        'Open',
        '2025-02-14 09:00:00'
    ),
    (
        'B-58510',
        'Systems',
        'Ram air turbine deployment test',
        'High',
        'Pending',
        '2025-02-07 16:30:00'
    ),
    -- A350-1000 Fleet Maintenance (Newest aircraft)
    (
        'B-58551',
        'Systems',
        'Post-delivery systems verification',
        'High',
        'Closed',
        '2026-01-20 10:00:00'
    ),
    (
        'B-58551',
        'Avionics',
        'Initial flight management system setup',
        'Medium',
        'Closed',
        '2026-01-22 14:30:00'
    ),
    (
        'B-58551',
        'Cabin',
        'New cabin configuration inspection',
        'Low',
        'Open',
        '2026-02-01 11:15:00'
    );