-- Q2 | DDL for user_devices_cumulated
-- DROP TABLE user_devices_cumulated;
CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    browser_type TEXT, 
    device_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (user_id, browser_type, date)
);