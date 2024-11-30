-- Q7 | DDL for monthly reduced fact table host_activity_reduced
-- DROP TABLE host_activity_reduced;
CREATE TABLE host_activity_reduced (
    month DATE,
    host TEXT,
    hit_array INTEGER[],
    unique_visitors INTEGER[],
    PRIMARY KEY (month, host)
)