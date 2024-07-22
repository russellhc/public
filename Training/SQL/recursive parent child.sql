CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    employee_name VARCHAR(100),
    manager_id INT
);

INSERT INTO employees (employee_id, employee_name, manager_id) VALUES
(1, 'CEO', NULL),           -- The CEO has no manager
(2, 'CTO', 1),              -- CTO reports to CEO
(3, 'CFO', 1),              -- CFO reports to CEO
(4, 'Engineer1', 2),        -- Engineer1 reports to CTO
(5, 'Engineer2', 2),        -- Engineer2 reports to CTO
(6, 'Accountant', 3);       -- Accountant reports to CFO


WITH RECURSIVE employee_hierarchy AS (
    -- Anchor member: start with employees who have no manager (root nodes)
    SELECT
        employee_id,
        employee_name,
        manager_id,
        employee_name AS path_string,
        employee_id as path_integer,
        1 AS level
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive member: find employees who report to the current level of employees
    SELECT
        e.employee_id,
        e.employee_name,
        e.manager_id,
        h.path_string || ' -> ' || e.employee_name AS path_string,
        h.path_integer || ' -> ' || e.employee_id AS path_integer,
        h.level + 1 AS level
    FROM employees e
    INNER JOIN employee_hierarchy h ON e.manager_id = h.employee_id
)

-- Final selection
SELECT
    employee_id,
    employee_name,
    manager_id,
    path_string,
    path_integer,
    level
FROM employee_hierarchy
ORDER BY path_string;