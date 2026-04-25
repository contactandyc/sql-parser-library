    /*
    // DO NOT DELETE, commented out temporarily!
    const char *queries2[] = {
        // Test 1: Simple Scan & Filter
        "SELECT name, comment "
        "FROM region "
        "WHERE name = 'AMERICA'",

        // Test 2: Joins, Sorting, Limit, and Offset
        "SELECT n.name AS Nation, r.name AS Region "
        "FROM nation n "
        "JOIN region r ON n.regionkey = r.regionkey "
        "ORDER BY Nation ASC "
        "LIMIT 5 OFFSET 2",

        // Test 3: Aggregation & Grouping
        "SELECT r.name AS Region, SUM(n.nationkey) AS NationKeySum "
        "FROM nation n "
        "JOIN region r ON n.regionkey = r.regionkey "
        "GROUP BY r.name "
        "ORDER BY Region ASC",

        // Test 4: Aggregation with HAVING
        "SELECT r.name AS Region, SUM(n.nationkey) AS NationKeySum "
        "FROM nation n "
        "JOIN region r ON n.regionkey = r.regionkey "
        "GROUP BY r.name "
        "HAVING NationKeySum > 10 "
        "ORDER BY Region DESC",

        // Test 5: The 4-Table TPC-H Beast
        "SELECT n.name AS Nation, SUM(l.extendedprice * (1 - l.discount)) AS Revenue "
        "FROM part p "
        "JOIN lineitem l ON p.partkey = l.partkey "
        "JOIN supplier s ON l.suppkey = s.suppkey "
        "JOIN nation n ON s.nationkey = n.nationkey "
        "WHERE p.mfgr = 'Manufacturer#1' "
        "GROUP BY n.name "
        "ORDER BY Revenue DESC "
        "LIMIT 10",

        // Test 6: Common Table Expressions (WITH) Support!
        "WITH regional_nations AS ( "
        "    SELECT n.name AS Nation, r.name AS Region, n.nationkey "
        "    FROM nation n "
        "    JOIN region r ON n.regionkey = r.regionkey "
        "    WHERE r.name = 'ASIA' "
        ") "
        "SELECT Region, SUM(nationkey) AS SumKey "
        "FROM regional_nations "
        "GROUP BY Region",

        // Test 7: Multiple CTEs referencing each other
        "WITH supplier_count AS ( "
        "    SELECT nationkey, COUNT(suppkey) AS supp_count "
        "    FROM supplier "
        "    GROUP BY nationkey "
        "), "
        "nation_regions AS ( "
        "    SELECT n.nationkey, n.name AS nation_name, r.name AS region_name "
        "    FROM nation n "
        "    JOIN region r ON n.regionkey = r.regionkey "
        ") "
        "SELECT nr.region_name, SUM(sc.supp_count) AS total_suppliers "
        "FROM nation_regions nr "
        "JOIN supplier_count sc ON nr.nationkey = sc.nationkey "
        "GROUP BY nr.region_name "
        "ORDER BY total_suppliers DESC",

        // Test 8: LEFT JOIN & COUNT
        "SELECT n.name AS Nation, COUNT(s.suppkey) AS Supp_Count "
        "FROM nation n "
        "LEFT JOIN supplier s ON n.nationkey = s.nationkey "
        "GROUP BY n.name "
        "ORDER BY Supp_Count ASC, Nation ASC "
        "LIMIT 10",

        // Test 9: Complex Aggregation Math
        "SELECT c.mktsegment, SUM(c.acctbal) AS TotalBal, (SUM(c.acctbal) / COUNT(c.custkey)) AS AvgBal "
        "FROM customer c "
        "GROUP BY c.mktsegment "
        "HAVING AvgBal > 4000 "
        "ORDER BY TotalBal DESC",

        // Test 10: Deeply Nested Subqueries
        "SELECT top_nations.Region, top_nations.Nation "
        "FROM ( "
        "    SELECT r.name AS Region, n.name AS Nation "
        "    FROM nation n "
        "    JOIN region r ON n.regionkey = r.regionkey "
        ") AS top_nations "
        "WHERE top_nations.Region = 'EUROPE' "
        "ORDER BY top_nations.Nation ASC",

        // Test 11: IS NULL and Boolean logic
        "SELECT c.name, c.acctbal "
        "FROM customer c "
        "LEFT JOIN orders o ON c.custkey = o.custkey "
        "WHERE o.orderkey IS NULL AND c.acctbal > 9000 "
        "ORDER BY c.acctbal DESC "
        "LIMIT 5",

        // Test 12: General Multi-Join test
        "SELECT n.name AS Nation, SUM(l.extendedprice * (1 - l.discount)) AS Revenue "
        "FROM part p "
        "JOIN lineitem l ON p.partkey = l.partkey "
        "JOIN supplier s ON l.suppkey = s.suppkey "
        "JOIN nation n ON s.nationkey = n.nationkey "
        "WHERE p.mfgr = 'Manufacturer#1' "
        "GROUP BY n.name "
        "ORDER BY Revenue DESC",

        // --- NEW: WINDOW FUNCTION TESTS ---

        // Test 13: Simple Global ROW_NUMBER (No partitions, just order)
        "SELECT name, regionkey, ROW_NUMBER() OVER (ORDER BY name ASC) AS row_num "
        "FROM nation "
        "LIMIT 10",

        // Test 14: Partitioned ROW_NUMBER (Resets state when region changes)
        "SELECT r.name AS Region, n.name AS Nation, ROW_NUMBER() OVER (PARTITION BY r.name ORDER BY n.name ASC) AS nation_rank "
        "FROM nation n "
        "JOIN region r ON n.regionkey = r.regionkey "
        "ORDER BY Region ASC, nation_rank ASC",

        // Test 15: RANK vs ROW_NUMBER (Tests ties and gaps)
        "SELECT mfgr, brand, size, "
        "ROW_NUMBER() OVER (PARTITION BY mfgr ORDER BY size DESC) AS rn, "
        "RANK() OVER (PARTITION BY mfgr ORDER BY size DESC) AS rnk "
        "FROM part "
        "WHERE mfgr = 'Manufacturer#1' "
        "LIMIT 15",

        // Test 16: Top-N per Category (Uses Window Function in subquery)
        "SELECT * FROM ( "
        "    SELECT r.name AS Region, n.name AS Nation, "
        "    ROW_NUMBER() OVER (PARTITION BY r.name ORDER BY n.nationkey DESC) AS rn "
        "    FROM nation n "
        "    JOIN region r ON n.regionkey = r.regionkey "
        ") AS ranked_nations "
        "WHERE ranked_nations.rn <= 2 "
        "ORDER BY Region ASC, rn ASC",

        // Test 17: Interval Math! (Implicitly casts shipdate and commitdate)
        "SELECT l.orderkey, l.shipdate, l.commitdate "
        "FROM lineitem l "
        "WHERE l.commitdate < l.shipdate - INTERVAL '30' DAY "
        "ORDER BY l.orderkey DESC "
        "LIMIT 10",

        // Test 18: Date Extraction & Grouping
        "SELECT EXTRACT(YEAR FROM l.shipdate) AS ship_year, COUNT(*) AS items "
        "FROM lineitem l "
        "GROUP BY ship_year "
        "ORDER BY items DESC "
        "LIMIT 5"
    };
    */