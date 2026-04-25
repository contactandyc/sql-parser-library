# Architecture Overview: SQL Parser & Virtual Machine

This document outlines the architecture of the custom SQL parsing and execution engine. The system is designed as a multi-stage pipeline that cleanly separates lexical analysis, syntax parsing, semantic binding, query optimization, compilation, and virtual machine execution.

## 1. The Execution Pipeline (High-Level)

The lifecycle of an SQL query passes through the following sequential stages:

1. **Tokenization:** Raw string $\rightarrow$ Tokens.
2. **Parsing (AST):** Tokens $\rightarrow$ Abstract Syntax Tree.
3. **Binding:** AST identifiers $\rightarrow$ Physical schema columns.
4. **Planning:** Bound AST $\rightarrow$ Optimized Execution Plan (Join orders, Index selection).
5. **Compilation:** AST & Plan $\rightarrow$ Strongly-typed VM Nodes & Execution Instructions.
6. **Execution (VM):** VM Nodes + Datasets $\rightarrow$ Materialized Result Set.

---

## 2. Module Breakdown

### Lexical & Syntax Analysis
These modules are responsible for understanding the text of the query and structuring it logically.

* **`sql_tokenizer.c/h`**
    * **Purpose:** Converts the raw SQL string into an array of lexical tokens (`sql_token_t`).
    * **Complexities:** Differentiates between keywords, identifiers, string literals, and complex compound literals (like `INTERVAL 1 DAY` or `TIMESTAMP '...'`). It handles comments and basic syntax validation.
* **`sql_ast.c/h`** & **`sql_query.c/h`**
    * **Purpose:** Parses the token array into an Abstract Syntax Tree (AST). `sql_query` handles the top-level structure (SELECT, FROM, JOIN, WHERE, CTEs), while `sql_ast` parses individual expressions (math, logical operators, function calls).
    * **Complexities:** Implements recursive descent parsing to handle operator precedence (e.g., `*` before `+`, `AND` before `OR`). It structures Common Table Expressions (CTEs), Subqueries, and Window clauses (`OVER (PARTITION BY...)`) into a purely textual tree (`sql_ast_node_t`) that has no awareness of the actual database schema yet.

### Semantic Analysis & Planning
These modules bridge the gap between abstract text and the physical database.

* **`sql_binder.c/h`**
    * **Purpose:** Resolves raw string identifiers (e.g., `users.id` or `id`) to physical database columns.
    * **Complexities:** Manages "query scope". It translates table aliases back to actual table names and asks the dynamic catalog (`sql_schema_lookup_cb`) if a column exists. It handles ambiguity checks (e.g., if two joined tables both have an `id` column) and injects the execution array index (`table_index`) so the VM knows exactly where to look for data in memory.
* **`sql_planner.c/h`**
    * **Purpose:** Generates a `sql_execution_plan_t` that dictates *how* the VM will fetch data.
    * **Complexities:** Performs filter pushdown (moving `WHERE` conditions directly into table scan requests so rows are filtered before joining). It evaluates join conditions to select algorithms (Hash Join for equality, Nested Loop otherwise). It also interfaces with `sql_index_t` to replace full table scans with B-Tree or Hash lookups when optimal.

### Compilation & Type System
The AST is string-based; the VM requires executable C-functions and rigid data types. These modules perform that translation.

* **`sql_ast_to_node.c`** & **`sql_node.c/h`**
    * **Purpose:** Converts the raw `sql_ast_node_t` tree into a strongly-typed `sql_node_t` execution tree.
    * **Complexities:** Infers data types (`SQL_TYPE_INT`, `SQL_TYPE_STRING`, etc.) and automatically injects implicit type conversions (CASTs) where needed (e.g., adding an INT to a DOUBLE). It resolves SQL function names to physical C-function pointers (`sql_node_cb`).
* **`sql_compiler.c/h`**
    * **Purpose:** Flattens the query into a `sql_compiled_query_t` containing arrays of projections, group-by keys, sort keys, and window function plans.
    * **Complexities:** * **Constant Folding:** Uses `sql_simplify_tree` to pre-evaluate static math/logical expressions at compile-time to save VM cycles.
        * **Aggregate Extraction:** Walks the tree to find aggregate functions (`SUM`, `COUNT`) and isolates them so the VM can step through them during the grouping phase.
        * **Pre-evaluating Subqueries:** If a subquery is a static scalar or `IN` list, it spawns a temporary VM, executes it, and replaces the subquery node with a literal result.

### Execution Engine (Virtual Machine)
The core data-processing powerhouse.

* **`sql_vm.c/h`**
    * **Purpose:** Executes the compiled plan against physical data.
    * **Complexities:** Implements a recursive query execution engine. It handles:
        * **Joins:** Nested loops and dynamic Hash Map generation for Hash Joins.
        * **Grouping:** Aggregates data using hash buckets (`group_node_t`) populated by evaluating the group-by keys.
        * **Window Functions:** Isolates window execution, sorts the subset of data based on partition/order keys, and finalizes window state (like `RANK()` or `ROW_NUMBER()`).
        * **Data Access:** Seamlessly iterates over both Materialized (in-memory arrays) and Streaming (cursor-based) `sql_dataset_t` structures.
* **`sql_result_set.c/h`**
    * **Purpose:** Stores the materialized output of the VM.
    * **Complexities:** Handles dynamic memory expansion as rows are appended. Manages the final global `ORDER BY` sorting, evaluating offset, and truncating via limit.

### State & Context Management
* **`sql_ctx.c/h`**
    * **Purpose:** The global state container for a query's lifecycle.
    * **Complexities:** Manages memory via arena allocators (`aml_pool_t`). It registers all standard SQL functions (math, string, dates) into a lookup table. Crucially, it manages "tracked pools"—isolated memory arenas used for subqueries that can be wiped clean to prevent memory leaks during heavy correlated subquery execution.
* **`sql_named_pointer.h`**
    * **Purpose:** A bi-directional hash map mapping SQL string identifiers (like "COALESCE") to physical C function pointers.

### Utilities
* **`sql_interval.c/h`**
    * **Purpose:** Parses complex time intervals (e.g., `INTERVAL 1 MONTH` or ISO-8601 strings) into a structured `sql_interval_t` for date math.
* **`date_utils.h`**
    * **Purpose:** Converts between human-readable strings, ISO-8601 UTC formats, and raw UNIX epoch timestamps used internally by the VM for `DATETIME` comparisons.

---

## 3. Key Inter-workings & Design Patterns

1. **Arena Memory Management (`aml_pool_t`):**
   The entire engine is built around memory arenas. Every token, AST node, VM node, and result string is allocated from a central pool attached to the `sql_ctx_t`. When a query finishes, the engine simply destroys the pool, achieving zero-overhead garbage collection and completely eliminating `free()` calls scattered throughout the code.

2. **The AST vs. VM Node Split:**
   The architecture strictly separates *Syntax* (`sql_ast_node_t`) from *Execution* (`sql_node_t`). The AST only knows that a user typed `age > 18`. The Compiler translates this into a VM Node that knows `age` is a `SQL_TYPE_INT` located at dataset index `0`, `18` is a literal integer, and the `>` operator maps to a specific C-function pointer for integer comparison.

3. **Dynamic Catalog Routing:**
   The engine does not hardcode table structures. Instead, `sql_vm_init` accepts callback functions (`fetch_table`, `resolve_column`). When the binder needs to know if `users.name` exists, it triggers `resolve_column`, allowing the engine to be embedded into any database backend, CSV reader, or JSON parser.

4. **CTE & Subquery Isolation:**
   When the VM encounters a subquery or a Common Table Expression (CTE), it recursively calls `internal_execute` to spawn a mini-VM. The result is wrapped into a "Virtual Dataset" (`ds->is_virtual = true`). To the outer query, this virtual dataset looks exactly like a physical table, allowing the exact same nested-loop and hash-join logic to process it transparently.

## 4. Standard Library & Function Specifications (Specs)

The `src/specs/`, `src/group_specs/`, and `src/window_specs/` directories constitute the standard library of the SQL engine. Rather than hardcoding operator logic into the compiler or VM, the engine uses a plugin architecture defined by the `sql_ctx_spec_t` interface.

### Purpose
These modules encapsulate the validation, type-coercion, and physical execution logic for every SQL keyword, operator, and function (e.g., `+`, `CONCAT`, `COALESCE`, `SUM`, `RANK`). They act as the definitive rulebook for how abstract operations behave at runtime.

The specs are divided into three functional categories:
* **Scalar Functions & Operators (`src/specs/`):** 1-to-1 mappings that evaluate a single row and return a single value (e.g., `LOWER`, `JSON_EXTRACT`, `BETWEEN`, `+`, `::`).
* **Aggregate Functions (`src/group_specs/`):** Many-to-1 mappings that accumulate state across multiple rows (e.g., `COUNT`, `AVG`).
* **Window Functions (`src/window_specs/`):** Many-to-Many mappings that evaluate rows within a partitioned window (e.g., `RANK`, `ROW_NUMBER`).

### Complexities

* **Dynamic Polymorphism in C:** SQL allows operators like `+` or functions like `MIN()` to accept various data types. Because C lacks native method overloading, the specs handle this manually during the `update` callback phase. For example, `arithmetic.c` inspects the AST node's children and dynamically assigns the `implementation` pointer to `sql_int_add`, `sql_double_add`, or `sql_datetime_int_add` based on the deduced types.
* **Implicit Type Coercion:** The specs are responsible for injecting safe type casting. If a user queries `INT_COL + 5.5`, the `update_arithmetic_spec` detects the mismatch, promotes the expected return type to `SQL_TYPE_DOUBLE`, and wraps the integer parameter in a `sql_convert` node before it ever reaches the VM.
* **Aggregate & Window State Lifecycles:** Stateful functions cannot simply execute and return. They must manage memory across thousands of rows. The specs handle this via a strict lifecycle:
    1.  `state_size`: Tells the VM exactly how many bytes to allocate in the hash map or partition block.
    2.  `agg_init`: Initializes the raw memory (e.g., setting a sum to `0.0`).
    3.  `agg_step`: Called per-row to mutate the state (e.g., adding the current row's value to the sum).
    4.  `agg_finalize`: Called at the end of the grouping phase to convert the raw C-struct state into a typed `sql_node_t` for the result set.
* **Constant Folding & Volatility:** The compiler aggressively optimizes the AST. If a function only contains literal parameters (e.g., `CONCAT('a', 'b')`), the compiler executes it immediately and replaces the node with a static literal. However, functions like `NOW()` or `RANK()` are flagged with `.is_volatile = true` inside their spec, strictly preventing the constant folder from collapsing them at compile time.

### Inter-workings

1.  **Registration:** During context initialization (`sql_register_ctx`), every spec is registered into the `sql_ctx_t`'s internal `macro_map_t` registry.
2.  **Compilation (`update` phase):** When the compiler (`sql_compiler.c`) converts an AST node into an executable VM node, it looks up the associated spec. It triggers the spec's `update` callback, passing the raw parameters. The spec validates parameter counts, coerces types, and returns a `sql_ctx_spec_update_t` containing the final execution pointer (`sql_node_cb`).
3.  **Execution (`func` phase):** During VM execution, the engine simply calls the resolved function pointer (e.g., `sql_string_concat`). For string and JSON operations, these callbacks heavily utilize the context's arena allocator (`aml_pool_t`) to ensure all intermediate string manipulations are automatically garbage collected when the query context is destroyed.
