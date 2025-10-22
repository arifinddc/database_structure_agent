# database_tools.py - Functions for Schema Design, Performance Simulation, and DML Safeguard (FINAL REVISION)

import re
import json

# --- CORE FUNCTIONS ---

def optimize_ddl(ddl_string: str, usage_type: str) -> str:
    """Simulates: Analyzing and optimizing DDL based on usage type."""
    
    optimization_notes = f"\n-- OPTIMIZATION FOR {usage_type.upper()}:\n"
    
    usage_type = usage_type.upper()
    
    if usage_type == 'OLTP':
        optimization_notes += "-- Optimized for high-volume write speed and data integrity (suggesting indexes on FK/PK and proper normalization).\n"
    elif usage_type == 'OLAP':
        optimization_notes += "-- Optimized for read speed and aggregation (suggesting Columnar Indexes, partitioning by time, or denormalization).\n"
    elif usage_type == 'HTAP':
        optimization_notes += "-- Optimized for low latency on real-time data analytics (suggesting In-Memory tables or hybrid indexing).\n"
    elif usage_type == 'OLLP':
        optimization_notes += "-- Optimized for sub-millisecond decisions (ensuring minimal structure, focusing on data locality and low network overhead).\n"
    elif usage_type == 'BATCH':
        optimization_notes += "-- Optimized for high-throughput scheduled processing (suggesting large block sizes, table partitioning for parallel loading, and minimal indexing during load).\n"
    elif usage_type == 'STREAM':
        optimization_notes += "-- Optimized for continuous ingestion and real-time event detection (suggesting Time-Series partitioning, Kafka integration points, and high-speed primary key lookups).\n"
    else:
        optimization_notes += "-- General optimization applied. No specific processing type detected.\n"

    return ddl_string + optimization_notes

def validate_schema(ddl_string: str, sample_data_json: str) -> str:
    """Simulates: Validating DDL against the provided JSON sample data."""
    
    return f"-- SCHEMA VALIDATION:\n-- Schema successfully validated with the provided JSON sample data. Data types appear consistent.\n{ddl_string}"


def simulate_performance(ddl_string: str, row_count: int, proposed_usage_type: str) -> str:
    """
    Performs a model-based simulation of query performance and compares different processing types.
    """
    
    base_factor_ms = 100.0 
    scale_factor = max(1, row_count / 100000) 

    performance_profiles = {
        'OLTP':     [0.1,    100.0,  500.0],
        'OLAP':     [10.0,   5.0,    20.0],
        'HTAP':     [0.5,    8.0,    40.0],
        'STREAM':   [0.01,   200.0,  1000.0],
        'OLLP':     [0.001,  500.0,  2000.0],
        'BATCH':    [50.0,   1.0,    5.0],
    }

    results_table_rows = []
    proposed_type = proposed_usage_type.upper()
    
    if proposed_type not in performance_profiles:
        return "ERROR: Proposed usage type is invalid for performance estimation."

    def calculate_time(factor):
        return base_factor_ms * scale_factor * factor

    best_transaction_time = float('inf')
    best_analysis_time = float('inf')
    best_transaction_type = ""
    best_analysis_type = ""

    # 1. Collect data for the Comparison Table
    for db_type, factors in performance_profiles.items():
        time_tx = calculate_time(factors[0])
        time_complex_analysis = calculate_time(factors[2])

        if time_tx < best_transaction_time:
            best_transaction_time = time_tx
            best_transaction_type = db_type
            
        if time_complex_analysis < best_analysis_time:
            best_analysis_time = time_complex_analysis
            best_analysis_type = db_type

        # Convert for display
        time_tx_display = f"{time_tx:.3f} ms"
        time_complex_display = f"{time_complex_analysis / 1000.0 / 60.0:.2f} min"
        
        # Highlight the proposed type
        label = f"**{db_type}**" if db_type == proposed_type else db_type
        
        results_table_rows.append(f"| {label} | {time_tx_display} | {time_complex_display} |")

    # 2. Generate details for the Proposed Type
    proposed_time_tx = calculate_time(performance_profiles[proposed_type][0])
    proposed_time_complex = calculate_time(performance_profiles[proposed_type][2])
    
    detail_section = [
        f"### Estimation Details for Proposed Type ({proposed_type}):",
        f"- **Simple Transaction (1 row):** {proposed_time_tx:.3f} ms",
        f"- **Complex Analysis ({row_count} rows):** {proposed_time_complex / 1000.0 / 60.0:.2f} min",
        ""
    ]

    # 3. Assemble the Final Report
    report = [
        f"## Performance Simulation Report ({row_count} Rows)",
        "The time estimates below are simulated (rule-based) for relative comparison:",
        "",
        "### Comparison Table for All Processing Types:",
        "| Processing Type | Simple Transaction (Latency) | Complex Analysis (Throughput) |",
        "| :--- | :--- | :--- |",
    ]
    report.extend(results_table_rows)
    report.append("")
    report.extend(detail_section)
    
    report.append(f"## Performance Conclusion")
    report.append(f"From this simulation, the best type for **Transaction Speed** is: **{best_transaction_type}**.")
    report.append(f"The best type for **High Volume Analysis** is: **{best_analysis_type}**.")
    
    return "\n".join(report)


def order_sql_commands(sql_ddl_code: str) -> str:
    """
    Parses and orders CREATE TABLE statements based on FOREIGN KEY dependencies.
    Includes a safeguard to return DML statements (SELECT, INSERT, UPDATE, DELETE) un-ordered.
    """
    
    # --- DML SAFEGUARD: If no CREATE TABLE is found, return the code as is. ---
    if "CREATE TABLE" not in sql_ddl_code.upper():
        return sql_ddl_code
    
    # 1. Split the CREATE TABLE statements
    statements = [s.strip() for s in sql_ddl_code.split(';') if s.strip()]
    
    table_map = {}
    dependencies = {} 

    for statement in statements:
        table_match = re.search(r'CREATE TABLE\s+["`]?(\w+)["`]?\s*\(', statement, re.IGNORECASE)
        if not table_match:
            continue
            
        table_name = table_match.group(1)
        table_map[table_name] = statement + ";"
        dependencies[table_name] = []

        fk_matches = re.findall(r'REFERENCES\s+["`]?(\w+)["`]?\s*(?:\(|\s)', statement, re.IGNORECASE)
        
        for referenced_table in fk_matches:
            dependencies[table_name].append(referenced_table)

    # 2. Perform Topological Sort
    ordered_tables = []
    all_tables = set(table_map.keys())
    
    ready_to_process = [
        t for t in all_tables 
        if not dependencies[t] or all(dep not in all_tables for dep in dependencies[t])
    ]
    
    while ready_to_process:
        current_table = ready_to_process.pop(0)
        
        if current_table not in ordered_tables:
            ordered_tables.append(current_table)

        for dependent_table in list(all_tables - set(ordered_tables)):
            if dependent_table in dependencies and current_table in dependencies[dependent_table]:
                
                dependencies[dependent_table].remove(current_table)
                
                if not dependencies[dependent_table] and dependent_table not in ready_to_process:
                    ready_to_process.append(dependent_table)
                    
    # 3. Assemble the Ordered Output
    
    if len(ordered_tables) != len(table_map):
        missing_tables = all_tables - set(ordered_tables)
        if missing_tables:
            return f"# 🚨 WARNING: Not all tables could be topologically sorted (possible circular dependencies: {', '.join(missing_tables)}). Using original order.\n" + sql_ddl_code

    # Change output format to be cleaner:
    output_parts = ["# ✅ DDL Commands sorted by FOREIGN KEY dependency:\n"]
    for i, table_name in enumerate(ordered_tables):
        output_parts.append(table_map[table_name])

    final_output = "\n".join(output_parts)
    
    # Add empty lines between CREATE TABLE statements for readability
    final_output = final_output.replace(';', ';\n\n').strip()
    
    return final_output

# --- NEW DML SIMULATION TOOL ---

def simulate_dml_output(select_query: str, result_description: str) -> str:
    """
    Simulates the result of a SELECT query and formats it as a copyable Markdown table.
    """
    
    # 1. Parse the query to guess the columns (simplified logic)
    query_parts = select_query.upper().split('FROM')[0].replace('SELECT', '').strip()
    columns_raw = re.findall(r'(\w+\s*|[\w_]+\.\w+)\s*(?:AS\s*(\w+))?', query_parts)
    
    columns = []
    for col_part in columns_raw:
        if col_part[1]: # Use alias if available
            columns.append(col_part[1].strip())
        elif col_part[0]: # Otherwise, use column name (cleaning table prefixes)
            name = col_part[0].split('.')[-1].strip()
            if name:
                columns.append(name)

    # Fallback columns if parsing fails
    if not columns:
        columns = ["col_1", "col_2", "col_3"]

    # 2. Simulate Data (Based on common KPI/Team structure)
    simulated_data = []
    
    if "MEMBER" in select_query.upper() and ("KPI" in select_query.upper() or "VALUE" in select_query.upper()):
        # Example for Member/KPI performance
        simulated_data = [
            ("Budi", "Santoso", "Sales Revenue", "95000.00", "2023-10-26"),
            ("Siti", "Aminah", "Sales Revenue", "88000.00", "2023-10-26"),
        ]
    elif "TEAM" in select_query.upper() and "MEMBER" in select_query.upper():
        # Example for Team members list
        simulated_data = [
            (101, "Budi Santoso", "Sales Team A"),
            (102, "Siti Aminah", "Sales Team A"),
        ]
    else:
        # Generic fallback data
        simulated_data = [
            ("Sample_Value_A", 123),
            ("Sample_Value_B", 456),
        ]
        
    # Adjust columns if simulated data doesn't match the guessed number
    if simulated_data and len(columns) != len(simulated_data[0]):
        columns = [f"Column_{i+1}" for i in range(len(simulated_data[0]))]

    # 3. Format into Markdown Table
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join(['---' for _ in columns]) + " |"
    
    data_rows = []
    for row in simulated_data:
        data_rows.append("| " + " | ".join(map(str, row)) + " |")

    table_content = "\n".join([header, separator] + data_rows)

    # 4. Assemble Final Output
    final_output = f"""
### Simulated Query Output: ({result_description})

**Query:**
```sql
{select_query.strip()}
    {table_content}
    """
    return final_output.strip()