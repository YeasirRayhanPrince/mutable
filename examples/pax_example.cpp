#include <mutable/mutable.hpp>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/util/Diagnostic.hpp>
#include <mutable/storage/DataLayout.hpp>
#include <mutable/catalog/Type.hpp>
#include "PerfEvent.hpp"
#include <iostream>

using namespace m;


// Custom PAX-in-PAX layout factory for employees table
storage::DataLayout create_custom_pax_layout() {
    const std::size_t num_tuples = 1000; // Assume up to 1000 tuples for this example

    // Create the main DataLayout
    storage::DataLayout layout(num_tuples);

    // Create the outer PAX block (holds 256 tuples)
    auto &outer_pax = layout.add_inode(256, 64 * 1024 * 8); // 64KB blocks

    // GROUP 1: ids (attribute index 0)
    // PAX block for just the id column
    auto &ids_pax = outer_pax.add_inode(256, 0, 256 * 4 * 8); // 256 tuples * 4 bytes * 8 bits
    ids_pax.add_leaf(
        Type::Get_Integer(Type::TY_Vector, 4), // INT(4)
        0,      // attribute index for 'id'
        0,      // offset within this block
        32      // stride: 4 bytes = 32 bits
    );

    // GROUP 2: name, is_manager (attribute indices 1, 4)
    // PAX block for name and boolean columns
    auto &name_mgr_pax = outer_pax.add_inode(256, 256 * 4 * 8, 256 * (50 + 1) * 8); // names + bools
    name_mgr_pax.add_leaf(
        Type::Get_Char(Type::TY_Vector, 50),  // VARCHAR(50) - still uses Char type internally
        1,      // attribute index for 'name'
        0,      // offset within this block
        400     // stride: 50 bytes = 400 bits
    );
    name_mgr_pax.add_leaf(
        Type::Get_Boolean(Type::TY_Vector),   // BOOL
        4,      // attribute index for 'is_manager'
        256 * 400,  // offset after all names
        8       // stride: 1 bit, but rounded to 8 for byte alignment
    );

    // GROUP 3: age, salary (attribute indices 2, 3)
    // PAX block for numeric columns
    auto &nums_pax = outer_pax.add_inode(256, 256 * (4 + 50 + 1) * 8, 256 * (4 + 8) * 8); // age + salary
    nums_pax.add_leaf(
        Type::Get_Integer(Type::TY_Vector, 4), // INT(4) for age
        2,      // attribute index for 'age'
        0,      // offset within this block
        32      // stride: 4 bytes = 32 bits
    );
    nums_pax.add_leaf(
        Type::Get_Double(Type::TY_Vector),     // DOUBLE for salary
        3,      // attribute index for 'salary'
        256 * 32,   // offset after all ages
        64      // stride: 8 bytes = 64 bits
    );

    return layout;
}

int main() {
    // Initialize mutable
    if (!m::init()) {
        std::cerr << "Failed to initialize mutable" << std::endl;
        return 1;
    }

    Diagnostic diag(false, std::cout, std::cerr); // Create diagnostic handler
    auto &C = Catalog::Get();

    try {
        // Create a database
        auto create_db_cmd = command_from_string(diag, "CREATE DATABASE testdb;");
        if (create_db_cmd) create_db_cmd->execute(diag);

        // Use the database
        auto use_db_cmd = command_from_string(diag, "USE testdb;");
        if (use_db_cmd) use_db_cmd->execute(diag);

        // Create a table with various column types
        std::string create_table_sql = R"(
            CREATE TABLE employees (
                id INT(4) PRIMARY KEY,
                name VARCHAR(50),
                age INT(4),
                salary DOUBLE,
                is_manager BOOL
            );
        )";
        auto create_table_cmd = command_from_string(diag, create_table_sql);
        if (create_table_cmd) create_table_cmd->execute(diag);

        // Apply our custom PAX-in-PAX layout to the table
        auto &db = C.get_database_in_use();
        auto &table = db.get_table(C.pool("employees"));

        // Create and set custom layout
        auto custom_layout = create_custom_pax_layout();
        table.layout(std::move(custom_layout));

        // Create new store with custom layout
        table.store(C.create_store(table));

        std::cout << "Applied custom PAX-in-PAX layout: [ids] | [name, is_manager] | [age, salary]\n";

        // Insert some sample data
        std::vector<std::string> insert_queries = {
            "INSERT INTO employees VALUES (1, \"Alice\", 30, 75000.50, TRUE);",
            "INSERT INTO employees VALUES (2, \"Bob\", 25, 65000.00, FALSE);",
            "INSERT INTO employees VALUES (3, \"Charlie\", 35, 85000.75, TRUE);",
            "INSERT INTO employees VALUES (4, \"Diana\", 28, 70000.25, FALSE);",
            "INSERT INTO employees VALUES (5, \"Eve\", 32, 90000.00, TRUE);"
        };

        for (const auto& insert_sql : insert_queries) {
            auto insert_cmd = command_from_string(diag, insert_sql);
            if (insert_cmd) insert_cmd->execute(diag);
        }

        std::cout << "\n" << std::string(80, '=') << "\n";
        std::cout << "PERFORMANCE ANALYSIS: PAX-IN-PAX LAYOUT\n";
        std::cout << "Layout: [ids] | [name, is_manager] | [age, salary]\n";
        std::cout << std::string(80, '=') << "\n";

        // GROUP 1 QUERIES: Testing id-only access (should have excellent cache locality)
        std::cout << "\n🔍 GROUP 1 QUERIES - Accessing [ids] only:\n";
        std::cout << "Expected: Low cache misses due to tight packing of id column\n\n";

        {
            std::cout << "Query G1-1: COUNT employees with id > 2\n";
            BenchmarkParameters params("G1_count_ids");
            PerfEventBlock perf(1, params, true);
            auto stmt = statement_from_string(diag, "SELECT COUNT(*) FROM employees WHERE id > 2;");
            if (stmt) execute_statement(diag, *stmt);
        }

        {
            std::cout << "\nQuery G1-2: SELECT ids only\n";
            BenchmarkParameters params("G1_select_ids");
            PerfEventBlock perf(1, params, false);
            auto stmt = statement_from_string(diag, "SELECT id FROM employees;");
            if (stmt) execute_statement(diag, *stmt);
        }

        // GROUP 2 QUERIES: Testing name + is_manager access
        std::cout << "\n🔍 GROUP 2 QUERIES - Accessing [name, is_manager] only:\n";
        std::cout << "Expected: Good cache locality as these columns are co-located\n\n";

        {
            std::cout << "Query G2-1: SELECT managers by name\n";
            BenchmarkParameters params("G2_managers");
            PerfEventBlock perf(1, params, false);
            auto stmt = statement_from_string(diag, "SELECT name FROM employees WHERE is_manager = TRUE;");
            if (stmt) execute_statement(diag, *stmt);
        }

        {
            std::cout << "\nQuery G2-2: COUNT non-managers\n";
            BenchmarkParameters params("G2_non_managers");
            PerfEventBlock perf(1, params, false);
            auto stmt = statement_from_string(diag, "SELECT COUNT(*) FROM employees WHERE is_manager = FALSE;");
            if (stmt) execute_statement(diag, *stmt);
        }

        // GROUP 3 QUERIES: Testing age + salary access
        std::cout << "\n🔍 GROUP 3 QUERIES - Accessing [age, salary] only:\n";
        std::cout << "Expected: Good cache locality for numerical computations\n\n";

        {
            std::cout << "Query G3-1: Average salary for age > 30\n";
            BenchmarkParameters params("G3_avg_salary");
            PerfEventBlock perf(1, params, false);
            auto stmt = statement_from_string(diag, "SELECT AVG(salary) FROM employees WHERE age > 30;");
            if (stmt) execute_statement(diag, *stmt);
        }

        {
            std::cout << "\nQuery G3-2: SELECT age and salary\n";
            BenchmarkParameters params("G3_age_salary");
            PerfEventBlock perf(1, params, false);
            auto stmt = statement_from_string(diag, "SELECT age, salary FROM employees;");
            if (stmt) execute_statement(diag, *stmt);
        }

        // CROSS-GROUP QUERIES: Testing access across different PAX groups
        std::cout << "\n🔍 CROSS-GROUP QUERIES - Accessing multiple groups:\n";
        std::cout << "Expected: Higher cache misses due to data from different PAX blocks\n\n";

        {
            std::cout << "Query CG-1: Cross all groups (id, name, age, salary)\n";
            BenchmarkParameters params("CG_all_groups");
            PerfEventBlock perf(1, params, false);
            auto stmt = statement_from_string(diag, "SELECT id, name, age, salary FROM employees WHERE age > 25;");
            if (stmt) execute_statement(diag, *stmt);
        }

        {
            std::cout << "\nQuery CG-2: Groups 1&3 (id, salary) - non-adjacent groups\n";
            BenchmarkParameters params("CG_id_salary");
            PerfEventBlock perf(1, params, false);
            auto stmt = statement_from_string(diag, "SELECT id, salary FROM employees WHERE salary > 70000;");
            if (stmt) execute_statement(diag, *stmt);
        }

        {
            std::cout << "\nQuery CG-3: Complex cross-group with filtering\n";
            BenchmarkParameters params("CG_complex");
            PerfEventBlock perf(1, params, false);
            auto stmt = statement_from_string(diag, "SELECT name, salary FROM employees WHERE id > 2 AND age < 35 AND is_manager = TRUE;");
            if (stmt) execute_statement(diag, *stmt);
        }

    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    if (diag.num_errors() > 0) {
        std::cerr << "Errors occurred during execution" << std::endl;
        return 1;
    }

    return 0;
}