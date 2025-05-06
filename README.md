# Microsoft SQL MCP Server

A robust SQL Server interface using the Model Context Protocol (MCP) standard, providing AI assistants with a controlled way to interact with SQL databases. This project offers tools for schema exploration, query execution, and data analysis, with safety mechanisms to prevent harmful operations.

## 🚀 Claude Desktop Integration (Primary Use Case)

This tool is primarily designed to work with Claude Desktop, allowing Claude to seamlessly access and query your SQL Server databases.

### Claude Desktop Setup

1. **Install Prerequisites**:
   - Python 3.8+ and SQL Server (2016+ recommended)
   - [Microsoft ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server) (17+ recommended)

2. **Create a Dedicated SQL User (Recommended)**:
   - Create a dedicated SQL Server user with read-only permissions
   - Never use sa/admin accounts or grant administrative privileges
   - Ensure the user has SELECT permissions on required tables
   - Restrict access to only necessary schemas
   - Example SQL for creating a dedicated user:
     ```sql
     -- Create login
     CREATE LOGIN SQLMCP WITH PASSWORD = 'YourStrongPassword';
     
     -- Switch to your database
     USE YourDatabaseName;
     
     -- Create user and grant permissions
     CREATE USER SQLMCP FOR LOGIN SQLMCP;
     GRANT SELECT TO SQLMCP;
     
     -- Optionally restrict to specific schemas
     GRANT SELECT ON SCHEMA::dbo TO SQLMCP;
     ```

3. **Install This Repository**:
   ```bash
   git clone https://github.com/aaaaalexander/MS_SQL_MCP_server.git
   cd MS_SQL_MCP_server
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   ```

4. **Create Configuration File**:
   - Create a file named `claude_config.json` in your Claude Desktop configuration folder
   - Use the structure below, replacing paths and credentials with your actual values:

```json
{
  "sqlmcp": {
    "command": "C:/path/to/your/venv/Scripts/python.exe",
    "args": [
      "C:/path/to/your/project/sql_mcp_server.py"
    ],
    "env": {
      "PYTHONUNBUFFERED": "1",
      "DB_SERVER": "\\\\SERVER\\INSTANCE",
      "DB_NAME": "DATABASENAME",
      "DB_USERNAME": "username",
      "DB_PASSWORD": "password",
      "DB_ALLOWED_SCHEMAS": "[\"dbo\"]",
      "DB_DEBUG": "true",
      "DB_LOG_LEVEL": "DEBUG"
    }
  }
}
```

5. **Important Configuration Notes**:
   - Use absolute paths for both the Python executable and the script
   - Use escaped backslashes (`\\`) for Windows network paths
   - Ensure proper SQL Server authentication details
   - Adjust `DB_ALLOWED_SCHEMAS` to restrict access to specific database schemas

6. Restart Claude Desktop to apply the changes
7. Claude will now have access to all the SQL Server tools provided by this server

### Using the SQL Tools in Claude

Once properly configured, you can ask Claude to:
- Explore database schemas
- Run SQL queries
- Analyze data
- Generate database reports
- Create data visualizations from SQL data

Example prompt: "Show me the structure of the Customers table and give me a count of customers by country."

## Features

- 🔒 **Secure Database Access**: Connect to Microsoft SQL Server databases with configurable security restrictions
- 🛠️ **Rich Tool Set**: Various database interaction tools for queries, schema exploration, and analysis 
- 📊 **Data Visualization Support**: Ability to export and analyze data in various formats
- 🧠 **AI-Ready Interface**: Implements the Model Context Protocol (MCP) for LLM integration
- ⚡ **Optimized Performance**: Connection pooling and efficiency optimizations for high throughput

## Alternative Installation (Standalone Mode)

If you're not using Claude Desktop, you can still run the server in standalone mode:

1. Clone this repository
2. Create a virtual environment: `python -m venv .venv`
3. Activate the virtual environment:
   - Windows: `.venv\Scripts\activate`
   - Linux/Mac: `source .venv/bin/activate`
4. Install dependencies: `pip install -r requirements.txt`
5. Copy `.env.example` to `.env` and configure your database settings
6. Run the server using the provided batch file: `run_sql_mcp.bat`

## Configuration

Edit the `.env` file with your SQL Server connection details:

```
DB_SERVER=your_server_name
DB_NAME=your_database_name
DB_USERNAME=your_username
DB_PASSWORD=your_password
DB_ALLOWED_SCHEMAS=["dbo"]
```

### Environment Variables Reference

The server supports three variable prefix formats for backward compatibility:

- **DB_** - The recommended standard prefix (e.g., `DB_SERVER`)  
- **SQLMCP_** - Transitional prefix (e.g., `SQLMCP_DB_SERVER`)  
- **DB_USER_** - Legacy prefix (e.g., `DB_USER_DB_SERVER`)

If multiple prefixes are defined for the same setting, the priority order is:
1. `DB_` (highest priority)
2. `SQLMCP_` (medium priority)
3. `DB_USER_` (lowest priority)

## Available Tools

### Schema Tools
- `list_tables`: List tables/views in allowed schemas
- `get_table_schema`: Get schema (columns, FKs) for a table
- `list_schemas`: List all available schemas
- `find_foreign_keys`: Find foreign key relationships
- `search_schema_objects`: Search for database objects by name
- `find_related_tables`: Find tables related to a specific table
- `get_query_examples`: Generate example queries for a table

### Query Tools
- `execute_select`: Execute a safe SELECT query with parameters
- `query_table`: Query tables with simplified filtering and sorting
- `get_sample_data`: Retrieve sample data from tables

### Analysis Tools
- `analyze_table_data`: Analyze column distributions and statistics
- `find_duplicate_records`: Find potential duplicate records
- `summarize_data`: Generate data summaries with grouping and metrics

### Metadata Tools
- `get_database_info`: Get server and database information
- `list_stored_procedures`: List available stored procedures
- `get_procedure_definition`: Get stored procedure code and parameters

## Security Considerations

- The server validates and sanitizes all SQL queries
- Read-only mode prevents data modification by default
- Schema restrictions limit access to specified database objects
- Connection pooling with timeout limits helps prevent resource exhaustion
- **Create a dedicated SQL user** with read-only permissions specifically for this service
- **Never run as sa/admin** or with administrative database privileges
- **Keep credentials secure** and never commit them to version control
- **Run behind a firewall** to prevent public exposure of the MCP endpoint

## Troubleshooting

For connection issues or configuration help, refer to the `sql_connection_help.md` file.

Common issues to check:
- Verify SQL Server is running and accessible
- Check firewall settings for port 1433 (default SQL Server port)
- Confirm credentials are correct in your configuration
- Ensure the SQL user has appropriate permissions on the tables/schemas
- Verify the ODBC driver is properly installed

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- The SQLAlchemy team for their excellent database toolkit
- The Model Context Protocol (MCP) specification
- Anthropic for Claude Desktop integration support