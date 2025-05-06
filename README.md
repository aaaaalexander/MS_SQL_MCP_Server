# SQL MCP Server

SQL MCP Server is a tool that enables large language models (LLMs) to interact with SQL Server databases through a standardized API. It provides a structured way for AI systems to query, analyze, and explore database structures using natural language.

## 🚀 Claude Desktop Integration (Primary Use Case)

This tool is primarily designed to work with Claude Desktop, allowing Claude to seamlessly access and query your SQL Server databases.

### Claude Desktop Setup

1. Install Claude Desktop from [Anthropic](https://www.anthropic.com/)
2. **Create a configuration file**:
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

3. **Important configuration notes**:
   - Make sure to use absolute paths for both the Python executable and the script
   - Use escaped backslashes (`\\`) for Windows network paths
   - Ensure the user has appropriate SQL Server permissions
   - Adjust `DB_ALLOWED_SCHEMAS` to restrict access to specific database schemas

4. Restart Claude Desktop to apply the changes
5. Claude will now have access to all the SQL Server tools provided by this server

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

## API Documentation

The SQL MCP Server implements tools for database interaction including:

- **Schema exploration**: View tables, columns, relationships, and metadata
- **Query execution**: Run SQL SELECT queries with sanitization and validation
- **Data analysis**: Generate statistics, summaries, and explore data patterns
- **Advanced tooling**: Table relationship mapping, query building, and data export

## Security Considerations

- The server validates and sanitizes all SQL queries
- Read-only mode prevents data modification by default
- Schema restrictions limit access to specified database objects
- Connection pooling with timeout limits helps prevent resource exhaustion

## Troubleshooting

For connection issues or configuration help, refer to the `sql_connection_help.md` file.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- The SQLAlchemy team for their excellent database toolkit
- The Model Context Protocol (MCP) specification
- Anthropic for Claude Desktop integration support