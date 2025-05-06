/**
 * Fixed MCP Protocol Proxy for Claude Desktop
 * 
 * This script acts as a bridge between Claude Desktop and the Python MCP server,
 * with robust filtering to ensure only valid JSON is sent to Claude Desktop.
 */

const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

// Log to file for debugging
const logFile = fs.createWriteStream(path.join(__dirname, 'proxy.log'), { flags: 'a' });
function log(message) {
  const timestamp = new Date().toISOString();
  const logMessage = `${timestamp} - ${message}\n`;
  logFile.write(logMessage);
  console.error(logMessage); // Also output to stderr for Claude Desktop logs
}

log('Starting DB_USER proxy with strict JSON validation');

// Configuration
const pythonPath = path.join(__dirname, '.venv', 'Scripts', 'python.exe');
const scriptPath = path.join(__dirname, 'sql_mcp_server.py');

// Environment variables to pass to the child process
const env = {
  ...process.env,
  DB_USER_DB_USERNAME: process.env.DB_USER_DB_USERNAME || 'DB_USER',
  DB_USER_DB_PASSWORD: process.env.DB_USER_DB_PASSWORD || 'YOUR_PASSWORD_HERE',
  DB_USER_LOG_LEVEL: 'INFO'
};

// Start the Python process
log(`Launching Python script: ${scriptPath}`);
const pythonProcess = spawn(pythonPath, [scriptPath], {
  env,
  stdio: ['pipe', 'pipe', 'pipe']
});

// Handle Python process events
pythonProcess.on('error', (err) => {
  log(`Failed to start Python process: ${err.message}`);
  process.exit(1);
});

pythonProcess.on('exit', (code, signal) => {
  log(`Python process exited with code ${code} and signal ${signal}`);
  process.exit(code || 0);
});

// Log stderr from Python for debugging
pythonProcess.stderr.on('data', (data) => {
  log(`Python stderr: ${data}`);
});

// Process stdin from Claude Desktop before passing to Python
process.stdin.on('data', (data) => {
  try {
    const message = data.toString();
    log(`Received from Claude: ${message.substring(0, 100)}${message.length > 100 ? '...' : ''}`);
    
    // Pass the message to Python
    pythonProcess.stdin.write(data);
  } catch (err) {
    log(`Error processing input: ${err.message}`);
    // Pass through the original data if processing fails
    pythonProcess.stdin.write(data);
  }
});

// This buffer will accumulate data until we have a complete JSON object
let outputBuffer = '';

// Process Python stdout before passing to Claude Desktop
pythonProcess.stdout.on('data', (data) => {
  try {
    // Convert buffer to string and add to our accumulated buffer
    const message = data.toString();
    log(`Raw output from Python: ${message.substring(0, 100)}${message.length > 100 ? '...' : ''}`);
    
    // With the fixed Python script, we should now only receive valid JSON-RPC messages
    // Directly try parsing and validating the message
    try {
      const jsonRpc = JSON.parse(message);
      
      // Verify it's a valid JSON-RPC message
      if (jsonRpc.jsonrpc === '2.0') {
        log(`Valid JSON-RPC message received from Python: id=${jsonRpc.id}`);
        
        // Forward the message directly to Claude Desktop
        process.stdout.write(message);
        return;
      } else {
        log(`Received JSON, but not valid JSON-RPC: ${message.substring(0, 100)}...`);
      }
    } catch (jsonError) {
      // Not a direct valid JSON message, accumulate in buffer
      outputBuffer += message;
      
      // Try to find and extract a complete JSON object from the buffer
      const jsonObject = extractJsonObject(outputBuffer);
      
      if (jsonObject) {
        log(`Found valid JSON object (length: ${jsonObject.length}), sending to Claude`);
        process.stdout.write(jsonObject);
        
        // Remove the extracted JSON from the buffer
        outputBuffer = outputBuffer.substring(outputBuffer.indexOf(jsonObject) + jsonObject.length);
      } else {
        // Prevent buffer from growing too large if no valid JSON is found
        if (outputBuffer.length > 10000) {
          log('Buffer too large with no valid JSON found, clearing buffer');
          outputBuffer = '';
        }
      }
    }
  } catch (err) {
    log(`Error processing output: ${err.message}`);
    // Reset buffer on error
    outputBuffer = '';
  }
});

/**
 * Extracts a complete JSON object from a string that might contain other content
 * @param {string} text - The text to extract JSON from
 * @returns {string|null} - The extracted JSON object as a string, or null if none found
 */
function extractJsonObject(text) {
  try {
    // First, try to parse the entire buffer as JSON
    JSON.parse(text);
    return text; // If no error, the entire buffer is valid JSON
  } catch (e) {
    // If that fails, look for a JSON object
    try {
      // Find the start of a JSON object
      const startIdx = text.indexOf('{');
      if (startIdx === -1) return null;
      
      // Extract everything from the start marker to the end
      const jsonCandidate = text.substring(startIdx);
      
      // Now find where the JSON object ends by trying progressive substrings
      for (let i = jsonCandidate.length; i > 0; i--) {
        const substring = jsonCandidate.substring(0, i);
        try {
          JSON.parse(substring);
          return substring; // Found a valid JSON object
        } catch (e) {
          // This substring isn't valid JSON, try a shorter one
          continue;
        }
      }
    } catch (e) {
      log(`Error trying to extract JSON: ${e.message}`);
    }
  }
  
  return null; // No valid JSON found
}

// Handle process exit
process.on('exit', () => {
  log('Proxy process exiting');
  logFile.end();
});

// Handle SIGINT (Ctrl+C)
process.on('SIGINT', () => {
  log('Received SIGINT, shutting down');
  pythonProcess.kill();
  process.exit(0);
});

log('Proxy initialized and ready');
