"""
Utilities to patch MCP's message handling to handle protocol issues.
"""
import logging
import sys
import json
import re
import asyncio
from typing import Any, Dict, Optional, Callable

logger = logging.getLogger(__name__)

def fix_json_message(message):
    """
    Fix common JSON message issues:
    1. Remove 'text' prefix if present
    2. Handle other potential prefixes
    3. Fix malformed JSON
    """
    if not isinstance(message, str):
        if isinstance(message, bytes):
            try:
                message = message.decode('utf-8')
            except UnicodeDecodeError:
                logger.error("Failed to decode bytes to string")
                return message
        else:
            return message
            
    # Check for 'text' prefix and remove it
    if message.startswith('text'):
        logger.info("Removing 'text' prefix from message")
        message = message[4:].strip()
    
    # Check for other potential prefixes using regex
    prefix_match = re.match(r'^[a-z]+\s+(\{.*)', message)
    if prefix_match:
        logger.info(f"Detected potential prefix pattern - extracting JSON part")
        message = prefix_match.group(1)
    
    # Ensure the message is valid JSON
    try:
        json.loads(message)
        logger.debug("Message is valid JSON")
        return message
    except json.JSONDecodeError as e:
        logger.warning(f"Invalid JSON after basic fixes: {str(e)}")
        
        # Try more aggressive fixes
        try:
            # Check if there's any JSON-like string within the text
            json_pattern = re.search(r'(\{.*\})', message)
            if json_pattern:
                potential_json = json_pattern.group(1)
                # Try to parse it
                json.loads(potential_json)
                logger.info("Found valid JSON within message")
                return potential_json
        except (json.JSONDecodeError, AttributeError):
            pass
            
        # If we get here, we couldn't fix it
        logger.error(f"Could not fix malformed JSON: {message[:100]}")
        return message

# Function to patch MCP message handling
def patch_mcp_message_handling():
    """
    Patch MCP message handling to fix common protocol issues.
    This includes fixing 'text' prefixes in messages.
    """
    try:
        # Import MCP modules
        from mcp.server.fastmcp import FastMCP
        
        logger.info("Patching MCP message handling...")
        
        # Function to patch message sending
        def patch_send(original_send):
            async def patched_send(self, message):
                try:
                    # Debug the outgoing message
                    if isinstance(message, str):
                        # Fix any JSON issues
                        fixed_message = fix_json_message(message)
                        
                        # Only use the fixed message if it's valid JSON
                        try:
                            json.loads(fixed_message)
                            logger.info(f"Using fixed JSON for outgoing message")
                            message = fixed_message
                        except json.JSONDecodeError:
                            # If the fixed message isn't valid JSON, let's see if the original was
                            try:
                                json.loads(message)
                                logger.info("Original message is valid JSON - using it")
                            except json.JSONDecodeError:
                                logger.error("Both original and fixed messages are invalid JSON")
                    
                    # Send the possibly fixed message
                    return await original_send(self, message)
                except Exception as e:
                    logger.error(f"Error in patched_send: {str(e)}")
                    # Fall back to original method
                    return await original_send(self, message)
            
            return patched_send
        
        # Function to patch message receiving
        def patch_receive_message(original_receive):
            async def patched_receive(self, reader):
                try:
                    # First, let the original method run
                    message = await original_receive(self, reader)
                    
                    # Check and fix the received message
                    if message:
                        original_message = message
                        fixed_message = fix_json_message(message)
                        
                        if fixed_message != original_message:
                            logger.info("Fixed incoming message")
                            message = fixed_message
                    
                    return message
                except Exception as e:
                    logger.error(f"Error in patched_receive: {str(e)}")
                    # Fall back to original method
                    return await original_receive(self, reader)
            
            return patched_receive
            
        # Function to patch message handling
        def patch_handle_message(original_handle):
            async def patched_handle(self, message):
                try:
                    # Fix the message before handling
                    fixed_message = fix_json_message(message)
                    
                    # Only use the fixed message if it's different
                    if fixed_message != message:
                        logger.info("Using fixed message for handling")
                        message = fixed_message
                        
                    # Check if it's valid JSON
                    try:
                        if isinstance(message, str):
                            json.loads(message)
                            logger.debug("Message is valid JSON before handling")
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON before handling: {str(e)}")
                        
                    # Handle the message
                    return await original_handle(self, message)
                except Exception as e:
                    logger.error(f"Error in patched_handle: {str(e)}")
                    # Fall back to original method
                    return await original_handle(self, message)
            
            return patched_handle
        
        # Apply patches if possible
        patched = False
        
        try:
            if hasattr(FastMCP, 'send'):
                original_send = FastMCP.send
                FastMCP.send = patch_send(original_send)
                logger.info("Successfully patched FastMCP.send")
                patched = True
            else:
                logger.warning("FastMCP.send not found for patching")
                
            if hasattr(FastMCP, '_receive_message'):
                original_receive = FastMCP._receive_message
                FastMCP._receive_message = patch_receive_message(original_receive)
                logger.info("Successfully patched FastMCP._receive_message")
                patched = True
            else:
                logger.warning("FastMCP._receive_message not found for patching")
                
            if hasattr(FastMCP, '_handle_message'):
                original_handle = FastMCP._handle_message
                FastMCP._handle_message = patch_handle_message(original_handle)
                logger.info("Successfully patched FastMCP._handle_message")
                patched = True
            else:
                logger.warning("FastMCP._handle_message not found for patching")
                
            if patched:
                logger.info("MCP message handling successfully patched")
                return True
            else:
                logger.warning("No MCP methods were patched")
                return False
        except Exception as e:
            logger.error(f"Failed to patch MCP message handling: {str(e)}")
            return False
            
    except ImportError as e:
        logger.error(f"Failed to import MCP modules for patching: {str(e)}")
        return False

# Standalone testing function
def test_json_fixing():
    """Test the JSON fixing function with various inputs."""
    test_cases = [
        'text {"method": "test", "params": {}}',
        'data {"method": "test", "params": {}}',
        '{"method": "test", "params": {}}',
        'text\n{"method": "test", "params": {}}',
        'random text {"method": "test", "params": {}} more text',
        'invalid json',
    ]
    
    for i, test in enumerate(test_cases):
        logger.info(f"Test case {i+1}: {test}")
        fixed = fix_json_message(test)
        logger.info(f"Fixed: {fixed}")
        
        try:
            json.loads(fixed)
            logger.info("Result: Valid JSON")
        except json.JSONDecodeError:
            logger.info("Result: Invalid JSON")
        
        logger.info("-" * 50)

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Test the JSON fixing
    test_json_fixing()
