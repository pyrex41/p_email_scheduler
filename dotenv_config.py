"""
Environment variable loading and configuration management.
Provides centralized loading of .env files and environment variables.
"""

import os
from typing import Any, Dict, Optional
from dotenv import load_dotenv

def load_env(env_file: str = ".env") -> None:
    """
    Load environment variables from .env file.
    
    Args:
        env_file: Path to the .env file. Defaults to ".env" in current directory.
    """
    # Load .env file if it exists
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), env_file)
    if os.path.isfile(env_path):
        load_dotenv(env_path)
        print(f"Loaded environment variables from {env_path}")
    else:
        print(f"No .env file found at {env_path}, using system environment variables")

def get_env(key: str, default: Optional[Any] = None) -> Any:
    """
    Get environment variable with default fallback.
    
    Args:
        key: Environment variable name
        default: Default value if environment variable is not set
        
    Returns:
        Environment variable value or default
    """
    return os.environ.get(key, default)

def get_bool_env(key: str, default: bool = False) -> bool:
    """
    Get boolean environment variable.
    
    Args:
        key: Environment variable name
        default: Default boolean value if not set
        
    Returns:
        Boolean value (True for "true", "yes", "1", "y", "t" case-insensitive)
    """
    value = get_env(key, str(default)).lower()
    return value in ("true", "yes", "1", "y", "t")

def get_int_env(key: str, default: int = 0) -> int:
    """
    Get integer environment variable.
    
    Args:
        key: Environment variable name
        default: Default integer value if not set
        
    Returns:
        Integer value or default if not a valid integer
    """
    try:
        return int(get_env(key, default))
    except (ValueError, TypeError):
        return default

def get_float_env(key: str, default: float = 0.0) -> float:
    """
    Get float environment variable.
    
    Args:
        key: Environment variable name
        default: Default float value if not set
        
    Returns:
        Float value or default if not a valid float
    """
    try:
        return float(get_env(key, default))
    except (ValueError, TypeError):
        return default

def get_email_config() -> Dict[str, Any]:
    """
    Get email configuration settings from environment variables.
    
    Returns:
        Dictionary with email configuration settings
    """
    return {
        "api_key": get_env("SENDGRID_API_KEY"),
        "dry_run": get_bool_env("EMAIL_DRY_RUN", True),
        "from_email": get_env("FROM_EMAIL", "medicare@example.com"),
        "from_name": get_env("FROM_NAME", "Medicare Services"),
        "test_email_sending": get_bool_env("TEST_EMAIL_SENDING", True),
        "production_email_sending": get_bool_env("PRODUCTION_EMAIL_SENDING", False),
    }

def get_app_config() -> Dict[str, Any]:
    """
    Get application configuration settings from environment variables.
    
    Returns:
        Dictionary with application configuration settings
    """
    return {
        "base_url": get_env("EMAIL_SCHEDULER_BASE_URL", "https://maxretain.com"),
        "quote_secret": get_env("QUOTE_SECRET", "your-default-secret-key"),
        "log_file": get_env("LOG_FILE", "logs/email_scheduler.log"),
    }

# Load environment variables when module is imported
load_env()