
def generate_link(org_id: int, contact_id: str, email_type: str, email_date: str) -> str:
    """
    Generate a tracking link for the email using quote ID system
    
    Args:
        org_id: Organization ID
        contact_id: Contact ID
        email_type: Type of email (birthday, effective_date, aep, post_window)
        email_date: Scheduled date for the email
        
    Returns:
        Generated URL for tracking
    """
    import hashlib
    import os

    # Convert contact_id to int for quote ID generation
    contact_id_int = int(contact_id)
    
    # Get quote secret from environment with default fallback
    quote_secret = os.environ.get('QUOTE_SECRET', 'your-default-secret-key')
    
    # Create data string to hash - EXACTLY matching TypeScript implementation
    # Convert numbers to strings first to ensure exact string concatenation
    org_id_str = str(org_id)
    contact_id_str = str(contact_id_int)
    data_to_hash = f"{org_id_str}-{contact_id_str}-{quote_secret}"
    
    # Generate hash using hashlib - encode as UTF-8 to match Node.js behavior
    hash_value = hashlib.sha256(data_to_hash.encode('utf-8')).hexdigest()[:8]
    
    # Combine components into quote ID
    quote_id = f"{org_id}-{contact_id_int}-{hash_value}"
    
    # Get base URL from environment or use default
    base_url = os.environ.get('EMAIL_SCHEDULER_BASE_URL', 'https://maxretain.com')
    
    # Ensure quote ID is properly URL encoded
    from urllib.parse import quote
    quote_id_enc = quote(quote_id)
    
    # Construct tracking URL with quote ID
    return f"{base_url.rstrip('/')}/compare?id={quote_id_enc}"