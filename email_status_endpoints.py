"""
API endpoints for checking email delivery status.
These can be integrated with app.py.
"""

from typing import Optional
from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel

from email_status_checker import (
    check_email_status,
    update_batch_statuses,
    get_batch_delivery_stats
)

# Create API router
router = APIRouter(prefix="/api/email-status", tags=["Email Status"])

# Define response models
class MessageStatusResponse(BaseModel):
    success: bool
    message_id: str
    status: Optional[str] = None
    error: Optional[str] = None
    data: Optional[dict] = None

class BatchUpdateResponse(BaseModel):
    success: bool
    org_id: int
    batch_id: Optional[str] = None
    stats: Optional[dict] = None
    error: Optional[str] = None

class BatchStatusResponse(BaseModel):
    success: bool
    org_id: int
    batch_id: str
    stats: Optional[dict] = None
    error: Optional[str] = None

# Define API endpoints
@router.get("/message/{message_id}", response_model=MessageStatusResponse)
async def get_message_status(request: Request, message_id: str):
    """
    Check the status of a specific email by its SendGrid message ID.
    """
    try:
        result = check_email_status(message_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/batch/update", response_model=BatchUpdateResponse)
async def update_batch_email_statuses(
    request: Request,
    org_id: int,
    batch_id: Optional[str] = None,
    limit: int = 100
):
    """
    Update delivery status of emails in a batch or all pending emails.
    """
    try:
        # Check if the batch_id is literally "update" and set it to None to prevent confusion
        if batch_id == "update":
            batch_id = None
            
        result = update_batch_statuses(org_id, batch_id, limit)
        
        # If no batch ID was specified, use a generic identifier in the response
        if batch_id is None:
            result["batch_id"] = "all_pending"
            
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/batch/{batch_id}", response_model=BatchStatusResponse)
async def get_batch_status(request: Request, org_id: int, batch_id: str):
    """
    Get delivery statistics for a batch.
    """
    try:
        result = get_batch_delivery_stats(org_id, batch_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Sample integration code for app.py:
"""
# Add this to app.py to integrate email status endpoints

from email_status_endpoints import router as status_router

app.include_router(status_router)
"""