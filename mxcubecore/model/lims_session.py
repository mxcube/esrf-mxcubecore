from datetime import datetime
from typing import Optional, Union
from pydantic import BaseModel, Field


class LIMSSession(BaseModel):
    """
    This model is the representation of the information about a experimental session
    """

    # Proposal code and number
    code: str = ""
    number: str = ""

    title: Optional[str] = None
    beamlineName: Optional[str] = None

    proposalId: int = None
    sessionId: int = None

    person: Optional[str] = None

    # status of the session depending on wether it has been rescheduled or moved
    isRescheduled: bool = False
    isScheduledBeamline: bool = False
    isScheduledTime: bool = False

    # Start and end dates officialy scheduled
    startDate: Optional[str] = None
    startTime: Optional[str] = None
    endDate: Optional[str] = None
    endTime: Optional[str] = None

    # If rescheduled the actual dates are used instead
    actualStartDate: Optional[str] = None
    actualStartTime: Optional[str] = None
    actualEndDate: Union[str, None] = None
    actualEndTime: Optional[str] = None

    # direct links to different services
    userPortalURL: Optional[str] = None
    dataPortalURL: Optional[str] = None
    logbookURL: Optional[str] = None
