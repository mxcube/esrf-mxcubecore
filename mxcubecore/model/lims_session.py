from datetime import timedelta, datetime
from typing import Optional, Union, List
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


class Proposal(BaseModel):
    proposalId: str = ""
    personId: str = ""
    type: str = ""
    code: str = ""
    number: str = ""
    title: str = ""


class Person(BaseModel):
    person_id: str = ""
    site_id: Optional[str] = ""
    email_address: Optional[str] = ""
    family_name: Optional[str] = ""
    # faxNumber: str = ""
    given_name: Optional[str] = ""
    login: Optional[str] = ""
    # phoneNumber: str = ""
    title: Optional[str] = ""


class Status(BaseModel):
    code: str = "error"
    msg: Optional[str] = ""


class Session(BaseModel):
    session_id: str = ""
    proposal_id: str = ""
    proposal_name: str = ""
    beamline_name: str = ""
    comments: str = ""
    start_date: datetime = Field(default_factory=datetime.now)
    end_date: datetime = Field(
        default_factory=lambda: datetime.now() + timedelta(days=1)
    )
    nb_shifts: str = ""
    scheduled: str = ""
    comments: str = ""
    is_scheduled_time: bool = False
    is_scheduled_beamline: bool = False


class ProposalTuple(BaseModel):
    """
    Kept for legacy purposes and used by ISPyBClient
    It is a tuple containning information about Proposal, Person, Laboratory
    Sessions, Status
    """

    proposal: Proposal = None
    person: Person = None
    status: Status = None
    sessions: Optional[List[Session]] = []
    todays_session: Optional[Session] = None
