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
    personId: str = ""
    laboratoryId: Optional[str] = ""
    siteId: Optional[str] = ""
    emailAddress: Optional[str] = ""
    familyName: Optional[str] = ""
    # faxNumber: str = ""
    givenName: Optional[str] = ""
    login: Optional[str] = ""
    # phoneNumber: str = ""
    title: Optional[str] = ""


class Status(BaseModel):
    code: str = "error"
    msg: Optional[str] = ""


class Laboratoty(BaseModel):
    laboratoryId: str = ""
    address: Optional[str] = ""
    city: Optional[str] = ""
    country: Optional[str] = ""
    laboratoryExtPk: Optional[str] = ""
    name: Optional[str] = ""


class Session(BaseModel):
    sessionId: str = ""
    proposalId: str = ""
    proposalName: str = ""
    beamlineName: str = ""
    comments: str = ""
    startDate: datetime = Field(default_factory=datetime.now)
    endDate: datetime = Field(
        default_factory=lambda: datetime.now() + timedelta(days=1)
    )
    nbShifts: str = ""
    scheduled: str = ""
    comments: str = ""


class ProposalTuple(BaseModel):
    """
    Kept for legacy purposes and used by ISPyBClient
    It is a tuple containning information about Proposal, Person, Laboratory
    Sessions, Status
    """

    proposal: Proposal = None
    person: Person = None
    laboratory: Laboratoty = None
    status: Status = None
    sessions: Optional[List[Session]] = []
    todays_session: Optional[Session] = None
