from datetime import timedelta, datetime
from typing import Optional, List
from pydantic import BaseModel, Field


class Proposal(BaseModel):
    proposal_id: str = ""
    person_id: str = ""
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
    beamline_name: str = ""
    start_date: str = ""
    start_time: str = ""
    end_date: str = ""
    end_time: str = ""

    # Proposal information
    title: str = ""
    code: str = ""
    number: str = ""
    proposal_id: str = ""
    proposal_name: str = ""

    comments: str = ""

    start_datetime: datetime = Field(default_factory=datetime.now)
    end_datetime: datetime = Field(
        default_factory=lambda: datetime.now() + timedelta(days=1)
    )

    # If rescheduled the actual dates are used instead
    actual_start_date: str = ""
    actual_start_time: str = ""
    actual_end_date: str = ""
    actual_end_time: str = ""

    nb_shifts: str = ""
    scheduled: str = ""

    # status of the session depending on wether it has been rescheduled or moved
    isRescheduled: bool = False
    is_scheduled_time: bool = False
    is_scheduled_beamline: bool = False

    # direct links to different services
    user_portal_URL: Optional[str] = None
    data_portal_URL: Optional[str] = None
    logbook_URL: Optional[str] = None


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
