"""
Representation of a claim
"""

import datetime as dt
import random
from dataclasses import dataclass
from typing import Any, Optional

from faker import Faker
from faker.providers import misc, phone_number, user_agent
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String

from ippon.data.fake.fakable import FakableModel, ForeignKey

fake = Faker()
fake.add_provider(phone_number)
fake.add_provider(user_agent)
fake.add_provider(misc)


@dataclass
class Claim(FakableModel):
    """A representation of a claim"""

    claim_number: str
    policy_number: str
    claim_date: dt.datetime
    claim_type: str
    claim_status: str
    claim_amount: int

    @classmethod
    def spoof_field(cls, field: str) -> Any:
        """Spoof ``field`` value"""

        if field == "claim_number":
            return fake.ssn()
        elif field == "policy_number":
            return ForeignKey("insurance.Policy", "policy_number").spoof()
        elif field == "claim_date":
            return fake.date_time_between(start_date="-1y", end_date="now")
        elif field == "claim_type":
            return fake.random_element(elements=("auto", "home", "umbrella"))
        elif field == "claim_status":
            return fake.random_element(elements=("active", "inactive"))
        elif field == "claim_amount":
            return random.randint(100, 500)
        else:
            raise NotImplementedError(
                f"Field {field} not implemented for {cls.__name__}"
            )
