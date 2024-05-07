"""
Representation of an insurance policy
"""


import datetime as dt
import random
from dataclasses import dataclass
from typing import Any, Optional

from faker import Faker
from faker.providers import misc, phone_number, user_agent
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String

from ippon.utils.fakable import FakableModel

fake = Faker()
fake.add_provider(phone_number)
fake.add_provider(user_agent)
fake.add_provider(misc)


@dataclass
class Policy(FakableModel):
    """A representation of an insurance policy"""

    policy_number: str
    insured_id: int
    start_date: dt.datetime
    end_date: dt.datetime
    policy_type: str
    policy_status: str
    policy_premium: int
    policy_deductible: int

    @classmethod
    def spoof_field(cls, field: str) -> Any:
        """Spoof ``field`` value"""

        if field == "policy_number":
            return fake.ssn()
        elif field == "insured_id":
            return random.randint(1, 100)
        elif field == "start_date":
            return fake.date_time_between(start_date="-1y", end_date="now")
        elif field == "end_date":
            return fake.date_time_between(start_date="now", end_date="+1y")
        elif field == "policy_type":
            return fake.random_element(elements=("auto", "home", "umbrella"))
        elif field == "policy_status":
            return fake.random_element(elements=("active", "inactive"))
        elif field == "policy_premium":
            return random.randint(100, 500)
        elif field == "policy_deductible":
            return random.randint(100, 500)
        else:
            raise NotImplementedError(
                f"Field {field} not implemented for {cls.__name__}"
            )
