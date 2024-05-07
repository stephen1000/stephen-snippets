"""
Representation of a policy version
"""

import datetime as dt
import random
from dataclasses import dataclass
from typing import Optional

from faker import Faker
from faker.providers import misc, phone_number, user_agent
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String

from ippon.data.fake.fakable import FakableModel

fake = Faker()
fake.add_provider(phone_number)
fake.add_provider(user_agent)
fake.add_provider(misc)


@dataclass
class PolicyVersion(FakableModel):
    """
    Representation of a policy version
    """

    policy_number: str = Column(String, primary_key=True)
    version_number: int = Column(Integer, primary_key=True)
    insured_id: int = Column(Integer, ForeignKey("insured.id"))
    start_date: dt.datetime = Column(DateTime)
    end_date: dt.datetime = Column(DateTime)
    policy_type: str = Column(String)
    policy_status: str = Column(String)
    policy_premium: int = Column(Integer)
    policy_deductible: int = Column(Integer)

    @classmethod
    def spoof(cls) -> FakableModel:
        """Spoof a version of a policy"""

        return cls(
            policy_number=fake.ssn(),
            version_number=random.randint(1, 100),
            insured_id=random.randint(1, 100),
            start_date=fake.date_time_between(start_date="-1y", end_date="now"),
            end_date=fake.date_time_between(start_date="now", end_date="+1y"),
            policy_type=fake.random_element(elements=("auto", "home", "umbrella")),
            policy_status=fake.random_element(elements=("active", "inactive")),
            policy_premium=random.randint(100, 500),
            policy_deductible=random.randint(100, 500),
        )
