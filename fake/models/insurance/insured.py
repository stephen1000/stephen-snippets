"""
Representation of an insured.
"""

import datetime as dt
import random
from dataclasses import dataclass
from typing import Optional

from faker import Faker
from faker.providers import misc, phone_number, user_agent

from ippon.utils.fakable import FakableModel

fake = Faker()
fake.add_provider(phone_number)
fake.add_provider(user_agent)
fake.add_provider(misc)


@dataclass
class Insured(FakableModel):
    """An insured person"""

    id: str
    first_name: str
    last_name: str
    email: str
    phone: str
    address: str
    city: str
    state: str
    zip_code: str
    date_of_birth: dt.datetime

    @classmethod
    def spoof_field(cls, field: str) -> Optional[str]:
        """Spoof ``field`` value"""

        if field == "id":
            return fake.uuid4()
        elif field == "first_name":
            return fake.first_name()
        elif field == "last_name":
            return fake.last_name()
        elif field == "email":
            return fake.email()
        elif field == "phone":
            return fake.phone_number()
        elif field == "address":
            return fake.street_address()
        elif field == "city":
            return fake.city()
        elif field == "state":
            return fake.state_abbr()
        elif field == "zip_code":
            return fake.zipcode()
        elif field == "date_of_birth":
            return fake.date_time_between(start_date="-100y", end_date="-18y")
        else:
            raise NotImplementedError(
                f"Field {field} not implemented for {cls.__name__}"
            )
