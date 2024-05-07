""" Fakable Representation of a generic call detail record"""

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
class CallDetail(FakableModel):
    """Record of a call detail"""

    from_number: str
    to_number: str
    start_time: dt.time
    end_time: Optional[dt.time]
    duration: dt.timedelta
    billing_number: str
    telephone_exchange: str
    call_id: str
    ext: Optional[str]
    call_result: Optional[str]
    ingress_route: str
    egress_route: str
    call_type: str
    voice_call_type: str
    fault_condition: Optional[str]

    @classmethod
    def spoof(cls) -> "CallDetail":
        """Spoof an instance of this class"""
        return cls(
            from_number=fake.phone_number(),
            to_number=fake.phone_number(),
            start_time=fake.date_time_this_decade(),
            end_time=None,
            duration=fake.time_delta(),
            billing_number=fake.phone_number(),
            telephone_exchange=fake.user_agent(),
            call_id=fake.uuid4(cast_to=str),
            ext=fake.country_calling_code(),
            call_result=random.choice(
                (None, "success", "number disconnected", "voicemail", "no answer")
            ),
            ingress_route=fake.country_calling_code(),
            egress_route=fake.country_calling_code(),
            call_type=random.choice(("voice", "voip", "sms")),
            voice_call_type=random.choice(
                (
                    "call setup",
                    "call continue",
                    "call operation",
                    "call end",
                    "call idle",
                    "call busy",
                    "out of service call",
                )
            ),
            fault_condition=None,
        )


if __name__ == "__main__":
    instance = CallDetail.spoof_json(indent=2)
    print(instance)
