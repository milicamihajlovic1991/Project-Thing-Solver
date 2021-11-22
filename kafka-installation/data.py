__author__ = 'Milica Mihajlovic, milicamihajlovic1991@gmail.com'
__version__ = '1.0'
__desc__ = 'This script is used to create dummy data'

from faker import Faker
from random import randint
fake = Faker()

def get_user_data() -> dict():
    return {
        'user_id': randint(1, 5000),
        'user_name': fake.name(),
        'user_country': fake.country(),
        'user_email': fake.email(),
        'enrollment_year': fake.year()
    }


if __name__ == "__main__":
    print(get_user_data())
