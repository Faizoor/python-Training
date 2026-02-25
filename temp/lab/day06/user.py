import re

class User:
    def __init__(self, username: str, email: str, age: int):
        if not isinstance(username, str) or not username:
            raise ValueError('username must be a non-empty string')
        if not isinstance(email, str) or not re.match(r"[^@]+@[^@]+\.[^@]+", email):
            raise ValueError('invalid email')
        if not isinstance(age, int) or age < 0:
            raise ValueError('age must be a non-negative integer')
        self.username = username
        self.email = email
        self.age = age

    def __repr__(self):
        return f"User(username={self.username!r}, email={self.email!r}, age={self.age!r})"
