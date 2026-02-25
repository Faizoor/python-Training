from user import User


def dict_to_user(d: dict):
    try:
        return User(d['username'], d['email'], d['age'])
    except Exception:
        return None


if __name__ == '__main__':
    records = [
        {'username': 'alice', 'email': 'alice@example.com', 'age': 30},
        {'username': '', 'email': 'bad', 'age': -1},
        {'username': 'bob', 'email': 'bob@example.com', 'age': 25}
    ]

    users = []
    for r in records:
        u = dict_to_user(r)
        if u is None:
            print(f"Skipping invalid record: {r}")
        else:
            users.append(u)

    print('Valid users:')
    for u in users:
        print(u)
