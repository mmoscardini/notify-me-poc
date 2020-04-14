from faker import Faker

def generate_listing_events(number_of_listing_events, conditions):
    fake = Faker()
    listing_events = []

    num_of_conditions = len(conditions[0])

    for listing_id in range(1, number_of_listing_events + 1):
        listing_event = {"id": listing_id}

        for i in range(num_of_conditions):
            listing_event[conditions[0][i]] = bool(fake.random.getrandbits(1))

        for i in range(num_of_conditions):
            listing_event[conditions[1][i]] = fake.random.randint(0, 3)

        for i in range(num_of_conditions):
            listing_event[conditions[2][i]] = fake.random.randint(0, 5)

        listing_events.append(listing_event)

    return listing_events
