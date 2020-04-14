from random import shuffle, seed
from faker.providers.person.en import Provider


def generate_conditions(iteration, num_of_conditions):
    first_names = list(set(Provider.first_names))[iteration:iteration+50]
    seed(123)
    shuffle(first_names)

    conditions = [
        first_names[i] for i in range(iteration*num_of_conditions, iteration*num_of_conditions + num_of_conditions)
    ]
    return conditions
