from random import shuffle, seed
from faker.providers.person.en import Provider

first_names = list(set(Provider.first_names))
seed(123)
shuffle(first_names)


def generate_conditions(iteration, num_of_conditions):
    conditions = first_names[iteration*num_of_conditions:iteration*num_of_conditions+num_of_conditions]
    return conditions
