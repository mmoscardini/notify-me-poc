LISTING_PROPERTY_TYPE_MAPPER = {
    "bedrooms": "integer",
    "price": "range",
    "has_pool": "boolean",
    "accept_ped": "boolean"
}


def get_property_type(property):
    return LISTING_PROPERTY_TYPE_MAPPER[property]