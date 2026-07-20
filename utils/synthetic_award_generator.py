from config.route_tiers import ROUTE_TIERS, DEFAULT_TIER
from config.multipliers import (
    ROUTE_MULTIPLIERS,
    CABIN_MULTIPLIERS,
    AIRLINE_MULTIPLIERS,
)

BASE_POINTS = 8000


def get_route_tier(origin, destination):
    return ROUTE_TIERS.get((origin, destination), DEFAULT_TIER)


def calculate_award_points(
    origin,
    destination,
    airline,
    cabin_class="economy",
):
    tier = get_route_tier(origin, destination)

    route_multiplier = ROUTE_MULTIPLIERS[tier]

    cabin_multiplier = CABIN_MULTIPLIERS.get(
        cabin_class,
        1.0,
    )

    airline_multiplier = AIRLINE_MULTIPLIERS.get(
        airline,
        1.0,
    )

    points = (
        BASE_POINTS
        * route_multiplier
        * cabin_multiplier
        * airline_multiplier
    )

    return int(round(points))
