from utils.synthetic_award_generator import calculate_award_points


def test_calculate_award_points_is_deterministic():
    first_result = calculate_award_points("LAX", "JFK", "Delta", "economy")
    second_result = calculate_award_points("LAX", "JFK", "Delta", "economy")

    assert first_result == second_result
    assert first_result > 0
