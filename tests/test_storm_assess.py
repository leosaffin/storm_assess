import storm_assess


def test_lon_lat_to_distance():
    # Example taken from https://pypi.org/project/haversine/
    lyon = [45.7597, 4.8422] # (lat, lon)
    paris = [48.8567, 2.3508]
    distance = storm_assess.lon_lat_to_distance(lyon[::-1], paris[::-1])
    assert distance == 392217.2595594006
