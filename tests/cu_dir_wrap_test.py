import unittest

from ofs_skill.skill_assessment.format_paired_one_d import get_distance_angle


class TestGetDistanceAngle(unittest.TestCase):
    """
    Unit tests for the `get_distance_angle` function.

    The `get_distance_angle(ofs_angle, obs_angle)` function computes the signed
    angular difference between a model (OFS) direction and an observation (OBS)
    direction in degrees.

    A positive result indicates that the OFS angle is clockwise relative to
    the OBS angle, while a negative result indicates it is counter-clockwise.
    Crucially, it must correctly handle the 0/360-degree wraparound.

    This test suite verifies:
    - Identical angles evaluating to 0.
    - Standard angular differences (no boundary crossing).
    - Wraparound angular differences (crossing the 360/0 boundary).
    - Edge cases where angles are exactly 180 degrees apart.
    """

    def test_identical_angles(self):
        """
        Test that identical OFS and OBS angles return a 0.0 degree difference.
        Evaluates various quadrants including exactly 0 and 360.
        """
        self.assertEqual(get_distance_angle(90.0, 90.0), 0.0)
        self.assertEqual(get_distance_angle(0.0, 0.0), 0.0)
        self.assertEqual(get_distance_angle(360.0, 360.0), 0.0)

    def test_standard_clockwise(self):
        """
        Test cases where the OFS angle is clockwise relative to the OBS angle,
        without crossing the 0/360 boundary.
        Expects a positive float result.
        """
        # OFS is 100, OBS is 90 -> Difference is +10 degrees
        self.assertEqual(get_distance_angle(100.0, 90.0), 10.0)
        self.assertEqual(get_distance_angle(180.0, 90.0), 90.0)

    def test_standard_counter_clockwise(self):
        """
        Test cases where the OFS angle is counter-clockwise relative to the OBS angle,
        without crossing the 0/360 boundary.
        Expects a negative float result.
        """
        # OFS is 80, OBS is 90 -> Difference is -10 degrees
        self.assertEqual(get_distance_angle(80.0, 90.0), -10.0)
        self.assertEqual(get_distance_angle(90.0, 180.0), -90.0)

    def test_wraparound_clockwise(self):
        """
        Test angular differences that cross the 0/360 boundary in a clockwise
        direction. Even though the OFS numerical value is smaller, it is physically
        clockwise of the OBS value.
        Expects a positive float result.
        """
        # OBS is 350, OFS is 10. Physically, OFS is 20 degrees clockwise.
        self.assertEqual(get_distance_angle(10.0, 350.0), 20.0)

    def test_wraparound_counter_clockwise(self):
        """
        Test angular differences that cross the 0/360 boundary in a counter-clockwise
        direction. Even though the OFS numerical value is larger, it is physically
        counter-clockwise of the OBS value.
        Expects a negative float result.
        """
        # OBS is 10, OFS is 350. Physically, OFS is 20 degrees counter-clockwise.
        self.assertEqual(get_distance_angle(350.0, 10.0), -20.0)

    def test_opposite_directions(self):
        """
        Test edge cases where the angles are exactly 180 degrees apart.
        The function should return exactly 180 (sign does not physically matter
        at exactly opposite angles, but we verify absolute magnitude).
        """
        self.assertEqual(abs(get_distance_angle(180.0, 0.0)), 180.0)
        self.assertEqual(abs(get_distance_angle(0.0, 180.0)), 180.0)

if __name__ == '__main__':
    unittest.main()
