"""Tests for plotting_functions.count_title_lines.

``count_title_lines`` sizes the dynamic top margin for the currents 1D
time-series plot (issue #221). Currents titles carry an optional depth
line and ADCP-type line on top of the scalar title rows, so the margin
must grow with the actual number of ``<br>``-separated rows or the
title overlaps the horizontal legend.
"""
import unittest

from ofs_skill.visualization.plotting_functions import count_title_lines


class TestCountTitleLines(unittest.TestCase):
    """Verify the ``<br>``-based line counter used for margin sizing."""

    def test_empty_title_is_one_line(self):
        """An empty title still occupies a single visual row."""
        self.assertEqual(count_title_lines(''), 1)

    def test_no_break_is_one_line(self):
        """A title with no ``<br>`` spans exactly one row."""
        self.assertEqual(count_title_lines('<b>Only one line<b>'), 1)

    def test_line_count_is_breaks_plus_one(self):
        """N ``<br>`` separators render N+1 rows."""
        self.assertEqual(count_title_lines('a<br>b<br>c'), 3)

    def test_scalar_title_four_lines(self):
        """A typical scalar title (header/station/OFS/date) is 4 rows."""
        scalar = (
            '<b>NOAA/NOS OFS Skill Assessment<br>'
            'CO-OPS station:&nbsp;Providence (8454000)<br>'
            'OFS:&nbsp;CBOFS&nbsp;&nbsp;&nbsp;Node ID:&nbsp;123'
            '&nbsp;&nbsp;&nbsp;'
            '<br>From:&nbsp;2025-07-01&nbsp;&nbsp;&nbsp;To:&nbsp;2025-07-02<b>'
        )
        self.assertEqual(count_title_lines(scalar), 4)

    def test_currents_title_with_depth_no_adcp_five_lines(self):
        """A currents title with a depth row but no ADCP-type row is 5 rows.

        This is the intermediate case (e.g. a USGS/CHS or side-looking
        currents station that has a depth line but no separate ADCP-type
        line). It exercises the ``+30`` px / ``+0.005`` margin step once and
        locks in the 5-line (180 px) top margin.
        """
        currents = (
            '<b>NOAA/NOS OFS Skill Assessment<br>'
            'USGS station:&nbsp;Some River (01234567)<br>'
            'OFS:&nbsp;CBOFS&nbsp;&nbsp;&nbsp;Node ID:&nbsp;123'
            '&nbsp;&nbsp;&nbsp;'
            '<br>Bin&nbsp;01&nbsp;—&nbsp;Obs&nbsp;depth&nbsp;2.0&nbsp;m'
            '<br>From:&nbsp;2025-07-01&nbsp;&nbsp;&nbsp;To:&nbsp;2025-07-02<b>'
        )
        self.assertEqual(count_title_lines(currents), 5)

    def test_currents_title_with_depth_and_adcp_six_lines(self):
        """A full currents title (depth + ADCP-type rows) is 6 rows."""
        currents = (
            '<b>NOAA/NOS OFS Skill Assessment<br>'
            'CO-OPS station:&nbsp;Some Bay (cb1401)<br>'
            'OFS:&nbsp;CBOFS&nbsp;&nbsp;&nbsp;Node ID:&nbsp;123'
            '&nbsp;&nbsp;&nbsp;'
            '<br>Bin&nbsp;01&nbsp;—&nbsp;Obs&nbsp;depth&nbsp;2.0&nbsp;m'
            '<br>Side-Looking ADCP'
            '<br>From:&nbsp;2025-07-01&nbsp;&nbsp;&nbsp;To:&nbsp;2025-07-02<b>'
        )
        self.assertEqual(count_title_lines(currents), 6)


if __name__ == '__main__':
    unittest.main()
