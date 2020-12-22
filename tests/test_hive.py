"""
specific tests for hive integration
"""
# see: https://docs.github.com/en/free-pro-team@latest/actions/creating-actions/creating-a-docker-container-action
# see: https://hub.docker.com/r/bde2020/hive/dockerfile

import unittest


class TestConnection(unittest.TestCase):
    """Test module functionality"""

    def test_unsecured(self):
        # TODO test unsecured connection to hive
        import hivejdbc
        pass
