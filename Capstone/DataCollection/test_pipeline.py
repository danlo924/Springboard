import unittest as u
import sys
# sys.path.append('C:\GitRepos\Springboard\Capstone\DataCollection') ## how can import this from another folder??
# for p in sys.path:
#     print(p)
import pipeline as p

class TestPipeline(u.TestCase):

    def test_get_sandp_companies_list(self):
        result = p.get_sandp_companies_list()