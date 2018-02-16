import json
from ce_comp import date_format
from ce_comp import generate_Endpoints
from ce_comp import endpoint_comparison

test_str = """

{
    "results": [
        {
            "name": "NGI_GRNET",
            "type": "NGI",
            "endpoints": [
                {
                    "name": "GR-01-AUTH",
                    "type": "SITES",
                    "results": [
                        {
                            "timestamp": "2018-01-24",
                            "availability": "100",
                            "reliability": "100",
                            "unknown": "0",
                            "uptime": "1",
                            "downtime": "0"
                        }
                    ]
                }
            ]
        }
    ]
}

"""


class TestClass(object):

    def test__date_format(self):

        assert "2018-02-07" == date_format("2018", "02", 7)
        assert "2018-02-17" == date_format("2018", "02", 17)
        
    def test_endpoint_generation(self):

        _dict1 = {}
        _dict1["availability"] = "100"
        _dict1["reliability"] = "100"

        _dict = {}
        _dict["GR-01-AUTH@NGI_GRNET"] = _dict1

        assert _dict == generate_Endpoints(json.loads(test_str))

    def test_endpoint_comparison(self):

        error = 0

        count = 0

        _dict = {}
        _dict["a_prod"] = float("100")
        _dict["a_devel"] = float("98")
        _dict["r_prod"] = float("-1")
        _dict["r_devel"] = float("100")
        _dict["d_a"] = "na"
        _dict["d_r"] = "na"

        _dict_t = {}
        _dict_t["a_prod"] = float("100")
        _dict_t["a_devel"] = float("98")
        _dict_t["r_prod"] = float("-1")
        _dict_t["r_devel"] = float("100")
        _dict_t["d_a"] = float("2")
        _dict_t["d_r"] = "na"

        assert (_dict_t, 2.0, 1) == endpoint_comparison(_dict, error, count)
        
