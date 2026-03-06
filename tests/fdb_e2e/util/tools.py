from pathlib import Path
from typing import Tuple

import eccodes as ec


class Tool:
    # e.g grib_set -s class=rd,expver=xxxx,type=fc,step=0,date={{ DATE|LONG_DATE }} "{{ SOURCE_DATA }}" data.xxxx.0.grib
    @classmethod
    def modify_metadata_source_file(
        cls,
        args: list[Tuple[str, str | int]],
        source_file: Path,
        target_file: Path,
    ):
        assert source_file.is_file()
        template_grib_fd = source_file.open("rb")

        while True:
            gid = ec.codes_grib_new_from_file(template_grib_fd)

            if gid is None:
                break

            count_data_points = int(ec.codes_get(gid, "numberOfDataPoints"))
            count_values = int(ec.codes_get(gid, "numberOfValues"))

            try:
                count_missing = int(ec.codes_get(gid, "numberOfMissing"))
                assert count_missing == 0
            except ec.KeyValueNotFoundError:
                pass

            # This only supports messages without missing datapoints
            assert count_data_points == count_values

            for k, v in args:
                if k in ["date", "time", "paramId"]:
                    ec.codes_set(gid, k, v)
                else:
                    ec.codes_set_string(gid, k, v)

            with target_file.open("a+b") as out:
                ec.codes_write(gid, out)

            ec.codes_release(gid)

        template_grib_fd.close()
