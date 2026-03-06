import datetime
from pathlib import Path

import eccodes as ec


class Tool:
    # e.g grib_set -s class=rd,expver=xxxx,type=fc,step=0,date={{ DATE|LONG_DATE }} "{{ SOURCE_DATA }}" data.xxxx.0.grib
    @classmethod
    def modify_metadata_source_file(
        cls,
        args: list[tuple[str, str | int]],
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

            yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
            ec.codes_set(gid, "date", int(yesterday))

            for k, v in args:
                if isinstance(v, int):
                    ec.codes_set(gid, k, v)
                else:
                    ec.codes_set_string(gid, k, v)

            with target_file.open("a+b") as out:
                ec.codes_write(gid, out)

            ec.codes_release(gid)

        template_grib_fd.close()


def generate_test_files_key_value(
    source_file_path,
    function_tmp,
    key_value_list: list[list[tuple[str, str | int]]],
    output_file_names: list[str],
):
    """Creates copies of the GRIB file at source_file_path in function_tmp. For each
       element of the key_value_list all given key-value-pairs are modified in the resulting GRIB
       file and the corresponding entry from output_file_names is used as a name.

       This means:

       setup_fdb_key_value(
           source_file_path,
           function_tmp,
           [
               [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "0")],
               [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", "0")],
               [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "1")],
               [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", "1")],
               [("class", "rd"), ("expver", "xxxx"), ("type", "fc"), ("step", "2")],
               [("class", "rd"), ("expver", "xxxy"), ("type", "fc"), ("step", "2")],
           ],
           [
               "data.xxxx.0.grib",
               "data.xxxy.0.grib",
               "data.xxxx.1.grib",
               "data.xxxy.1.grib",
               "data.xxxx.2.grib",
               "data.xxxy.2.grib",
           ],
       )

        Is creating 6 files with the names of output_file_names. For each file the GRIB metadata
        is modified as given in key_value_list.


    Args:
        source_file_path (Path): Path to the template GRIB file
        function_tmp (Path): Path to the output folder
        key_value_list: List of key-value pair lists for modifying the GRIB metadata
        output_file_names: Names of the output files

    Returns:
        target_files - A list of paths to the created files
    """
    assert len(key_value_list) == len(output_file_names)

    # Data setup
    target_files: list[Path] = []

    for i in range(len(key_value_list)):
        cur_key_value_list = key_value_list[i]
        output_file_name = output_file_names[i]
        target_file_path = function_tmp / f"data.{output_file_name}.grib"
        Tool.modify_metadata_source_file(
            cur_key_value_list,
            source_file=source_file_path,
            target_file=target_file_path,
        )
        target_files.append(target_file_path)

    return target_files
