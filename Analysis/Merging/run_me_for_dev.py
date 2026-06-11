"""Developer helper to run the Analysis/Merging main_01 script.

Edit the paths below to point at your local files and run this script in PyCharm.
"""
from pathlib import Path
import subprocess


def run():
    # EDIT THESE PATHS before running
    keuringsinfo = Path("/home/davidlinux/PycharmProjects/InfraDbToArangoDb/Analysis/keuringsinfo_20260605_133843.xlsx")
    kasten = Path("/home/davidlinux/PycharmProjects/InfraDbToArangoDb/Analysis/Merging/kastenVlaanderen_l72_Merge_20260421_220143_1_ExportFeatures1_ZonderNullLocatie_TableToExcel.xlsx")
    output = Path("/home/davidlinux/PycharmProjects/InfraDbToArangoDb/Analysis/keuringsinfo_20260605_133843_met_locatie.xlsx")

    cmd = [
        "python",
        "Analysis/Merging/main_01_toevoegen_locatie_klassen.py",
        "--input-keuringsinfo",
        str(keuringsinfo),
        "--input-kasten",
        str(kasten),
        "--output",
        str(output),
    ]

    print("Running:", " ".join(cmd))
    subprocess.run(cmd)


if __name__ == '__main__':
    run()
