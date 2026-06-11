"""Developer helper to run the Analysis/Merging main_02 script.

Edit the paths below to point at your local files and run this script in PyCharm.
"""
from pathlib import Path
import subprocess


def run():
    # EDIT THESE PATHS before running
    keuringsinfo = Path("/home/davidlinux/PycharmProjects/InfraDbToArangoDb/Analysis/keuringsinfo_20260605_133843_met_locatie.xlsx")
    inbreuken = Path("/home/davidlinux/Documenten/AWV/Keuringen/MyVinotte Min.Vl.Gemeenschap Reports list.detaillijst-latest-keuring.xlsx")
    output = Path("/home/davidlinux/PycharmProjects/InfraDbToArangoDb/Analysis/keuringsinfo_20260605_133843_met_inbreuken.xlsx")

    cmd = [
        "python",
        "Analysis/Merging/main_02_toevoegen_inbreuken.py",
        "--input-keuringsinfo",
        str(keuringsinfo),
        "--input-inbreuken",
        str(inbreuken),
        "--output",
        str(output),
    ]

    print("Running:", " ".join(cmd))
    subprocess.run(cmd)


if __name__ == '__main__':
    run()