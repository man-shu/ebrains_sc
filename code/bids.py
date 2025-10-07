from glob import glob
from pathlib import Path
import os
from joblib import Parallel, delayed
import pandas as pd
import json


def json_sidecar(bids_path, metadata_file, dry=True):
    """Creates a JSON sidecar file with the provided metadata.

    Parameters
    ----------
    file_path : str
        Path to save the JSON sidecar file (without .json extension)
    metadata : dict
        Dictionary containing metadata to be saved in the JSON file
    """
    if not dry:
        with open(metadata_file, "r") as f:
            metadata = json.load(f)
            print(f"Loaded metadata from {metadata_file}")

        json_path = f"{bids_path}.json"
        with open(json_path, "w") as json_file:
            json.dump(metadata, json_file, indent=4)
    print(f"Created JSON sidecar at {json_path}")


def create_bids(
    out_dir, sub_dir, metadata_file="relmat_sidecar.json", dry=True
):

    csv_files = list(sub_dir.glob(f"*/dwi/*JulichBrain207*csv"))
    csv_files.sort()

    for fi in csv_files:
        file_name = os.path.basename(fi)
        file_name_parts = file_name.split("_")
        ses_dir = file_name_parts[1]
        meas = file_name_parts[6].split(".")[0]
        if meas == "nosift":
            meas = "density"
        elif meas == "siftweighted":
            meas = "sift2"

        space = file_name_parts[5]
        connectome_type = file_name_parts[3]
        if space == "MNI152" and connectome_type == "connectome":
            output_path_dir = out_dir / sub_dir.name / ses_dir / "dwi"

            if not dry:
                os.makedirs(output_path_dir, exist_ok=True)

            output_path = output_path_dir / (
                f"{sub_dir.name}_{ses_dir}_meas-{meas}_relmat.dense.tsv"
            )

            if not dry:
                matrix = pd.read_csv(fi, header=None, index_col=None)
                print(matrix)
                matrix.to_csv(output_path, header=False, index=False, sep="\t")
            print(f"Copied {fi} to {output_path}")

            # create the json sidecar
            json_sidecar(output_path.with_suffix(""), metadata_file, dry=dry)


if __name__ == "__main__":

    root_directory = Path("/data/parietal/store2/work/haggarwa/")
    out_dir = Path("/data/parietal/store3/work/haggarwa/dwi_connectivity")
    sub_dirs = list(root_directory.glob("sub-*"))
    sub_dirs.sort()
    dry = False

    Parallel(n_jobs=12)(
        delayed(create_bids)(out_dir, sub_dir, dry=True)
        for sub_dir in sub_dirs
    )
