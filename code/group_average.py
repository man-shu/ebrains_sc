from glob import glob
from pathlib import Path
import os
from joblib import Parallel, delayed
import pandas as pd
import json
import numpy as np


def json_sidecar(bids_path, metadata_file, dry=True):
    """Creates a JSON sidecar file with the provided metadata.

    Parameters
    ----------
    file_path : str
        Path to save the JSON sidecar file (without .json extension)
    metadata : dict
        Dictionary containing metadata to be saved in the JSON file
    """
    json_path = f"{bids_path}.json"
    if not dry:
        with open(metadata_file, "r") as f:
            metadata = json.load(f)
            print(f"Loaded metadata from {metadata_file}")

            file_name = os.path.basename(bids_path)
            file_name_parts = file_name.split("_")
            breakpoint()
            meas = file_name_parts[1].split("-")[1]
            if meas == "density":
                metadata["RelationshipMeasure"] = "density"
                metadata["MeasureDescription"] = (
                    "Streamline density (count / region volume) between each"
                    " pair of regions (414 * 414)."
                )
            metadata["MeasureDescription"].replace(
                ".", ", additionally, averaged across subjects."
            )

        with open(json_path, "w") as json_file:
            json.dump(metadata, json_file, indent=4)
    print(f"Created JSON sidecar at {json_path}")


def compute_group_average(
    out_dir, metadata_file="relmat_sidecar.json", dry=True
):

    for measure in ["density", "sift2"]:
        tsv_files = list(out_dir.glob(f"**/ses-*/**/*{measure}*tsv"))
        tsv_files.sort()

        matrices = []

        for fi in tsv_files:
            file_name = os.path.basename(fi)
            file_name_parts = file_name.split("_")
            ses_dir = file_name_parts[1]
            meas = file_name_parts[2].split("-")[1]
            output_path_dir = out_dir / "group" / "dwi"

            if not dry:
                os.makedirs(output_path_dir, exist_ok=True)

            output_path = output_path_dir / (
                f"group_meas-{meas}_relmat.dense.tsv"
            )

            if not dry:
                matrix = pd.read_csv(fi, header=None, index_col=None, sep="\t")
                matrices.append(matrix.values)

        if not dry:
            breakpoint()
            mean_matrix = np.array(matrices).mean(axis=0)
            pd.DataFrame(mean_matrix).to_csv(
                output_path, header=False, index=False, sep="\t"
            )
        print(f"Created group average matrix at {output_path}")

        # create the json sidecar
        json_sidecar(output_path.with_suffix(""), metadata_file, dry=dry)


if __name__ == "__main__":

    out_dir = Path("/data/parietal/store3/work/haggarwa/dwi_connectivity")
    dry = False
    compute_group_average(out_dir, dry=dry)
