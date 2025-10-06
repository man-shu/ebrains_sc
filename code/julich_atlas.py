import siibra
import json
import pandas as pd
import os
from pathlib import Path


def fetch(output_dir):
    """Fetches the JulichBrain207 atlas from Siibra and saves the nifti file,
    json sidecar, and labels tsv file in a specified directory.

    The atlas is saved in BIDS format with appropriate metadata.

    Returns
    -------
    None
    """
    # fetch the JulichBrain207 atlas from Siibra
    julich_atlas = siibra.get_map(
        space="mni152", parcellation="julich 3", maptype="labelled"
    )

    # make directory to save atlas files
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # fetch the atlas nifti file
    julich_nifti = julich_atlas.fetch()
    atlas_path = (
        output_dir / "atlas-JulichBrain207_space-MNI152NLin2009cAsym_dseg"
    )
    julich_nifti.to_filename(f"{atlas_path}.nii.gz")

    # create the json sidecar
    json_data = {
        "Name": julich_atlas.name,
        "Authors": julich_atlas.authors,
        "BIDSVersion": "1.1.0",
        "SpatialReference": "http://www.bic.mni.mcgill.ca/~vfonov/icbm/2009/mni_icbm152_nlin_asym_09c_nifti.zip/mni_icbm152_t1_tal_nlin_asym_09c.nii",
        "Space": julich_atlas.space.name,
        "Resolution": "Matched with original template resolution (1x1x1 mm^3)",
        "Dimensions": len(julich_nifti.shape),
        "License": julich_atlas.LICENSE,
        "ReferencesAndLinks": julich_atlas.urls,
        "Species": str(julich_atlas.species),
    }
    with open(f"{atlas_path}.json", "w") as json_file:
        json.dump(json_data, json_file, indent=4)

    # create the labels file
    labels_dict = {
        "index": [i for i, _ in enumerate(julich_atlas.regions, 1)],
        "label": [region for region in julich_atlas.regions],
    }
    labels_df = pd.DataFrame(labels_dict)

    labels_df.to_csv(f"{atlas_path}.tsv", index=False, sep="\t")

    return {"maps": f"{atlas_path}.nii.gz", "labels": labels_df}
