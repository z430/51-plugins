import fiftyone as fo
import fiftyone.zoo as foz

dataset = foz.load_zoo_dataset(
    "coco-2017",
    split="validation",
    max_samples=500,
    shuffle=True,
    dataset_name="coco-2017-validation",
)
session = fo.launch_app(dataset)
session.wait(-1)
