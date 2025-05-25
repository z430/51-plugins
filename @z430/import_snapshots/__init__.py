import json
import os

import fiftyone as fo
import fiftyone.core.storage as fos
import fiftyone.operators as foo
import fiftyone.operators.types as types
import tqdm


class ImportSnapshots(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="import_snapshots",
            label="Import Snapshots",
            description="Import previously exported snapshots into a dataset",
            light_icon="/assets/icon-light.svg",
            dark_icon="/assets/icon-dark.svg",
            allow_delegated_execution=True,
            allow_immediate_execution=True,
            default_choice_to_delegated=True,
            dynamic=True,
        )

    def resolve_input(self, ctx):
        inputs = types.Object()

        _import_snapshots_input(ctx, inputs)
        return types.Property(inputs, view=types.View(label="Import snapshots"))

    def execute(self, ctx):
        input_dir = _parse_path(ctx, "input_dir")
        media_dir = _parse_path(ctx, "media_dir")
        target_dataset = ctx.params.get("target_dataset", "CURRENT_DATASET")

        # Get the target dataset
        dataset = self._get_or_create_dataset(ctx, target_dataset)
        if not dataset:
            return

        # Find all JSON files in the input directory
        json_files = [f for f in os.listdir(input_dir) if f.endswith(".json")]
        total_files = len(json_files)

        # Set up progress tracking for delegated execution
        if ctx.delegated:
            ctx.update_progress(
                0, total_files, message=f"Starting import of {total_files} samples..."
            )

        # Process each JSON file
        for i, json_file in enumerate(tqdm.tqdm(json_files)):
            # Update progress periodically for delegated execution
            if ctx.delegated and i % max(1, int(total_files / 10)) == 0:
                ctx.update_progress(
                    i, total_files, message=f"Importing samples... ({i}/{total_files})"
                )

            if i % 100 == 0:
                print(f"Processing {i + 1}/{total_files}: {json_file}")

            # Import the sample
            self._import_sample(json_file, input_dir, media_dir, dataset)

        # Mark as complete when done
        if ctx.delegated:
            ctx.update_progress(
                total_files,
                total_files,
                message=f"Import complete. Imported {total_files} samples.",
            )

        # Reload the app view
        if not ctx.delegated:
            ctx.trigger("reload_dataset")

    def _get_or_create_dataset(self, ctx, target_dataset):
        """Get or create the target dataset based on user selection."""
        if target_dataset == "CURRENT_DATASET":
            return ctx.dataset

        # Load different dataset
        dataset_name = ctx.params.get("dataset_name", "")
        if not dataset_name:
            ctx.error("No dataset name provided")
            return None

        # Check if dataset exists, create if it doesn't
        if dataset_name in fo.list_datasets():
            dataset = fo.load_dataset(dataset_name)
        else:
            dataset = fo.Dataset(dataset_name)

        return dataset

    def _import_sample(self, json_file, input_dir, media_dir, dataset):
        """Import a single sample from JSON file."""
        # Load the sample JSON
        json_path = os.path.join(input_dir, json_file)
        with open(json_path, "r") as f:
            sample_dict = json.load(f)

        # Check for the media file in the input directory
        media_filename = os.path.basename(sample_dict.get("filepath", ""))
        media_path = os.path.join(input_dir, media_filename)

        # Create the sample
        sample = fo.Sample.from_dict(sample_dict)

        # Update the sample's filepath to the imported media location
        if os.path.exists(media_path):
            # Determine destination for media files
            if media_dir:
                # Use custom media directory if specified
                destination_dir = media_dir
            else:
                # Otherwise use dataset's default storage location
                destination_dir = os.path.join(
                    fo.config.default_dataset_dir, dataset.name
                )

            os.makedirs(destination_dir, exist_ok=True)
            new_media_path = os.path.join(destination_dir, media_filename)

            # Only copy if doesn't already exist at destination
            if not os.path.exists(new_media_path):
                fos.copy_file(media_path, new_media_path)

            sample.filepath = new_media_path

        # Add to dataset
        dataset.add_sample(sample)


def _import_snapshots_input(ctx, inputs):
    # Choose input directory (where snapshots are stored)
    input_file_explorer = types.FileExplorerView(
        choose_dir=True,
        button_label="Choose snapshots directory...",
    )

    inputs.file(
        "input_dir",
        required=True,
        label="Input Directory",
        description="Choose a directory containing exported snapshots",
        view=input_file_explorer,
    )

    # Choose output directory for media files
    output_file_explorer = types.FileExplorerView(
        choose_dir=True,
        button_label="Choose media output directory...",
    )

    inputs.file(
        "media_dir",
        required=False,
        label="Media Output Directory",
        description="Choose a directory to store imported media files (optional)",
        view=output_file_explorer,
    )

    # Choose target dataset
    target_choices = types.RadioGroup()
    target_choices.add_choice(
        "CURRENT_DATASET",
        label="Current dataset",
        description="Import into the current dataset",
    )
    target_choices.add_choice(
        "OTHER_DATASET",
        label="Other dataset",
        description="Import into another dataset",
    )

    inputs.enum(
        "target_dataset",
        target_choices.values(),
        default="CURRENT_DATASET",
        view=target_choices,
    )

    # Conditionally show dataset name field
    target_dataset = ctx.params.get("target_dataset", "CURRENT_DATASET")
    if target_dataset == "OTHER_DATASET":
        # Show dropdown of existing datasets with option to create new
        dataset_names = fo.list_datasets()

        dataset_choices = types.DropdownView()
        dataset_choices.add_choice("", label="Create new dataset...")

        for name in dataset_names:
            dataset_choices.add_choice(name, label=name)

        inputs.str(
            "dataset_name",
            required=True,
            label="Dataset name",
            description="Choose or enter a dataset name",
            view=dataset_choices,
        )

    # Always validate that input_dir exists and has JSON files
    input_dir = _parse_path(ctx, "input_dir")
    if input_dir is None:
        return False

    if not fos.isdir(input_dir):
        inputs.get("input_dir").invalid = True
        inputs.get("input_dir").error_message = "Directory does not exist"
        return False

    # Check that the directory has at least one JSON file
    json_files = [f for f in os.listdir(input_dir) if f.endswith(".json")]
    if not json_files:
        inputs.get("input_dir").invalid = True
        inputs.get("input_dir").error_message = "No snapshots found in this directory"
        return False

    return True


def _parse_path(ctx, key):
    value = ctx.params.get(key, None)
    return value.get("absolute_path", None) if value else None


def register(p):
    p.register(ImportSnapshots)
