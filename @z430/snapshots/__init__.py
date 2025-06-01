"""
FiftyOne Snapshots Plugin

This plugin allows users to export samples from a dataset to JSON files
with their associated media, and later import those snapshots back into FiftyOne.

Copyright (c) 2025 Your Name
License: MIT
"""

import os
import json

import fiftyone as fo
import fiftyone.core.storage as fos
import fiftyone.operators as foo
import fiftyone.operators.types as types
import tqdm
from loguru import logger


class SnapshotSamples(foo.Operator):
    """
    A FiftyOne operator that exports samples from a dataset to JSON files
    along with their associated media files.

    This operator can export selected samples, the current view, or the entire dataset,
    preserving all metadata and fields associated with each sample.
    """

    @property
    def config(self):
        """
        Returns the operator configuration.

        Returns:
            foo.OperatorConfig: The operator configuration
        """
        return foo.OperatorConfig(
            name="snapshot_samples",
            label="Snapshot Samples",
            description="Export samples and their metadata to a directory",
            light_icon="/assets/icon-light.svg",
            dark_icon="/assets/icon-dark.svg",
            allow_delegated_execution=True,
            allow_immediate_execution=True,
            default_choice_to_delegated=True,
            dynamic=True,
        )

    def resolve_input(self, ctx):
        """
        Defines the input interface for the operator.

        Args:
            ctx: The operator context

        Returns:
            types.Property: The input property for the operator
        """
        inputs = types.Object()
        _snapshot_samples_input(ctx, inputs)
        return types.Property(inputs, view=types.View(label="Snapshot samples"))

    def execute(self, ctx):
        """
        Executes the operator on the given context.

        Args:
            ctx: The operator context containing user inputs and dataset information
        """
        target = ctx.params.get("target", None)
        output_dir = _parse_path(ctx, "output_dir")

        if not output_dir:
            ctx.error("No output directory specified")
            return

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        target_view = _get_target_view(ctx, target)
        sample_count = len(target_view)

        logger.info(f"Exporting {sample_count} samples to {output_dir}")

        # Export the samples in FiftyOne format to the specified directory
        for sample in tqdm.tqdm(target_view, desc="Exporting samples"):
            sample_json = sample.to_json(pretty_print=True)
            # write the sample JSON to a file
            sample_filename = f"{os.path.basename(sample.filepath).split('.')[0]}.json"
            sample_path = os.path.join(output_dir, sample_filename)
            with open(sample_path, "w") as f:
                f.write(sample_json)

            # copy the media file to the output directory
            if sample.filepath:
                media_filename = os.path.basename(sample.filepath)
                media_path = os.path.join(output_dir, media_filename)
                fos.copy_file(sample.filepath, media_path)

        logger.info(f"Successfully exported {sample_count} samples to {output_dir}")

        if not ctx.delegated:
            ctx.trigger("reload_dataset")


def _snapshot_samples_input(ctx, inputs):
    ready = _get_src_dst_collections(ctx, inputs)
    return ready


def _get_src_dst_collections(ctx, inputs):
    has_view = ctx.view != ctx.dataset.view()
    has_selected = bool(ctx.selected)
    default_target = None

    if has_view or has_selected:
        target_choices = types.RadioGroup()
        target_choices.add_choice(
            "DATASET",
            label="Entire dataset",
            description="Export the entire dataset",
        )

        if has_view:
            target_choices.add_choice(
                "CURRENT_VIEW",
                label="Current view",
                description="Export the current view",
            )
            default_target = "CURRENT_VIEW"

        if has_selected:
            target_choices.add_choice(
                "SELECTED_SAMPLES",
                label="Selected samples",
                description="Export only the selected samples",
            )
            default_target = "SELECTED_SAMPLES"

        inputs.enum(
            "target",
            target_choices.values(),
            default=default_target,
            view=target_choices,
        )

    file_explorer = types.FileExplorerView(
        choose_dir=True,
        button_label="Choose a directory...",
    )

    prop = inputs.file(
        "output_dir",
        required=True,
        label="Output Directory",
        description="Choose a directory at which to write the export",
        view=file_explorer,
    )

    output_dir = _parse_path(ctx, "output_dir")
    if output_dir is None:
        return False

    if fos.isdir(output_dir):
        inputs.bool(
            "overwrite",
            default=True,
            label="Directory already exists. Overwrite it?",
            view=types.CheckboxView(),
        )
        overwrite = ctx.params.get("overwrite", True)

        if not overwrite:
            prop.invalid = True
            prop.error_message = "The specified directory already exists"
            return False

    return True


def _get_target_view(ctx, target):
    if target == "SELECTED_SAMPLES":
        return ctx.view.select(ctx.selected)

    if target == "DATASET":
        return ctx.dataset

    return ctx.view


class ImportSnapshots(foo.Operator):
    """
    A FiftyOne operator that imports previously exported snapshots into a dataset.

    This operator can import snapshots into the current dataset or another dataset,
    and allows for flexible configuration of import options such as media location
    and sample tagging.
    """

    @property
    def config(self):
        """
        Returns the operator configuration.

        Returns:
            foo.OperatorConfig: The operator configuration
        """
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
        """
        Defines the input interface for the operator.

        Args:
            ctx: The operator context

        Returns:
            types.Property: The input property for the operator
        """
        inputs = types.Object()

        _import_snapshots_input(ctx, inputs)
        return types.Property(inputs, view=types.View(label="Import snapshots"))

    def execute(self, ctx):
        """
        Executes the operator on the given context.

        Args:
            ctx: The operator context containing user inputs and dataset information
        """
        input_dir = _parse_path(ctx, "input_dir")
        media_dir = _parse_path(ctx, "media_dir")
        target_dataset = ctx.params.get("target_dataset", "CURRENT_DATASET")
        tags = ctx.params.get("tags", "")

        logger.info(f"Starting import process with parameters: {ctx.params}")

        if not input_dir or not os.path.isdir(input_dir):
            error_msg = "Input directory does not exist or was not specified"
            ctx.error(error_msg)
            logger.error(error_msg)
            return

        # Get the target dataset
        dataset = self._get_or_create_dataset(ctx, target_dataset)
        if dataset is None:
            logger.error("No dataset specified or created")
            return

        # Find all JSON files in the input directory
        json_files = [f for f in os.listdir(input_dir) if f.endswith(".json")]
        total_files = len(json_files)
        logger.info(
            f"Found {total_files} JSON files in {input_dir} to import into dataset {dataset.name}"
        )

        # Process each JSON file
        for i, json_file in enumerate(tqdm.tqdm(json_files)):
            # Import the sample
            sample = self._import_sample(json_file, input_dir, media_dir, dataset)

            # Add tags if provided
            if tags and sample is not None:
                sample.tags.extend(tags)
                sample.save()

        dataset.save()
        logger.info(f"Imported {total_files} samples into dataset {dataset.name}")

        # Reload the app view
        if not ctx.delegated:
            ctx.trigger("reload_dataset")

    def _get_or_create_dataset(self, ctx, target_dataset):
        """
        Get or create the target dataset based on user selection.

        Args:
            ctx: The operator context
            target_dataset: The target dataset selection (CURRENT_DATASET or OTHER_DATASET)

        Returns:
            fo.Dataset: The target dataset, or None if dataset could not be created/loaded
        """
        try:
            if target_dataset == "CURRENT_DATASET":
                if ctx.dataset is None:
                    ctx.error("No dataset is currently loaded")
                    logger.error("No dataset is currently loaded")
                    return None
                logger.info(f"Using current dataset: {ctx.dataset.name}")
                return ctx.dataset

            # Load different dataset
            dataset_name = ctx.params.get("dataset_name", "")
            logger.info(f"Target dataset: {dataset_name}")
            if not dataset_name:
                ctx.error("No dataset name provided")
                logger.error("No dataset name provided")
                return None

            # Check if dataset exists, create if it doesn't
            if dataset_name in fo.list_datasets():
                try:
                    dataset = fo.load_dataset(dataset_name)
                    logger.info(f"Loaded existing dataset: {dataset_name}")
                except Exception as e:
                    ctx.error(f"Error loading dataset {dataset_name}: {e}")
                    logger.error(f"Error loading dataset {dataset_name}: {e}")
                    return None
            else:
                try:
                    logger.info(f"Creating new dataset: {dataset_name}")
                    dataset = fo.Dataset(dataset_name)
                    dataset.persistent = ctx.params.get("persistent", True)
                    dataset.description = ctx.params.get("description", "")
                except Exception as e:
                    ctx.error(f"Error creating dataset {dataset_name}: {e}")
                    logger.error(f"Error creating dataset {dataset_name}: {e}")
                    return None

            logger.debug(f"Dataset ready: {dataset}")
            return dataset

        except Exception as e:
            ctx.error(f"Unexpected error handling dataset: {e}")
            logger.error(f"Unexpected error handling dataset: {e}")
            return None

    def _import_sample(self, json_file, input_dir, media_dir, dataset):
        """
        Import a single sample from a JSON file.

        Args:
            json_file: Name of the JSON file containing sample data
            input_dir: Directory containing the JSON file and media files
            media_dir: Directory to store media files (optional)
            dataset: Target dataset to add the sample to

        Returns:
            fo.Sample: The imported sample, or None if import failed
        """
        try:
            # Load the sample JSON
            json_path = os.path.join(input_dir, json_file)
            with open(json_path, "r") as f:
                sample_dict = json.load(f)

            # Check for essential data
            if not isinstance(sample_dict, dict):
                logger.error(f"Invalid JSON format in file: {json_file}")
                return None

            # Check for the media file in the input directory
            media_filename = os.path.basename(sample_dict.get("filepath", ""))
            media_path = os.path.join(input_dir, media_filename)

            # Create the sample
            sample = fo.Sample.from_dict(sample_dict)

            # Update the sample's filepath to the imported media location
            if media_filename and os.path.exists(media_path):
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
                    logger.debug(f"Copied media file: {media_filename}")
                else:
                    logger.debug(f"Media file already exists: {media_filename}")

                sample.filepath = new_media_path
            elif media_filename:
                logger.warning(f"Media file not found: {media_filename}")

            # Add to dataset
            dataset.add_sample(sample)
            return sample

        except Exception as e:
            logger.error(f"Error importing sample from {json_file}: {e}")
            return None


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
        # Show dropdown of existing datasets
        dataset_names = fo.list_datasets()

        # Use a radio group first to select existing or new
        dataset_type_choices = types.RadioGroup()
        dataset_type_choices.add_choice(
            "EXISTING",
            label="Use existing dataset",
            description="Select from existing datasets",
        )
        dataset_type_choices.add_choice(
            "NEW",
            label="Create new dataset",
            description="Create a new dataset",
        )

        inputs.enum(
            "dataset_type",
            dataset_type_choices.values(),
            default="EXISTING" if dataset_names else "NEW",
            view=dataset_type_choices,
        )

        # Based on selection, show different inputs
        dataset_type = ctx.params.get(
            "dataset_type", "EXISTING" if dataset_names else "NEW"
        )

        if dataset_type == "EXISTING" and dataset_names:
            # Show dropdown of existing datasets
            dataset_choices = types.DropdownView()

            for name in dataset_names:
                dataset_choices.add_choice(name, label=name)

            inputs.str(
                "dataset_name",
                required=True,
                label="Dataset name",
                description="Choose an existing dataset",
                view=dataset_choices,
            )
        else:
            name_prop = inputs.str(
                "dataset_name",
                required=False,
                label="Name",
                description=(
                    "Choose a name for the dataset. If omitted, a randomly "
                    "generated name will be used"
                ),
            )

            name = ctx.params.get("dataset_name", None)

            if name and fo.dataset_exists(name):
                name_prop.invalid = True
                name_prop.error_message = f"Dataset {name} already exists"

            inputs.str(
                "description",
                required=False,
                label="Description",
                description="An optional description for the dataset",
            )
            inputs.bool(
                "persistent",
                default=True,
                required=True,
                label="Persistent",
                description="Whether to make the dataset persistent",
                view=types.CheckboxView(),
            )

    inputs.list(
        "tags",
        types.String(),
        required=False,
        label="Tags",
        description="Optional tag(s) for the samples",
        view=types.AutocompleteView(multiple=True),
    )

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
    p.register(SnapshotSamples)
    p.register(ImportSnapshots)
