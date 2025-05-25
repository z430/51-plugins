import base64
import contextlib
import multiprocessing.dummy
import os

import eta.core.utils as etau

import fiftyone as fo
import fiftyone.core.fields as fof
import fiftyone.core.media as fom
import fiftyone.core.storage as fos
import fiftyone.core.utils as fou
import fiftyone.operators as foo
import fiftyone.operators.types as types
import fiftyone.types as fot

import tqdm


class SnapshotSamples(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="snapshot_samples",
            label="Snapshot Samples",
            description="Snapshots samples in a dataset",
            light_icon="/assets/icon-light.svg",
            dark_icon="/assets/icon-dark.svg",
            allow_delegated_execution=True,
            allow_immediate_execution=True,
            default_choice_to_delegated=True,
            dynamic=True,
        )

    def resolve_input(self, ctx):
        inputs = types.Object()

        _snapshot_samples_input(ctx, inputs)
        return types.Property(inputs, view=types.View(label="Snapshot samples"))

    def execute(self, ctx):
        target = ctx.params.get("target", None)
        output_dir = _parse_path(ctx, "output_dir")

        target_view = _get_target_view(ctx, target)

        # Export the samples in FiftyOne format to the specified directory
        for sample in tqdm.tqdm(target_view):
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


def _parse_path(ctx, key):
    value = ctx.params.get(key, None)
    return value.get("absolute_path", None) if value else None


def _get_target_view(ctx, target):
    if target == "SELECTED_SAMPLES":
        return ctx.view.select(ctx.selected)

    if target == "DATASET":
        return ctx.dataset

    return ctx.view


def register(p):
    p.register(SnapshotSamples)
