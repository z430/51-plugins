import multiprocessing.dummy


import fiftyone as fo
import fiftyone.core.storage as fos
import fiftyone.core.utils as fou
import fiftyone.operators as foo
import fiftyone.operators.types as types

import torch


class RunPredictions(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="run_predictions",
            label="Run Predictions",
            light_icon="/assets/icon-light.svg",
            dark_icon="/assets/icon-dark.svg",
            # allow_delegated_execution=True,
            allow_immediate_execution=True,
            default_choice_to_delegated=True,
            dynamic=True,
            execute_as_generator=True,
        )

    def resolve_input(self, ctx):
        inputs = types.Object()

        sample_inputs(ctx, inputs)

        return types.Property(inputs, view=types.View(label="Import Images"))

    def execute(self, ctx):
        target = ctx.params.get("target", None)
        view = _get_target_view(ctx, target)
        for update in _run_predictions(ctx, view):
            yield update
        if not ctx.delegated:
            yield ctx.trigger("reload_dataset")


def sample_inputs(ctx, inputs):
    has_view = ctx.view != ctx.dataset.view()
    has_selected = bool(ctx.selected)
    default_target = None

    if has_view or has_selected:
        target_choices = types.RadioGroup()
        target_choices.add_choice(
            "DATASET",
            label="Entire dataset",
            description="Run prediction to the entire dataset",
        )

        if has_view:
            target_choices.add_choice(
                "CURRENT_VIEW",
                label="Current view",
                description="Run prediction to the current view",
            )
            default_target = "CURRENT_VIEW"

        if has_selected:
            target_choices.add_choice(
                "SELECTED_SAMPLES",
                label="Selected samples",
                description="Run prediction to the selected samples",
            )
            default_target = "SELECTED_SAMPLES"

    target = ctx.params.get("target", default_target)
    target_view = _get_target_view(ctx, target)

    count = len(target_view)
    if count > 0:
        sample_text = "sample" if count == 1 else "samples"
        inputs.str(
            "msg",
            label=f"Running predictions on {count} {sample_text}",
            view=types.Warning(),
        )
    else:
        prop = inputs.str(
            "msg",
            label="No samples to run predictions on",
            view=types.Warning(),
        )
        prop.invalid = True


def _get_target_view(ctx, target):
    if target == "SELECTED_SAMPLES":
        return ctx.view.selected(ctx.selected)

    if target == "DATASET":
        return ctx.dataset.view()

    return ctx.view


def _parse_path(ctx, key):
    value = ctx.params.get(key, None)
    return value.get("absolute_path", None) if value else None


def _glob_files(directory=None, glob_patt=None):
    if directory is not None:
        glob_patt = f"{directory}/*"

    if glob_patt is None:
        return []

    return fos.get_glob_matches(glob_patt)


def _create_sample(filepath, tags, gsn, gso, metadata=None):
    sample = fo.Sample(filepath=filepath, tags=tags)
    sample["gsn"] = gsn
    sample["gso"] = gso
    return sample


def load_model(device):
    model = torch.hub.load("ultralytics/yolov5", "yolov5s")
    model.to(device)
    model.eval()
    return model


def get_predictions(model, image):
    results = model(image)
    return results.pandas().to_dict(orient="records")


def _run_predictions(ctx, view):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = load_model(device)

    if ctx.delegated:
        for sample in view:
            results = get_predictions(model, image)
            sample["predictions"] = results
            sample.save()
        return

    batcher = fou.DynamicBatcher(filepaths, target_latency=0.2, max_batch_beta=2.0)

    num_added = 0

    with batcher:
        for batch in batcher:
            num_added += len(batch)
            samples = map(make_sample, batch)
            ctx.dataset._add_samples_batch(samples, True, False, True)

            progress = num_added / num_total
            label = f"Loaded {num_added} of {num_total}"
            yield ctx.trigger("set_progress", dict(progress=progress, label=label))


def _upload_media_tasks(ctx, filepaths):
    upload_dir = _parse_path(ctx, "upload_dir")
    if not ctx.params.get("upload", None):
        upload_dir = None

    if upload_dir is None:
        return filepaths, None

    overwrite = ctx.params.get("overwrite", False)

    inpaths = filepaths
    filename_maker = fou.UniqueFilenameMaker(
        output_dir=upload_dir, ignore_existing=overwrite
    )
    filepaths = [filename_maker.get_output_path(inpath) for inpath in inpaths]

    tasks = list(zip(inpaths, filepaths))

    return filepaths, tasks


def _upload_media(ctx, tasks):
    if ctx.delegated:
        inpaths, outpaths = zip(*tasks)
        fos.copy_files(inpaths, outpaths)
        return

    num_uploaded = 0
    num_total = len(tasks)

    # @todo can switch to this if we require `fiftyone>=0.22.2`
    # num_workers = fou.recommend_thread_pool_workers()

    if hasattr(fou, "recommend_thread_pool_workers"):
        num_workers = fou.recommend_thread_pool_workers()
    else:
        num_workers = fo.config.max_thread_pool_workers or 8

    with multiprocessing.dummy.Pool(processes=num_workers) as pool:
        for _ in pool.imap_unordered(_do_upload_media, tasks):
            num_uploaded += 1
            if num_uploaded % 10 == 0:
                progress = num_uploaded / num_total
                label = f"Uploaded {num_uploaded} of {num_total}"
                yield ctx.trigger("set_progress", dict(progress=progress, label=label))


def _do_upload_media(task):
    inpath, outpath = task
    fos.copy_file(inpath, outpath)
