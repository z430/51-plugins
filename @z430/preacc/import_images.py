import multiprocessing.dummy


import fiftyone as fo
import fiftyone.core.storage as fos
import fiftyone.core.utils as fou
import fiftyone.operators as foo
import fiftyone.operators.types as types


class ImportImages(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="import_images",
            label="Import Images",
            light_icon="/assets/icon-light.svg",
            dark_icon="/assets/icon-dark.svg",
            allow_delegated_execution=True,
            allow_immediate_execution=True,
            default_choice_to_delegated=True,
            dynamic=True,
            execute_as_generator=True,
        )

    def resolve_input(self, ctx):
        inputs = types.Object()

        _import_images_inputs(ctx, inputs)

        return types.Property(inputs, view=types.View(label="Import Images"))

    def execute(self, ctx):
        for update in _import_media_only(ctx):
            yield update
        if not ctx.delegated:
            yield ctx.trigger("reload_dataset")


def _import_images_inputs(ctx, inputs):
    ready = False

    file_explorer = types.FileExplorerView(
        choose_dir=True,
        button_label="Choose a directory...",
    )
    prop = inputs.file(
        "directory",
        required=True,
        label="Directory",
        description="Choose a directory of media to add to this dataset",
        view=file_explorer,
    )
    directory = _parse_path(ctx, "directory")

    if directory:
        n = len(_glob_files(directory=directory))
        if n > 0:
            ready = True
            prop.view.caption = f"Found {n} files"
        else:
            prop.invalid = True
            prop.error_message = "No matching files"
    else:
        prop.view.caption = None

    if not ready:
        return False

    inputs.list(
        "tags",
        types.String(),
        default=None,
        label="Tags",
        description="Optional tag(s) to give each new sample",
        view=types.AutocompleteView(multiple=True),
    )

    inputs.str("gsn", label="GSN", required=True)
    inputs.str("gso", label="GSO", required=True)
    inputs.str(
        "datetime", label="Insert Datetime with format: 20250110T111003", required=False
    )

    ready = _upload_media_inputs(ctx, inputs)
    if not ready:
        return False

    return True


def _parse_path(ctx, key):
    value = ctx.params.get(key, None)
    return value.get("absolute_path", None) if value else None


def _glob_files(directory=None, glob_patt=None):
    if directory is not None:
        glob_patt = f"{directory}/*"

    if glob_patt is None:
        return []

    return fos.get_glob_matches(glob_patt)


def _upload_media_inputs(ctx, inputs):
    inputs.bool(
        "upload",
        default=False,
        required=False,
        label="Upload media",
        description=(
            "You can optionally upload the media to another location "
            "before adding it to the dataset. Would you like to do this?"
        ),
        view=types.CheckboxView(),
    )

    upload = ctx.params.get("upload", False)

    if upload:
        file_explorer = types.FileExplorerView(
            choose_dir=True,
            button_label="Choose a directory...",
        )
        inputs.file(
            "upload_dir",
            required=True,
            label="Upload directory",
            description="Provide a directory into which to upload the media",
            view=file_explorer,
        )
        upload_dir = _parse_path(ctx, "upload_dir")

        if upload_dir is None:
            return False

        inputs.bool(
            "overwrite",
            default=False,
            required=False,
            label="Overwrite existing",
            description=(
                "Do you wish to overwrite existing media of the same name "
                "(True) or append a unique suffix when necessary to avoid "
                "name clashses (False)"
            ),
            view=types.CheckboxView(),
        )

    return True


def _create_sample(filepath, tags, gsn, gso, metadata=None):
    sample = fo.Sample(filepath=filepath, tags=tags)
    sample["gsn"] = gsn
    sample["gso"] = gso
    return sample


def _import_media_only(ctx):
    tags = ctx.params.get("tags", None)
    gsn = ctx.params.get("gsn", None)
    gso = ctx.params.get("gso", None)

    directory = _parse_path(ctx, "directory")
    glob_patt = None

    filepaths = _glob_files(directory=directory, glob_patt=glob_patt)
    num_total = len(filepaths)

    if num_total == 0:
        return

    filepaths, tasks = _upload_media_tasks(ctx, filepaths)
    if tasks:
        for progress in _upload_media(ctx, tasks):
            yield progress

    # make_sample = lambda f: fo.Sample(filepath=f, tags=tags)
    def make_sample(f):
        return _create_sample(
            filepath=f,
            tags=tags,
            gsn=gsn,
            gso=gso,
        )

    if ctx.delegated:
        samples = map(make_sample, filepaths)
        ctx.dataset.add_samples(samples, num_samples=len(filepaths))
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
