"""``FixedParallelRunner`` wraps Kedro's ``ParallelRunner`` to fix hook
execution in child processes on macOS (spawn mode).

When ``PACKAGE_NAME`` is ``None`` and the multiprocessing start method is
``spawn`` (macOS default), child processes skip ``_bootstrap_subprocess``
because ``PACKAGE_NAME`` is ``None``.  This causes ``settings.HOOKS`` to
return an empty list and all ``after_node_run`` hooks (including validation
hooks) are silently skipped.

Calling ``configure_project`` in the child process loads the pipeline
registry, which creates objects with circular references that break
result pickling over the multiprocessing queue.

``FixedParallelRunner`` works around both issues by reading
``settings.HOOKS`` in the main process (where the project is fully
configured), configuring only ``settings`` (not the pipeline registry)
in each worker, and registering hooks through a patched
``Task._run_node_synchronization``.  All hooks from ``settings.HOOKS``
are registered — the same set as the ``SequentialRunner``.
"""

from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from typing import TYPE_CHECKING, Any

from kedro.runner.parallel_runner import ParallelRunner

if TYPE_CHECKING:
    from concurrent.futures import Executor


def _init_worker(hook_classes: tuple[type, ...], settings_module: str) -> None:
    """Configure settings and register hooks in each worker process.

    Only ``settings`` (not the pipeline registry) are configured, which
    avoids circular-reference issues that break result pickling.  All hooks
    from ``settings.HOOKS`` are registered — the same set as the
    ``SequentialRunner``.

    Args:
        hook_classes: Hook classes read from ``settings.HOOKS`` in the main
            process.  Each class is instantiated fresh in the worker.
        settings_module: Dotted path to the project's settings module
            (e.g. ``"optimuskg.settings"``).
    """
    from kedro.framework.hooks.manager import _create_hook_manager, _register_hooks
    from kedro.framework.project import settings
    from kedro.runner.runner import Task

    # Configure settings (but NOT pipelines) so hooks that access
    # the catalog via KedroSession can resolve the project config.
    import kedro.framework.project as _project

    settings.configure(settings_module)
    _project.PACKAGE_NAME = settings_module.removesuffix(".settings")

    hooks = tuple(cls() for cls in hook_classes)

    @staticmethod  # type: ignore[misc]
    def _fixed_sync(
        package_name: str | None = None,
        logging_config: dict[str, Any] | None = None,
    ):  # type: ignore[override]
        hook_manager = _create_hook_manager()
        _register_hooks(hook_manager, hooks)
        return hook_manager

    Task._run_node_synchronization = _fixed_sync  # type: ignore[assignment]


def _get_settings_module() -> str:
    """Derive the settings module path from the currently-loaded settings."""
    from kedro.framework.project import settings

    for mod_path in settings._loaded_py_modules:  # type: ignore[union-attr]
        if mod_path.endswith(".settings"):
            return mod_path

    # Fallback: find the package that owns the hook classes
    hook_mod = type(settings.HOOKS[0]).__module__
    package = hook_mod.split(".")[0]
    return f"{package}.settings"


class FixedParallelRunner(ParallelRunner):
    """``ParallelRunner`` with proper hook support on macOS (spawn mode).

    Reads ``settings.HOOKS`` in the main process and registers the same
    hooks in each worker — identical to what ``SequentialRunner`` uses.
    """

    def _get_executor(self, max_workers: int) -> Executor:
        from kedro.framework.project import settings

        hook_classes = tuple(type(h) for h in settings.HOOKS)
        settings_module = _get_settings_module()

        context = os.environ.get("KEDRO_MP_CONTEXT")
        if context and context not in {"fork", "spawn"}:
            context = None
        ctx = get_context(context)
        return ProcessPoolExecutor(
            max_workers=max_workers,
            mp_context=ctx,
            initializer=_init_worker,
            initargs=(hook_classes, settings_module),
        )
