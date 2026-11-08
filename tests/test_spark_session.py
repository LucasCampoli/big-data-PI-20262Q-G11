"""The Java check runs before the JVM starts: supported versions pass, the rest stop the run."""
import dataclasses
import subprocess

import pytest

from pipeline import spark as spark_module
from pipeline.config import Config
from pipeline.errors import PipelineError


def fake_java(monkeypatch, banner):
    def run(*args, **kwargs):
        return subprocess.CompletedProcess(args, 0, stdout="", stderr=banner + "\nOpenJDK Runtime Environment")
    monkeypatch.setattr(spark_module.subprocess, "run", run)


@pytest.mark.parametrize("banner", ['openjdk version "17.0.12" 2024-07-16',
                                    'openjdk version "21.0.12" 2026-07-21',
                                    'openjdk version "25.0.4" 2026-07-21 LTS'])
def test_supported_java_passes(monkeypatch, banner):
    fake_java(monkeypatch, banner)
    cfg = dataclasses.replace(Config.from_env(), java_home="")
    assert spark_module.check_java(cfg) == banner


def test_other_java_stops_the_run(monkeypatch):
    fake_java(monkeypatch, 'openjdk version "11.0.24" 2024-07-16')
    with pytest.raises(PipelineError, match="supports JDK 17, 21 or 25"):
        spark_module.check_java(dataclasses.replace(Config.from_env(), java_home=""))
