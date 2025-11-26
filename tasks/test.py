from pathlib import Path

from invoke import task, Context


def pytest_with_coverage(
    context: Context,
    suite: str,
    test_args: str = "",
    colour: bool = True,
    reports_dir: Path = Path("./reports"),
    tests_dir: Path = Path("./tests")
):
    tests_path = tests_dir / suite

    coverage_dir = reports_dir / "coverage"

    junit_xml = reports_dir / f"{suite}.junit.xml"
    coverage_xml = coverage_dir / f"coverage.{suite}.xml"
    coverage_html = coverage_dir / suite
    coverage_file = coverage_dir / f".coverage.{suite}"

    junit_xml_switch = f"--junitxml={junit_xml}"
    coverage_report_xml_switch = f"--cov-report=xml:{coverage_xml}"
    coverage_report_html_switch = f"--cov-report=html:{coverage_html}"
    colour_switch = "--color=yes" if colour else "--color=no"

    coverage_dir.mkdir(parents=True, exist_ok=True)

    test_command = (
        "pytest "
        f"{tests_path} "
        f"{junit_xml_switch} "
        f"{coverage_report_xml_switch} "
        f"{coverage_report_html_switch} "
        f"{colour_switch} "
        f"{test_args}"
    )

    context.run(test_command, env={
        "PYTHONDEVMODE": "1",
        "COVERAGE_FILE": f"{coverage_file}"
    })


@task
def unit(
    context: Context,
    test_args: str = "",
    colour: bool = True,
    reports_dir: Path = Path("./reports"),
    tests_dir: Path = Path("./tests")
):
    """Run unit tests."""
    pytest_with_coverage(
        context=context,
        suite="unit",
        test_args=test_args,
        colour=colour,
        reports_dir=reports_dir,
        tests_dir=tests_dir
    )


@task
def integration(
    context: Context,
    test_args: str = "",
    colour: bool = True,
    reports_dir: Path = Path("./reports"),
    tests_dir: Path = Path("./tests")
):
    """Run integration tests."""
    pytest_with_coverage(
        context=context,
        suite="integration",
        test_args=test_args,
        colour=colour,
        reports_dir=reports_dir,
        tests_dir=tests_dir
    )


@task
def component(
    context: Context,
    test_args: str = "",
    colour: bool = True,
    reports_dir: Path = Path("./reports"),
    tests_dir: Path = Path("./tests")
):
    """Run component tests."""
    pytest_with_coverage(
        context=context,
        suite="component",
        test_args=test_args,
        colour=colour,
        reports_dir=reports_dir,
        tests_dir=tests_dir
    )


@task
def report(
    context: Context,
    reports_dir: Path = Path("./reports"),
):
    """Generate combined test reports."""
    context.run((
        "junitparser merge "
        "reports/*.junit.xml "
        "reports/all.junit.xml"
    ))
    coverage_dir = reports_dir / "coverage"
    coverage_file_paths = coverage_dir.glob(".coverage.*")
    coverage_files = [str(path) for path in coverage_file_paths]
    coverage_files_string = " ".join(coverage_files)
    context.run((
        "coverage combine "
        "--data-file=./reports/coverage/.coverage "
        f"{coverage_files_string}"
    ))
    context.run((
        "coverage html "
        "--data-file=./reports/coverage/.coverage "
        "--directory=./reports/coverage/all"
    ))
    context.run((
        "coverage xml "
        "--data-file=./reports/coverage/.coverage "
        "-o ./reports/coverage/all/coverage.all.xml"
    ))
    context.run((
        "coverage report "
        "--data-file=./reports/coverage/.coverage"
    ))

@task(pre=[unit, integration, component, report])
def all(_context: Context):
    """Run all tests."""
    pass
