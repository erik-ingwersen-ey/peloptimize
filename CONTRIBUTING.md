# Contributor Guide

This section specifies the **contributing guidelines** for _Peloptimize_ project.

This project is

Here is a list of important resources for contributors:

- [Source Code]
- [Documentation]
- [Issue Tracker]
- [Code of Conduct]

[proprietary license]: https://opensource.org/licenses/Proprietary

[source code]: https://github.com/ingwersen-erik/peloptimize

[documentation]: https://peloptimize.readthedocs.io/

[issue tracker]: https://github.com/ingwersen-erik/peloptimize/issues

## How to report a bug

Report bugs on the [Issue Tracker].

To make it easier to other teammates to reproduce and identify the root-cause of the bug, try to answer the following questions, when filing an issue whenever applicable:

- Which operating system and Python version are you using?
- Which version of this project are you using?
- What did you do?
- What did you expect to see?
- What did you see instead?

The best way to get your bug fixed is to provide a test case, and/or steps to reproduce the issue.

### Tips

The next commands, might help you answer the previously listed questions:

**To identify the Python version:**

```console
$ python --version
```

**To identify the system information:**


````{tab} Windows
```console
C:\Users\erikingwersen>systeminfo
Host Name:                 ERIKINGWERS802E
OS Name:                   Microsoft Windows 11 Home
OS Manufacturer:           Microsoft Corporation
OS Configuration:          Standalone Workstation
OS Build Type:             Multiprocessor Free
Registered Owner:          Erik Ingwersen
Registered Organization:
...
Time Zone:                 (UTC-03:00) Brasilia
Total Physical Memory:     8,173 MB
Available Physical Memory: 3,954 MB
Virtual Memory: Max Size:  12,525 MB
Virtual Memory: Available: 7,913 MB
Virtual Memory: In Use:    4,612 MB
Page File Location(s):     C:\pagefile.sys
Domain:                    WORKGROUP
Logon Server:              \\ERIKINGWERS802E
Hyper-V Requirements:      A hypervisor has been detected. Features required for Hyper-V will not be displayed.
```
````

````{tab} Unix (MacOS / Linux)
```console
$ system_profiler SPSoftwareDataType SPHardwareDataType
Software:

    System Software Overview:

      System Version: macOS 12.2.1
      Kernel Version: Darwin 21.3.0
      Boot Volume: Macintosh HD
      Boot Mode: Normal
      Computer Name: MacBook Pro de Erik
      User Name: Erik Ingwersen (erikingwersen)
      Secure Virtual Memory: Enabled
      System Integrity Protection: Enabled
      Time since boot: 13 days 10:54

Hardware:

    Hardware Overview:

      Model Name: MacBook Pro
      Model Identifier: MacBookPro16,1
      Processor Name: 8-Core Intel Core i9
      Processor Speed: 2.4 GHz
      Number of Processors: 1
      Total Number of Cores: 8
      L2 Cache (per Core): 256 KB
      L3 Cache: 16 MB
      Hyper-Threading Technology: Enabled
```
````

**To identify the project version:**

To identify the package version you're using, enter your Python environment,
import the package and finally, access the `__version__` attribute from the package:

```console
$ python
Python 3.8.8 (default, Apr 13 2021, 12:59:45)
[Clang 10.0.0 ] :: Anaconda, Inc. on darwin
Type "help", "copyright", "credits" or "license" for more information.
>>> import pandas
>>> pandas.__version__
'1.3.5'
```

## How to request a feature

Request features on the [Issue Tracker].

Even if the project you're working on is not open-source, and there's a 99%
chance that you'll end up being the one in charge of developing the "new feature",
I would still advise you to create a new issue on the [Issue Tracker]. By mapping
the work to be done using the [Issue Tracker], others from your team will have
more visibility on what you're currently working on. This reduces the risk of
two people unknowingly working on the same issue. Additionally, you can link
project issues to commits, that might help you later on to categorize different
commits.

## How to set up your development environment

You need Python 3.7+ and the following tools:

- [Poetry]
- [Nox]
- [nox-poetry]

### Install Poetry

If you don't have Poetry installed, please refer to the following
[guide](https://python-poetry.org/docs/#installation), for instructions on how
to install it.

### Install _Peloptimize_

Install the package with development requirements:

````{tab} Poetry
:new-set:
```console
$ poetry install
```
````

````{tab} Pip
```console
$ pip install -e .
```
````

### Run Interactive Session

You can now run an interactive Python session, or the command-line interface:

````{tab} Poetry
:new-set:
```console
$ poetry run python
$ poetry run peloptimize
```
````

````{tab} Pip
```console
$ python
$ python peloptimize
```
````

[poetry]: https://python-poetry.org/

[nox]: https://nox.thea.codes/

[nox-poetry]: https://nox-poetry.readthedocs.io/

## How to test the project

Run the full test suite:

```console
$ nox
```

List the available [Nox](https://nox.thea.codes/) sessions:

```console
$ nox --list-sessions
```

You can also run a specific [Nox](https://nox.thea.codes/) session. For example, invoke the unit test suite like this:

```console
$ nox --session=tests
```

Unit tests are located in the _tests_ directory, and are written using the [pytest] testing framework.

[pytest]: https://pytest.readthedocs.io/

## How to submit changes

Open a [pull request] to submit changes to this project.

Your pull request needs to meet the following guidelines for acceptance:

- The [Nox](https://nox.thea.codes/) test suite must pass **without errors and warnings.**
- Include unit tests. This project maintains 100% code coverage.
- If your changes add functionality, update the documentation accordingly.

Feel free to submit early, though—we can always iterate on this.

To run linting and code formatting checks before committing your change, you can
install pre-commit as a Git hook by running the following command:

```console
$ nox --session=pre-commit -- install
```

It is recommended to open an issue before starting work on anything. This will
allow a chance to talk it over with the owners and validate your approach.

[pull request]: https://github.com/ingwersen-erik/peloptimize/pulls

<!-- github-only -->

[code of conduct]: CODE_OF_CONDUCT.md
