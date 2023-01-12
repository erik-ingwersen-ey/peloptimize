[//]: # (Remove the following line to remove the "Powered by EY" logo)

[//]: # (![]&#40;docs/_static/EY_logo_5.gif&#41;)

# Peloptimize

[![PyPI](https://img.shields.io/pypi/v/peloptimize.svg)][pypi_]
[![Status](https://img.shields.io/pypi/status/peloptimize.svg)][status]
[![Python Version](https://img.shields.io/pypi/pyversions/peloptimize)][python version]
[![License](https://img.shields.io/pypi/l/peloptimize)][license]

[![Read the documentation at https://peloptimize.readthedocs.io/](https://img.shields.io/readthedocs/peloptimize/latest.svg?label=Read%20the%20Docs)][read the docs]
[![Codecov](https://codecov.io/gh/ingwersen-erik/peloptimize/branch/main/graph/badge.svg)][codecov]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]

[pypi_]: https://pypi.org/project/peloptimize/

[status]: https://pypi.org/project/peloptimize/

[python version]: https://pypi.org/project/peloptimize

[read the docs]: https://peloptimize.readthedocs.io/

[tests]: https://github.com/ingwersen-erik/peloptimize/actions?workflow=Tests

[codecov]: https://app.codecov.io/gh/ingwersen-erik/peloptimize

[pre-commit]: https://github.com/pre-commit/pre-commit

[black]: https://github.com/psf/black

## Description

Criação de modelos de Machine Learning para predição de custos, alimentando
um modelo de otimização de custos.

[//]: # (Criação de modelos de Machine Learning para predição de custos relacionados ao)

[//]: # (processo de pelotização da Vale S/A e otimização de custos através da aplicação)

[//]: # (de um modelo de otimização baseado nos resultados dos algoritmos preditivos.)

Acesse a documentação do projeto em: [documentação](https://erik-ingwersen-ey.github.io/peloptimize/)

----

## Installation

You can install _Peloptimize_ via [pip]:

```console
$ pip install peloptimize
```

That command installs the package in your local Python environment.
However, during development you might want to consider installing it in **
development mode**.

You can do so by adding the `-e` option the installation command:

```console
$ pip install -e .
```

> You can learn more about the differences between `pip install`
> and `pip install -e`
> commands
> at: [pip documentation - development-mode](https://setuptools.pypa.io/en/latest/userguide/development_mode
> . html#development-mode)

### Python Environment

When installing the _Peloptimize_, it might be a good idea to install
it in a specific Python environment. By default, _Peloptimize_
doesn't restrict the usage of any Python environment
managers ([conda](https://www.anaconda.com/),
[venv](https://virtualenv.pypa.io/en/latest/)
, [poetry](https://python-poetry.org/), etc.).

For more on how to **create**, **activate**, or **manage** environments, please
refer to:

- [conda cheat sheet](https://docs.conda.io/projects/conda/en/4.6.0/_downloads/52a95608c49671267e40c689e0bc00ca/conda-cheatsheet.pdf)
- [virtualenv cheat sheet](https://cheatography.com/ilyes64/cheat-sheets/python-virtual-environments/)
- [poetry cheat sheet](https://vikasz.hashnode.dev/python-poetry-cheatsheet)

> **Edit:** each environment manager has its own ups and downs. I won't start
> the endless
> discussion on which environment manager is the best here. At the end, it depends
> on your
> personal preferences. **Having said that, I strongly recommend choosing one and
sticking with it.**
> Refer to your teammates to define which tool will be the project's default
> environment manager.

## Usage

Please see the [Command-line Reference] for details.

**Edit:** you don't need to be exhaustive, when writing the usage section.
Add links to other documents that explain in-depth the various usages of
_Peloptimize_. Consider adding small videos in the form of
`gifs` too. They're a great way to visually explain the usage of the package,
and show new users how to use it.

## Contributing

This repository has a [Contributor Guide], to help others contribute to
the project.

## License

_Peloptimize_ is distributed under the terms of the
[Proprietary license](license). **Non authorized use, or distribution is
prohibited.**

## Issues

Before filling new issues, please read the [file an issue] document.
Remember to include a clear and concise description of the problem you're
having, as well as the steps to reproduce it.

<!-- github-only -->

[license]: https://github.com/ingwersen-erik/peloptimize/blob/main/LICENSE
[contributor guide]: https://github.com/ingwersen-erik/peloptimize/blob/main/CONTRIBUTING.md
[command-line reference]: https://peloptimize.readthedocs.io/en/latest/usage.html
