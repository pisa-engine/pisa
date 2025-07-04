Contributing to PISA
====================

Thank you for your interest in contributing to PISA!

Please make sure to check out the [Code of conduct](https://github.com/pisa-engine/pisa/blob/master/.github/CODE_OF_CONDUCT.md) first.

## Versioning

To update the project's version we currently use [bump](https://github.com/fabiospampinato/bump).
Bump can be installed using:
```
npm install -g @fabiospampinato/bump
```

All you need to do is running the following inside the repository:
```
bump [major|minor]patch]
git push origin master --tags
```

## Formatting

We use `clang-format` 20 to format the C++ code. The easiest way to
obtain it is to install the Python package. This is an example using
[pipx](https://pipx.pypa.io/latest/):

```shell
pipx install clang-format==20.1.6
```
